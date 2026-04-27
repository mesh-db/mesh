# MeshDB operator runbook

Sample manifests and step-by-step procedures for operating a
multi-raft MeshDB cluster on Kubernetes. Adapt to your
namespace / image registry / storage class. The reference
shape is **3 peers, replication_factor = 3, num_partitions = 8**
— a configuration that tolerates one node failure and serves
moderate write loads.

## Files

```
deploy/
├── grafana/
│   └── meshdb-cluster.json     # importable dashboard JSON
├── prometheus/
│   └── alerts.yaml             # rule_files entry
├── k8s/
│   ├── configmap.yaml          # ServerConfig.toml
│   ├── service.yaml            # headless + Bolt ClusterIP
│   ├── statefulset.yaml        # 3 pods with anti-affinity
│   └── pdb.yaml                # max 1 unavailable
└── RUNBOOK.md                  # this file
```

## First-time bring-up

```bash
kubectl create namespace meshdb
kubectl apply -f deploy/k8s/configmap.yaml
kubectl apply -f deploy/k8s/service.yaml
kubectl apply -f deploy/k8s/statefulset.yaml
kubectl apply -f deploy/k8s/pdb.yaml
```

Expected sequence:
1. `meshdb-0` starts, bootstraps the meta Raft group (its config
   has `bootstrap = true` set by the init container).
2. `meshdb-1` and `meshdb-2` start in order. Each joins the
   meta cluster as it comes up.
3. Once every peer's `/readyz` returns 200, the cluster is
   serving traffic. Connect Bolt drivers via the
   `meshdb-bolt` Service.

Verify:

```bash
kubectl -n meshdb get pods -l app=meshdb
kubectl -n meshdb exec meshdb-0 -- /usr/local/bin/meshdb-server validate-backup --help
```

## Day-2 operations

### Rolling upgrade (new image)

```bash
kubectl -n meshdb set image statefulset/meshdb \
  meshdb=ghcr.io/mesh-db/meshdb-server:VERSION
```

The StatefulSet's `OrderedReady` strategy + the SIGTERM-driven
leadership drain combine to do this without dropping below
quorum: each pod takes itself out of `/readyz` before exiting,
its partition leadership transfers in-place, the new image
comes up and rejoins, then the next pod cycles.

If a pod fails to come back up (`/readyz` never goes 200), the
rollout halts. Investigate before forcing.

### Drain a single peer (planned removal)

For temporary removal (e.g., node maintenance, k8s upgrade):

```bash
kubectl -n meshdb cordon <node>
kubectl -n meshdb drain <node> --ignore-daemonsets --delete-emptydir-data
```

The PDB ensures the kubelet only drains one peer at a time.
Each peer leaves the rotation cleanly:
1. SIGTERM arrives.
2. `/readyz` returns 503 — kubelet stops sending traffic.
3. `drain_leadership` runs (bounded by
   `shutdown_drain_timeout_seconds`, default 30s).
4. Process exits; kubelet reclaims the pod.

For **permanent** removal of a peer, additionally run
`MultiRaftCluster::drain_peer` from a surviving peer to remove
the gone peer from every partition's voter set, then scale the
StatefulSet down.

### Recovering from a peer-down emergency

When a peer goes down hard (kernel panic, disk failure, network
partition):

1. **Quorum check.** With 3 peers / rf=3, 2 surviving peers
   keep quorum. The partition leader for each affected
   partition re-elects within ~5 seconds. New writes succeed.
2. **Check the dashboard.** The `mesh_multiraft_partitions_led`
   gauge on the surviving peers should go up; the dead peer's
   line goes flat. Apply lag stays at 0 on survivors.
3. **Restore.** Either:
   - **Easy path**: bring the failed pod back. Its persistent
     volume still has the data; openraft replays the log on
     restart and catches up.
   - **Lost-disk path**: the PVC is gone. Delete the pod, let
     the StatefulSet recreate it with a fresh PVC, and openraft
     installs a snapshot from the leader. Allow more time —
     snapshots can be GBs.

### Backup

```bash
# From inside any peer:
kubectl -n meshdb exec meshdb-0 -- /usr/local/bin/meshdb-server \
  --config /etc/meshdb/meshdb.toml \
  # In future this becomes a CLI subcommand; for now the
  # backup is triggered programmatically through the
  # MeshService::take_cluster_backup gRPC RPC. See
  # crates/meshdb-server/src/main.rs for invocation patterns.
```

The backup writes a snapshot to each peer's
`data_dir/raft/<group>/` rocksdb. Archive the data_dirs +
the manifest JSON.

### Restore

1. Stop the cluster (`kubectl scale statefulset/meshdb --replicas=0`).
2. Restore each peer's data_dir from the archive into the
   matching PVC.
3. Run `meshdb-server validate-backup --manifest=path
   --data-dir=<peer-data-dir> --peer-id=<N>` per peer.
4. Scale back up. The cluster's recovery loop resolves any
   in-doubt PREPAREs at boot.

### TLS cert rotation

If `[bolt_tls] reload_interval_seconds` and `[grpc_tls]
reload_interval_seconds` are set (recommended), cert rotation
is **zero-downtime**:

1. Update the TLS Secret (or the cert/key files via your
   cert-manager / secret-rotator).
2. The reload task picks up the new cert within
   `reload_interval_seconds` (default 60s).
3. New TLS handshakes use the rotated cert; in-flight
   connections finish on the old one.

If you didn't set the reload knobs, you'll need a rolling
restart to pick up new certs:

```bash
kubectl -n meshdb rollout restart statefulset/meshdb
```

### Adding a peer (scaling up)

This is **not** a one-step `kubectl scale`. New peers need to
be added to the cluster's metadata Raft group as voters, then
to each partition's replica set. The order matters; adding the
StatefulSet replica without first updating the cluster
membership leaves the new pod isolated.

Currently this is a manual gRPC orchestration; documented in
the `MultiRaftCluster::add_partition_replica` API. A
`meshdb-server cluster add-peer` CLI subcommand is on the v3
roadmap.

### Removing a peer (scaling down)

Reverse of adding. Drain the peer's partition leadership +
voter membership before scaling the StatefulSet down. Use
`MultiRaftCluster::drain_peer`.

## Observability

### Grafana

Import `deploy/grafana/meshdb-cluster.json`. The dashboard's
`instance` template variable populates from the
`meshdb_cypher_queries_total` series, so any peer scraped by
Prometheus appears.

### Prometheus

Add to your config:

```yaml
rule_files:
  - /etc/prometheus/meshdb-alerts.yaml

scrape_configs:
  - job_name: meshdb
    kubernetes_sd_configs:
      - role: pod
    relabel_configs:
      - source_labels: [__meta_kubernetes_pod_label_app]
        action: keep
        regex: meshdb
      - source_labels: [__meta_kubernetes_pod_container_port_name]
        action: keep
        regex: metrics
```

Mount `deploy/prometheus/alerts.yaml` at the configured
`rule_files` path. Alerts cover peer-down, leader-skew, apply
lag, in-doubt PREPAREs, DDL gate timeouts, and p99 latency.

## Common pitfalls

* **Bolt clients hanging on connect.** Check `/readyz`. If a
  peer is in shutdown drain its readiness probe is 503; the
  client's TLS handshake will timeout. Wait for the new pod
  to come up.
* **`/readyz` never returns 200 on the bootstrap peer.** The
  init container sets `bootstrap = true` only on `meshdb-0`.
  Other peers waiting for the bootstrap to finish report
  unready until the meta group elects. Usually clears within
  10s.
* **One peer holds all partition leadership.** Cosmetic but
  uneven load. Run `drain_leadership` from that peer (or
  restart it) — surviving peers re-elect and the load
  redistributes. The `mesh_multiraft_partitions_led` metric
  shows the skew.
* **DDL gate timeouts on a healthy cluster.** Likely the meta
  replica on one peer is genuinely slow (disk I/O). Check
  `mesh_multiraft_apply_lag{group="meta"}` per peer.
* **Audit log file growing unbounded.** Expected — admin
  operations are infrequent so size grows slowly. Rotate
  externally (logrotate, `cron`) if it becomes an issue;
  the format is line-oriented JSON so any line-aware rotator
  works.

## Further reading

* `CLAUDE.md` — architecture overview + per-mode design
  decisions.
* `crates/meshdb-server/src/config.rs` — every config field
  with rustdoc on its semantics.
* `crates/meshdb-rpc/src/multi_raft_cluster.rs` — the
  multi-raft cluster's API surface (drain, rebalance, backup).
