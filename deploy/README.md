# MeshDB deployment artifacts

Sample, production-shaped manifests + dashboards for running a
multi-raft MeshDB cluster on Kubernetes alongside Prometheus
and Grafana.

| Path | Purpose |
|------|---------|
| `k8s/configmap.yaml` | `ServerConfig` TOML with the operational hardening knobs (drain timeout, query timeout, plan cache, audit log, …) all set. |
| `k8s/service.yaml` | Headless Service for stable per-pod DNS + a ClusterIP Service for Bolt drivers. |
| `k8s/statefulset.yaml` | 3-pod StatefulSet with anti-affinity, `OrderedReady` rolling updates, livez/readyz probes. |
| `k8s/pdb.yaml` | PodDisruptionBudget — voluntary disruptions never break quorum. |
| `grafana/meshdb-cluster.json` | Importable dashboard: query rate, p99 latency, leader skew, apply lag, in-doubt PREPAREs, DDL gate, forward writes. |
| `prometheus/alerts.yaml` | Alert rules: peer-down, meta-without-leader, apply lag, in-doubt PREPAREs, DDL gate timeouts, sustained high latency. |
| `RUNBOOK.md` | Day-2 operations: rolling upgrades, drain, backup/restore, TLS rotation, adding/removing peers, observability setup. |

These are intentionally minimal references. Adapt to your
namespace, image registry, storage class, and observability
backends. `RUNBOOK.md` is the place to start if you're new to
operating MeshDB.
