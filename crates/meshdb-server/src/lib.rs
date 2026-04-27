// Bolt's deeply-nested async dispatch (HELLO → BEGIN → RUN →
// PULL → COMMIT, each its own `#[tracing::instrument]`-decorated
// async fn) plus the trigger-fire-recursion now plumbed through
// `meshdb-rpc` blows the default 128 query depth on stable rustc.
// Bumping for the same reason as `meshdb-rpc::lib` — the
// compiler's suggested fix.
#![recursion_limit = "256"]

pub mod bolt;
pub mod config;
pub mod metrics;
pub mod tls_reload;
pub mod value_conv;

use anyhow::{anyhow, Context, Result};
use config::{ClusterMode, ServerConfig};
use meshdb_cluster::raft::{BasicNode, GraphStateMachine, RaftCluster};
use meshdb_cluster::{
    Cluster, ClusterState, Membership, PartitionId, PartitionMap, PartitionReplicaMap, Partitioner,
    Peer, PeerId,
};
use meshdb_rpc::{
    CoordinatorLog, GrpcNetwork, MeshRaftService, MeshService, MetaGraphApplier, MultiRaftCluster,
    ParticipantLog, PartitionGraphApplier, RaftGroupTarget, Routing, StoreGraphApplier,
};
use meshdb_storage::{RocksDbStorageEngine, StorageEngine};
use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;

/// Bundle of everything the binary needs to serve a peer: the storage-backed
/// query/write service, an optional Raft cluster (built when `peers` is
/// non-empty), and the matching gRPC service handler for incoming Raft RPCs.
///
/// Tests can destructure this and compose a tonic server manually; the
/// binary uses [`serve`] which handles bootstrap, server task spawning, and
/// graceful Ctrl-C shutdown.
pub struct ServerComponents {
    pub service: MeshService,
    pub raft: Option<Arc<RaftCluster>>,
    pub raft_service: Option<MeshRaftService>,
    /// Multi-raft cluster handles, populated only in
    /// [`ClusterMode::MultiRaft`]. Holds the metadata Raft group plus
    /// every partition Raft group this peer is a replica of. Tests
    /// destructure this for direct access to per-group Raft handles.
    pub multi_raft: Option<Arc<MultiRaftCluster>>,
}

/// Build the storage-backed [`MeshService`] synchronously. Supports
/// single-node and routing mode — Raft mode requires async bootstrap
/// and must go through [`build_components`] instead.
pub fn build_service(config: &ServerConfig) -> Result<MeshService> {
    config.validate().map_err(|e| anyhow!(e))?;
    std::fs::create_dir_all(&config.data_dir)
        .with_context(|| format!("creating data dir {}", config.data_dir.display()))?;
    let store: Arc<dyn StorageEngine> = Arc::new(
        RocksDbStorageEngine::open(&config.data_dir)
            .with_context(|| format!("opening store at {}", config.data_dir.display()))?,
    );

    match config.resolved_mode() {
        ClusterMode::Single => {
            let store_for_triggers = store.clone();
            Ok(apply_apoc_import(
                MeshService::new(store),
                config,
                &store_for_triggers,
            ))
        }
        ClusterMode::Routing => Ok(build_routing_service(config, store)?),
        ClusterMode::Raft => Err(anyhow!(
            "Raft mode requires async bootstrap; call build_components instead"
        )),
        ClusterMode::MultiRaft => Err(anyhow!(
            "multi-raft mode requires async bootstrap; call build_components instead"
        )),
    }
}

/// Bake the operator-configured [`ImportConfig`] (if any) and
/// the [`TriggerRegistry`](meshdb_executor::apoc_trigger::TriggerRegistry)
/// into the service's procedure-registry factory. The two
/// attachments share a single closure so we don't pay two
/// separate factory wraps. Leaves the factory at its default
/// when neither feature is compiled in / configured — in that
/// case `apoc.load.*` and `apoc.trigger.*` both refuse with
/// messages pointing at the missing config / attachment.
#[allow(unused_variables)]
fn apply_apoc_import(
    service: MeshService,
    config: &ServerConfig,
    store: &Arc<dyn StorageEngine>,
) -> MeshService {
    apply_apoc_import_with_registry(
        service,
        config,
        store,
        #[cfg(feature = "apoc-trigger")]
        None,
    )
}

/// `apply_apoc_import` variant that accepts a pre-built
/// [`TriggerRegistry`] — used by the multi-raft bootstrap so the
/// same registry instance shared by the meta applier (refreshed on
/// `InstallTrigger` apply) is also exposed via the procedure
/// factory (used by `apoc.trigger.list()` and the firing path).
/// Without sharing, the applier would refresh its private copy
/// and the user-visible registry would never reflect the change.
#[allow(unused_variables)]
fn apply_apoc_import_with_registry(
    service: MeshService,
    config: &ServerConfig,
    store: &Arc<dyn StorageEngine>,
    #[cfg(feature = "apoc-trigger")] preset_trigger_registry: Option<
        meshdb_executor::apoc_trigger::TriggerRegistry,
    >,
) -> MeshService {
    #[cfg(feature = "apoc-load")]
    let import_cfg = config.apoc_import.clone();
    #[cfg(feature = "apoc-trigger")]
    let trigger_registry = preset_trigger_registry.or_else(|| {
        match meshdb_executor::apoc_trigger::TriggerRegistry::from_storage(store.clone()) {
            Ok(r) => Some(r),
            Err(e) => {
                tracing::warn!(error = %e, "loading triggers from storage failed; apoc.trigger.* disabled");
                None
            }
        }
    });
    // No-op short-circuit when neither feature is on.
    #[cfg(not(any(feature = "apoc-load", feature = "apoc-trigger")))]
    {
        return service;
    }
    #[cfg(any(feature = "apoc-load", feature = "apoc-trigger"))]
    service.with_procedure_registry_factory(move || {
        let mut p = meshdb_executor::ProcedureRegistry::new();
        p.register_defaults();
        #[cfg(feature = "apoc-load")]
        if let Some(cfg) = &import_cfg {
            p.set_import_config(cfg.clone());
        }
        #[cfg(feature = "apoc-trigger")]
        if let Some(reg) = &trigger_registry {
            p.set_trigger_registry(reg.clone());
        }
        p
    })
}

/// Open the configured admin-operation audit log, if any. Each
/// peer maintains its own file so a multi-peer cluster's audit
/// trail is the union of every peer's local file. `None` when
/// `audit_log_path` is unset — admin operations still succeed
/// without leaving a record.
fn open_audit_log(config: &ServerConfig) -> Result<Option<Arc<meshdb_rpc::AuditLog>>> {
    let Some(path) = config.audit_log_path.as_ref() else {
        return Ok(None);
    };
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("creating audit-log parent dir {}", parent.display()))?;
    }
    let log = meshdb_rpc::AuditLog::open(path.clone())
        .with_context(|| format!("opening audit log {}", path.display()))?;
    Ok(Some(Arc::new(log)))
}

/// Shared routing-mode assembly used by both `build_service` (sync)
/// and `build_components` (async). Opens the durable 2PC coordinator
/// log under `data_dir` so recovery and rotation are wired up
/// end-to-end regardless of which entry point the caller used.
fn build_routing_service(
    config: &ServerConfig,
    store: Arc<dyn StorageEngine>,
) -> Result<MeshService> {
    let peers: Vec<Peer> = config
        .peers
        .iter()
        .map(|p| {
            let base = Peer::new(PeerId(p.id), p.address.clone());
            match &p.bolt_address {
                Some(ba) => base.with_bolt_address(ba),
                None => base,
            }
        })
        .collect();
    let cluster = Arc::new(
        Cluster::new(PeerId(config.self_id), config.num_partitions, peers)
            .context("building cluster")?,
    );
    let client_tls = build_grpc_client_tls(config)?;
    let routing = Arc::new(match client_tls.clone() {
        Some(tls) => Routing::with_tls(cluster, tls).context("building routing")?,
        None => Routing::new(cluster).context("building routing")?,
    });
    let log = Arc::new(
        CoordinatorLog::open(coordinator_log_path(&config.data_dir))
            .context("opening coordinator log")?,
    );
    let participant_log = Arc::new(
        ParticipantLog::open(participant_log_path(&config.data_dir))
            .context("opening participant log")?,
    );
    let query_timeout = config
        .query_timeout_seconds
        .map(std::time::Duration::from_secs);
    let store_for_triggers = store.clone();
    Ok(apply_apoc_import(
        MeshService::with_routing_and_log(store, routing, Some(log))
            .with_participant_log(Some(participant_log))
            .with_client_tls(client_tls)
            .with_query_timeout(query_timeout)
            .with_query_max_rows(config.query_max_rows)
            .with_max_concurrent_queries(config.max_concurrent_queries)
            .with_audit_log(open_audit_log(config)?)
            .with_plan_cache(config.plan_cache_size.map(meshdb_rpc::PlanCache::new)),
        config,
        &store_for_triggers,
    ))
}

/// Build the [`tonic::transport::ClientTlsConfig`] used for outgoing
/// peer channels when `config.grpc_tls` is set, or `Ok(None)` when
/// it's not. Called by both the routing- and Raft-mode builders so
/// every code path gets the same config.
fn build_grpc_client_tls(
    config: &ServerConfig,
) -> Result<Option<tonic::transport::ClientTlsConfig>> {
    let Some(tls_cfg) = config.grpc_tls.as_ref() else {
        return Ok(None);
    };
    meshdb_rpc::tls::install_default_crypto_provider();
    // mTLS: when the listener requires client certs, every
    // outgoing peer endpoint must present its own identity. Reuse
    // `cert_path` / `key_path` so a single per-peer cert covers
    // both directions (server identity + client identity).
    let client = if tls_cfg.client_ca_path.is_some() {
        meshdb_rpc::tls::build_client_tls_config_mtls(
            &tls_cfg.ca_path,
            &tls_cfg.cert_path,
            &tls_cfg.key_path,
        )
        .context("building grpc client tls config (mtls)")?
    } else {
        meshdb_rpc::tls::build_client_tls_config(&tls_cfg.ca_path)
            .context("building grpc client tls config")?
    };
    Ok(Some(client))
}

/// Path of the coordinator recovery log inside the configured data
/// directory. Kept in a helper so the meshdb-rpc test harness can point
/// at the same file during integration tests.
pub fn coordinator_log_path(data_dir: &std::path::Path) -> std::path::PathBuf {
    data_dir.join("coordinator-log.jsonl")
}

/// Path of the participant recovery log inside the configured data
/// directory. Sibling to the coordinator log; records every
/// `BatchWrite` phase this peer receives so a crash after PREPARE
/// ACK doesn't lose the staged batch.
pub fn participant_log_path(data_dir: &std::path::Path) -> std::path::PathBuf {
    data_dir.join("participant-log.jsonl")
}

/// Build the full [`ServerComponents`] bundle. Switches on
/// [`ServerConfig::resolved_mode`]:
///
/// - [`ClusterMode::Single`] → local-only service, no Raft, no
///   routing.
/// - [`ClusterMode::Routing`] → hash-partitioned routing service
///   with a durable 2PC coordinator log under `data_dir`. No Raft.
/// - [`ClusterMode::Raft`] → single-Raft-group replication with a
///   [`StoreGraphApplier`] wiring the executor's writes into the
///   state machine.
/// Path of the on-disk marker that records the `num_partitions`
/// value used when this peer's data directory was first
/// initialized. Used by [`check_num_partitions_marker`] to refuse
/// silent resharding on restart.
pub fn num_partitions_marker_path(data_dir: &Path) -> PathBuf {
    data_dir.join("num_partitions")
}

/// Verify the operator hasn't changed `num_partitions` since the
/// data directory was first initialized. On first start (no
/// marker), write the current value. On subsequent starts, compare
/// the marker to `current` and bail with a descriptive error if
/// they differ — the only safe path forward is the offline
/// dump-and-restore workflow.
pub fn check_num_partitions_marker(data_dir: &Path, current: u32) -> Result<()> {
    let marker = num_partitions_marker_path(data_dir);
    match std::fs::read_to_string(&marker) {
        Ok(contents) => {
            let persisted: u32 = contents.trim().parse().with_context(|| {
                format!(
                    "marker file {} is corrupted (expected u32, got {:?})",
                    marker.display(),
                    contents
                )
            })?;
            if persisted != current {
                return Err(anyhow!(
                    "num_partitions changed across restart: data dir was \
                     initialized with {persisted}, but the current config says \
                     {current}. Hash-partitioned data on disk is invalidated by \
                     this change — keys would re-hash to different partitions \
                     and routing would silently miss them. To change the \
                     partition count safely, dump the graph (apoc.export.cypher.all), \
                     wipe the data directories on every peer, and re-import \
                     against a fresh cluster started with the new value."
                ));
            }
            Ok(())
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            // Fresh data dir. Record the current value so a future
            // restart catches the misconfiguration.
            std::fs::write(&marker, current.to_string()).with_context(|| {
                format!("writing num_partitions marker at {}", marker.display())
            })?;
            Ok(())
        }
        Err(e) => Err(anyhow::Error::from(e).context(format!(
            "reading num_partitions marker at {}",
            marker.display()
        ))),
    }
}

pub async fn build_components(config: &ServerConfig) -> Result<ServerComponents> {
    config.validate().map_err(|e| anyhow!(e))?;
    std::fs::create_dir_all(&config.data_dir)
        .with_context(|| format!("creating data dir {}", config.data_dir.display()))?;
    // Anti-footgun: refuse to start if the operator changed
    // `num_partitions` across restarts. Hash-partitioned data on
    // disk is sensitive to the partition count — every key would
    // re-hash to a different partition under the new count, and
    // our routing path would silently miss data. The marker file
    // is written on first start; subsequent starts must match it.
    // The escape hatch is the offline dump-and-restore workflow
    // documented in CLAUDE.md ("Partition count change").
    check_num_partitions_marker(&config.data_dir, config.num_partitions)?;
    let store: Arc<dyn StorageEngine> = Arc::new(
        RocksDbStorageEngine::open(&config.data_dir)
            .with_context(|| format!("opening store at {}", config.data_dir.display()))?,
    );

    let query_timeout = config
        .query_timeout_seconds
        .map(std::time::Duration::from_secs);
    match config.resolved_mode() {
        ClusterMode::Single => {
            let store_for_triggers = store.clone();
            return Ok(ServerComponents {
                service: apply_apoc_import(
                    MeshService::new(store)
                        .with_query_timeout(query_timeout)
                        .with_query_max_rows(config.query_max_rows)
                        .with_max_concurrent_queries(config.max_concurrent_queries)
                        .with_audit_log(open_audit_log(config)?),
                    config,
                    &store_for_triggers,
                ),
                raft: None,
                raft_service: None,
                multi_raft: None,
            });
        }
        ClusterMode::Routing => {
            return Ok(ServerComponents {
                service: build_routing_service(config, store)?,
                raft: None,
                raft_service: None,
                multi_raft: None,
            });
        }
        ClusterMode::Raft => {
            // Fall through to the Raft bootstrap path below.
        }
        ClusterMode::MultiRaft => {
            return build_multi_raft_components(config, store).await;
        }
    }

    // Build the initial cluster state Raft will manage.
    let peers: Vec<Peer> = config
        .peers
        .iter()
        .map(|p| {
            let base = Peer::new(PeerId(p.id), p.address.clone());
            match &p.bolt_address {
                Some(ba) => base.with_bolt_address(ba),
                None => base,
            }
        })
        .collect();
    let membership = Membership::new(peers);
    let peer_ids: Vec<PeerId> = membership.peer_ids().collect();
    let partition_map = PartitionMap::round_robin(&peer_ids, config.num_partitions)
        .context("building partition map")?;
    let initial_state = ClusterState::new(membership, partition_map);

    // GrpcNetwork only registers channels for peers other than self.
    let peer_map: Vec<(u64, String)> = config
        .peers
        .iter()
        .filter(|p| p.id != config.self_id)
        .map(|p| (p.id, p.address.clone()))
        .collect();
    let client_tls = build_grpc_client_tls(config)?;
    let network = match client_tls.clone() {
        Some(tls) => GrpcNetwork::with_tls(peer_map, tls).context("building grpc network")?,
        None => GrpcNetwork::new(peer_map).context("building grpc network")?,
    };

    // The graph applier translates committed GraphCommand entries into
    // local store writes — this is what makes the Raft replication actually
    // affect the database.
    let applier: Arc<dyn GraphStateMachine> = Arc::new(StoreGraphApplier::new(store.clone()));

    let raft_dir = config.data_dir.join("raft");
    std::fs::create_dir_all(&raft_dir)
        .with_context(|| format!("creating raft dir {}", raft_dir.display()))?;
    let raft_cluster = RaftCluster::open_persistent(
        config.self_id,
        &raft_dir,
        initial_state,
        network,
        Some(applier),
    )
    .await
    .context("building raft cluster")?;
    let raft_service = MeshRaftService::new(raft_cluster.raft.clone());
    let raft = Arc::new(raft_cluster);

    // The MeshService routes writes through Raft so every replica's local
    // store ends up consistent.
    let store_for_triggers = store.clone();
    let service = apply_apoc_import(
        MeshService::with_raft(store, raft.clone())
            .with_client_tls(client_tls)
            .with_query_timeout(query_timeout)
            .with_query_max_rows(config.query_max_rows)
            .with_max_concurrent_queries(config.max_concurrent_queries)
            .with_audit_log(open_audit_log(config)?)
            .with_plan_cache(config.plan_cache_size.map(meshdb_rpc::PlanCache::new)),
        config,
        &store_for_triggers,
    );

    Ok(ServerComponents {
        service,
        raft: Some(raft),
        raft_service: Some(raft_service),
        multi_raft: None,
    })
}

/// Build the per-group Raft handles + dispatch registry for
/// `mode = "multi-raft"`. One metadata Raft group spans every peer;
/// per-partition Raft groups are instantiated only on this peer's
/// replicas of the [`PartitionReplicaMap`].
///
/// This phase wires up the bootstrap and leader-election machinery —
/// `MeshService` itself is built in single-node configuration so the
/// existing read path keeps working. The write path is rerouted
/// through `MultiRaftCluster` in a follow-up commit (Phase 6).
async fn build_multi_raft_components(
    config: &ServerConfig,
    store: Arc<dyn StorageEngine>,
) -> Result<ServerComponents> {
    let rf = config
        .resolved_replication_factor()
        .ok_or_else(|| anyhow!("multi-raft mode requires a replication factor"))?;

    // Build peer list and replica map.
    let peers: Vec<Peer> = config
        .peers
        .iter()
        .map(|p| {
            let base = Peer::new(PeerId(p.id), p.address.clone());
            match &p.bolt_address {
                Some(ba) => base.with_bolt_address(ba),
                None => base,
            }
        })
        .collect();
    let membership = Membership::new(peers);
    let peer_ids: Vec<PeerId> = membership.peer_ids().collect();
    // Choose between round-robin and weighted placement. Weighted
    // is selected only when at least one peer specifies an explicit
    // `weight` in the TOML config — otherwise we use round-robin so
    // existing clusters (and existing snapshots / persisted replica
    // maps) stay bit-identical.
    let any_weighted = config.peers.iter().any(|p| p.weight.is_some());
    let initial_replica_map = if any_weighted {
        let weighted: Vec<(PeerId, f64)> = config
            .peers
            .iter()
            .map(|p| (PeerId(p.id), p.weight.unwrap_or(1.0)))
            .collect();
        PartitionReplicaMap::weighted_replicas(&weighted, config.num_partitions, rf)
            .context("building weighted partition replica map")?
    } else {
        PartitionReplicaMap::round_robin_replicas(&peer_ids, config.num_partitions, rf)
            .context("building partition replica map")?
    };
    // The legacy PartitionMap (single owner per partition) still
    // populates ClusterState — multi-raft routing reads from
    // replica_map, but ClusterState's structure stays unchanged so
    // the meta group's snapshot/restore is shared with single-Raft.
    let partition_map = PartitionMap::round_robin(&peer_ids, config.num_partitions)
        .context("building partition map")?;
    // Seed the meta-replicated ClusterState with the initial
    // replica map so a cold start has the placement durable from
    // the first applied entry. On a warm restart, openraft loads
    // the persisted ClusterState which carries any
    // `SetPartitionReplicas` updates applied since bootstrap.
    let initial_state =
        ClusterState::with_replica_map(membership, partition_map, initial_replica_map.clone());
    let replica_map = Arc::new(initial_replica_map);

    // One channel pool, shared across every group via for_group().
    let peer_map: Vec<(u64, String)> = config
        .peers
        .iter()
        .filter(|p| p.id != config.self_id)
        .map(|p| (p.id, p.address.clone()))
        .collect();
    let client_tls = build_grpc_client_tls(config)?;
    let base_network = match client_tls.clone() {
        Some(tls) => GrpcNetwork::with_tls(peer_map, tls).context("building grpc network")?,
        None => GrpcNetwork::new(peer_map).context("building grpc network")?,
    };

    // Metadata group — every peer is a member.
    let meta_dir = config.data_dir.join("raft").join("meta");
    std::fs::create_dir_all(&meta_dir)
        .with_context(|| format!("creating meta raft dir {}", meta_dir.display()))?;
    let meta_applier_concrete = MetaGraphApplier::new(store.clone());
    // Single TriggerRegistry instance shared between the meta
    // applier (so InstallTrigger / DropTrigger entries replicated
    // through meta refresh it) and the procedure-registry factory
    // below (so `apoc.trigger.list()` reads from the same map).
    // Without this sharing, the applier would refresh its own
    // private copy and the user-visible registry would never see
    // the new triggers.
    #[cfg(feature = "apoc-trigger")]
    let shared_trigger_registry = match meshdb_executor::apoc_trigger::TriggerRegistry::from_storage(
        store.clone(),
    ) {
        Ok(reg) => Some(reg),
        Err(e) => {
            tracing::warn!(error = %e, "loading triggers from storage failed; trigger surface disabled");
            None
        }
    };
    #[cfg(feature = "apoc-trigger")]
    let meta_applier_concrete = match &shared_trigger_registry {
        Some(reg) => meta_applier_concrete.with_trigger_registry(reg.clone()),
        None => meta_applier_concrete,
    };
    let meta_applier: Arc<dyn GraphStateMachine> = Arc::new(meta_applier_concrete);
    let meta_raft = RaftCluster::open_persistent(
        config.self_id,
        &meta_dir,
        initial_state.clone(),
        base_network.for_group(RaftGroupTarget::Meta),
        Some(meta_applier),
    )
    .await
    .context("building meta raft cluster")?
    .with_label("meta");

    // Read the persisted ClusterState — on a warm restart this
    // contains any `SetPartitionReplicas` entries applied since
    // bootstrap, so the partition-spin-up loop below sees the
    // current placement, not the original round-robin assignment.
    let persisted_state = meta_raft.current_state().await;
    let effective_replica_map = persisted_state
        .partition_replica_map
        .clone()
        .unwrap_or_else(|| (*replica_map).clone());
    let replica_map = Arc::new(effective_replica_map);

    // Per-partition groups — only the ones whose replica set
    // contains this peer.
    let partitioner = Partitioner::new(config.num_partitions);
    let mut partitions: HashMap<PartitionId, Arc<RaftCluster>> = HashMap::new();
    let mut partition_appliers: HashMap<PartitionId, Arc<PartitionGraphApplier>> = HashMap::new();
    for p in 0..config.num_partitions {
        let partition_id = PartitionId(p);
        if !replica_map.contains(partition_id, PeerId(config.self_id)) {
            continue;
        }
        let dir = config.data_dir.join("raft").join(format!("p-{p}"));
        std::fs::create_dir_all(&dir)
            .with_context(|| format!("creating partition raft dir {}", dir.display()))?;
        let applier_concrete = Arc::new(PartitionGraphApplier::new(
            partition_id,
            store.clone(),
            partitioner.clone(),
        ));
        let applier: Arc<dyn GraphStateMachine> = applier_concrete.clone();
        let raft = RaftCluster::open_persistent(
            config.self_id,
            &dir,
            initial_state.clone(),
            base_network.for_group(RaftGroupTarget::Partition(partition_id)),
            Some(applier),
        )
        .await
        .with_context(|| format!("building partition {p} raft cluster"))?
        .with_label(format!("p-{p}"));
        partitions.insert(partition_id, Arc::new(raft));
        partition_appliers.insert(partition_id, applier_concrete);
    }

    let multi_raft = Arc::new(MultiRaftCluster::new(
        config.self_id,
        Arc::new(meta_raft),
        partitions,
        partition_appliers,
        replica_map,
    ));

    // Dispatch table for inbound MeshRaft RPCs. Cloned so we can
    // hand one clone to the gRPC service (which moves it into
    // tonic) and stash another in `MultiRaftCluster` for the
    // runtime-spin-up path. Both clones share the same
    // `Arc<RwLock<...>>` for the partitions table — registering a
    // new partition through either becomes visible to the other.
    let registry = multi_raft.build_registry();
    multi_raft.attach_dispatch_registry(registry.clone());
    let raft_service = MeshRaftService::with_registry(registry);

    // Open the coordinator log so cross-partition decisions are
    // durable across a coordinator crash. The participant log isn't
    // opened in multi-raft mode — partition Raft logs are the
    // durable participant record for staged commands.
    let coordinator_log = Arc::new(
        crate::CoordinatorLog::open(coordinator_log_path(&config.data_dir))
            .context("opening coordinator log for multi-raft")?,
    );

    // Wire writes through the multi-raft path: single-partition
    // writes propose through the partition Raft (server-side
    // forwarded to the leader peer when this isn't it); cross-
    // partition writes ride a Spanner-style 2PC. Reads still hit
    // the local store directly. The trigger registry was opened
    // above (and attached to the meta applier); pass it through
    // to the procedure factory so `apoc.trigger.list()` reads
    // from the same instance the applier refreshes.
    let store_for_triggers = store.clone();
    let linearizable = matches!(
        config.read_consistency,
        Some(crate::config::ReadConsistency::Linearizable)
    );
    let query_timeout = config
        .query_timeout_seconds
        .map(std::time::Duration::from_secs);
    let service = apply_apoc_import_with_registry(
        MeshService::with_multi_raft(store, multi_raft.clone())
            .with_coordinator_log(Some(coordinator_log))
            .with_client_tls(client_tls)
            .with_read_consistency(linearizable)
            .with_query_timeout(query_timeout)
            .with_query_max_rows(config.query_max_rows)
            .with_max_concurrent_queries(config.max_concurrent_queries)
            .with_audit_log(open_audit_log(config)?)
            .with_plan_cache(config.plan_cache_size.map(meshdb_rpc::PlanCache::new)),
        config,
        &store_for_triggers,
        #[cfg(feature = "apoc-trigger")]
        shared_trigger_registry,
    );

    Ok(ServerComponents {
        service,
        raft: None,
        raft_service: Some(raft_service),
        multi_raft: Some(multi_raft),
    })
}

/// Initialize the seed peer's Raft instance with the configured member list.
/// No-op on non-seed peers and on single-node configs.
pub async fn initialize_if_seed(config: &ServerConfig, raft: &RaftCluster) -> Result<()> {
    if !config.bootstrap {
        return Ok(());
    }
    let members: BTreeMap<u64, BasicNode> = config
        .peers
        .iter()
        .map(|p| (p.id, BasicNode::new(p.address.clone())))
        .collect();
    raft.initialize(members)
        .await
        .context("seeding raft cluster")
}

/// Instantiate a partition Raft group at runtime — used by the
/// rebalancing path when a peer is added as a replica of a
/// partition it didn't bootstrap with. Builds the rocksdb dir, the
/// `RaftCluster`, and the `PartitionGraphApplier`, then registers
/// the group with both `MultiRaftCluster` (so the local write path
/// finds it) and the dispatch `RaftGroupRegistry` (so inbound
/// `MeshRaft` RPCs from the existing replicas land).
///
/// Caller is responsible for ensuring the partition Raft on at
/// least one existing peer subsequently calls `change_membership`
/// to add this peer as a voter — otherwise this function builds
/// an isolated group that never replicates from anyone.
pub async fn instantiate_partition_group(
    config: &ServerConfig,
    multi_raft: &Arc<MultiRaftCluster>,
    store: Arc<dyn StorageEngine>,
    partition: PartitionId,
    base_network: &GrpcNetwork,
) -> Result<()> {
    if multi_raft.hosts_partition(partition) {
        // Already instantiated on this peer — re-running the API
        // is idempotent so a partial failure can be retried safely.
        return Ok(());
    }
    let dir = config
        .data_dir
        .join("raft")
        .join(format!("p-{}", partition.0));
    std::fs::create_dir_all(&dir)
        .with_context(|| format!("creating partition raft dir {}", dir.display()))?;
    let partitioner = Partitioner::new(config.num_partitions);
    let applier_concrete = Arc::new(PartitionGraphApplier::new(
        partition,
        store.clone(),
        partitioner,
    ));
    let applier: Arc<dyn GraphStateMachine> = applier_concrete.clone();
    // Build initial state from the meta replica's current view —
    // openraft uses it only as the fallback for a fresh data dir,
    // and on a learner-join replication will overwrite anyway.
    let initial_state = multi_raft.meta.current_state().await;
    let raft = Arc::new(
        RaftCluster::open_persistent(
            config.self_id,
            &dir,
            initial_state,
            base_network.for_group(RaftGroupTarget::Partition(partition)),
            Some(applier),
        )
        .await
        .with_context(|| format!("building partition {} raft cluster", partition.0))?
        .with_label(format!("p-{}", partition.0)),
    );
    // Register with both maps. Order matters: the dispatch table
    // before MultiRaftCluster so an inbound RPC that lands during
    // the brief window can still be served (it'll find the new
    // group in the dispatch table; the multi-raft cluster is only
    // needed by the local write path, which the operator hasn't
    // started routing here yet).
    if let Some(reg) = multi_raft
        .dispatch_registry
        .read()
        .expect("dispatch_registry lock poisoned")
        .as_ref()
    {
        reg.register_partition(partition, Arc::new(raft.raft.clone()));
    }
    multi_raft.add_runtime_partition_group(partition, raft, applier_concrete);
    Ok(())
}

/// Initialize the multi-raft groups this peer is responsible for
/// seeding on first startup. Each Raft group's "seed" peer is
/// chosen deterministically — the lowest-id replica in the group's
/// replica set initializes that group. The metadata group's seed is
/// the peer with `bootstrap = true` (matching the legacy single-Raft
/// convention so an existing TOML config keeps working).
///
/// On a non-fresh data directory this re-call surfaces openraft's
/// `NotAllowed` error, which is treated as a no-op since the
/// persisted state is the source of truth after restart.
pub async fn initialize_multi_raft_if_seed(
    config: &ServerConfig,
    multi_raft: &MultiRaftCluster,
) -> Result<()> {
    let all_members: BTreeMap<u64, BasicNode> = config
        .peers
        .iter()
        .map(|p| (p.id, BasicNode::new(p.address.clone())))
        .collect();

    if config.bootstrap {
        if let Err(e) = multi_raft.meta.initialize(all_members.clone()).await {
            // Already initialized → fine, the persisted state takes over.
            tracing::info!(error = %e, "meta-raft initialize skipped (likely already initialized)");
        }
    }

    // Each partition group's seed is the first peer in its replica
    // set (lowest NodeId). Skip when this peer isn't that one — the
    // designated seed will run on its own peer.
    for (partition, raft) in multi_raft.partitions_snapshot() {
        let replicas = multi_raft.replica_map.replicas(partition);
        let seed = replicas
            .iter()
            .map(|p| p.0)
            .min()
            .expect("non-empty replicas");
        if seed != config.self_id {
            continue;
        }
        let members: BTreeMap<u64, BasicNode> = replicas
            .iter()
            .filter_map(|peer_id| all_members.get(&peer_id.0).map(|n| (peer_id.0, n.clone())))
            .collect();
        if let Err(e) = raft.initialize(members).await {
            tracing::info!(
                partition = ?partition,
                error = %e,
                "partition raft initialize skipped (likely already initialized)"
            );
        }
    }

    Ok(())
}

/// Bind the configured listen address and serve until Ctrl-C.
///
/// For multi-peer configs this also bootstraps the Raft cluster (if
/// `bootstrap = true`) after the gRPC server is listening, so the seed peer
/// can immediately replicate the initial membership entry to followers.
pub async fn serve(config: ServerConfig) -> Result<()> {
    let components = build_components(&config).await?;
    let listener = TcpListener::bind(&config.listen_address)
        .await
        .with_context(|| format!("binding {}", config.listen_address))?;
    let local_addr = listener.local_addr()?;
    tracing::info!(addr = %local_addr, "meshdb-server listening");

    let raft_handle = components.raft.clone();
    let multi_raft_handle = components.multi_raft.clone();
    let ServerComponents {
        service,
        raft: _,
        raft_service,
        multi_raft: _,
    } = components;
    // Cheap clones (every field is Arc-wrapped). Lets the recovery
    // call below reuse the service after the server task has taken
    // ownership of the original.
    let service_for_recovery = service.clone();

    // Rehydrate 2PC participant-side state from the on-disk log
    // BEFORE the gRPC server accepts new traffic. Without this step,
    // a COMMIT arriving immediately after restart would see empty
    // staging for a txid that crashed mid-PREPARE and fail with
    // `failed_precondition` instead of succeeding. No-op in
    // single-node / Raft modes where the participant log isn't
    // configured.
    if let Err(e) = service.recover_participant_staging() {
        tracing::warn!(error = %e, "participant staging recovery failed; continuing");
    }

    // Poll every peer's `ResolveTransaction` RPC to learn the
    // coordinator's decision for any in-doubt txid this peer is
    // holding. Short-circuits the staging TTL in the common case
    // where the coordinator is still alive but this peer crashed
    // mid-flight.
    if let Err(e) = service.recover_participant_decisions().await {
        tracing::warn!(error = %e, "participant decision recovery failed; continuing");
    }

    // Reconcile any transactions left unfinished by a prior crash
    // before we start accepting new traffic. In single-node and Raft
    // modes this is a no-op (no coordinator log). In routing mode,
    // recovery drains the persisted log by pushing committed
    // transactions forward to completion on every peer and rolling
    // back un-decided ones; failures talking to unreachable peers
    // are logged and skipped so a partial cluster restart can still
    // make progress.
    if let Err(e) = service.recover_pending_transactions().await {
        tracing::warn!(error = %e, "coordinator recovery failed; continuing");
    }

    // Bound-lifetime garbage collection for 2PC participant staging.
    // Drops any `BatchWrite(Prepare)` entry whose coordinator never
    // followed up with COMMIT / ABORT within DEFAULT_STAGING_TTL,
    // covering the case where a coordinator disappears without ever
    // restarting to trigger the recovery log path.
    let staging_sweeper = service.spawn_staging_sweeper(meshdb_rpc::DEFAULT_SWEEP_INTERVAL);

    // Background rotator for the coordinator's durable 2PC log.
    // Periodically drops already-completed transaction entries so
    // the log doesn't grow linearly with lifetime tx count in a
    // long-running cluster. Returns None in single-node / Raft
    // modes where there's no coordinator log to rotate.
    let log_rotator = service.spawn_log_rotator(
        meshdb_rpc::DEFAULT_ROTATION_INTERVAL,
        meshdb_rpc::DEFAULT_MIN_COMPLETED,
    );

    // Periodic in-doubt recovery loop for multi-raft mode. Catches
    // a coordinator that fsynced CommitDecision then crashed while
    // the cluster otherwise stayed healthy — startup recovery
    // already ran inline; this loop covers PREPAREs that arrive
    // post-startup. Returns None in every other mode.
    let multi_raft_recovery_loop =
        service.spawn_multi_raft_recovery_loop(meshdb_rpc::DEFAULT_RECOVERY_INTERVAL);

    // Periodic apply-lag metrics poller for multi-raft mode. Reads
    // openraft's metrics watcher every 10s and updates the per-group
    // `mesh_multiraft_apply_lag` and `mesh_multiraft_last_applied`
    // gauges. Returns None in every other mode.
    let multi_raft_metrics_poller =
        service.spawn_multi_raft_metrics_poller(std::time::Duration::from_secs(10));

    // Optional Bolt listener. Binds before we start the gRPC server so
    // that a port-in-use error at startup is immediately fatal rather
    // than surfacing only on the first Bolt client connection.
    let service_arc = Arc::new(service.clone());
    let bolt_auth = config.bolt_auth.clone().map(Arc::new);
    let bolt_task = if let Some(bolt_addr) = config.bolt_address.as_ref() {
        let bolt_listener = TcpListener::bind(bolt_addr)
            .await
            .with_context(|| format!("binding bolt {}", bolt_addr))?;
        let bolt_local = bolt_listener.local_addr()?;
        let auth_state = if bolt_auth.is_some() {
            "enabled"
        } else {
            "disabled"
        };
        let (tls_acceptor, _tls_reload_task) = if let Some(tls_cfg) = config.bolt_tls.as_ref() {
            bolt::install_default_crypto_provider();
            match tls_cfg.reload_interval_seconds {
                Some(interval_secs) => {
                    let (acceptor, handle) = bolt::build_tls_acceptor_with_reload(
                        &tls_cfg.cert_path,
                        &tls_cfg.key_path,
                        std::time::Duration::from_secs(interval_secs),
                    )
                    .context("building bolt tls acceptor with hot-reload")?;
                    (Some(acceptor), Some(handle))
                }
                None => {
                    let acceptor = bolt::build_tls_acceptor(&tls_cfg.cert_path, &tls_cfg.key_path)
                        .context("building bolt tls acceptor")?;
                    (Some(acceptor), None)
                }
            }
        } else {
            (None, None)
        };
        let tls_state = if tls_acceptor.is_some() {
            "enabled"
        } else {
            "disabled"
        };
        let advertised = config
            .resolved_bolt_versions()
            .map_err(|e| anyhow::anyhow!("bolt_advertised_versions: {e}"))?
            .map(Arc::new);
        let advertised_state = match advertised.as_deref() {
            Some(v) => format!("clamped to {} version(s)", v.len()),
            None => "default (all 6)".to_string(),
        };
        tracing::info!(addr = %bolt_local, auth = auth_state, tls = tls_state, advertised = %advertised_state, "meshdb-server bolt listening");
        let bolt_service = service_arc.clone();
        let bolt_auth_clone = bolt_auth.clone();
        // Assemble the ROUTE context. `local_advertised` is echoed
        // back in single-node mode; `peers` carries every configured
        // peer + its optional Bolt address so routing-mode tables
        // list siblings; `raft` lets the handler resolve the current
        // leader at ROUTE time for a spec-correct WRITE role.
        //
        // Peers are snapshotted from `config.peers` — static for the
        // server's lifetime — rather than pulled from the live
        // Raft/Routing state because driver-facing Bolt endpoints
        // don't change on membership churn and the static list
        // already has what we need.
        let peers_for_route: Vec<Peer> = config
            .peers
            .iter()
            .map(|p| {
                let base = Peer::new(PeerId(p.id), p.address.clone());
                match &p.bolt_address {
                    Some(ba) => base.with_bolt_address(ba),
                    None => base,
                }
            })
            .collect();
        // `local_advertised` is what the ROUTE handler echoes back
        // in the routing table. Default to the literal bind string
        // (e.g. `"localhost:7687"`) so a `bolt_address = "localhost"`
        // config works as-is, but let operators override with
        // `bolt_advertised_address` when the two must diverge — bind
        // to a private IP, advertise a public DNS name. The test
        // harness also uses the override to bind IPv4-deterministic
        // `127.0.0.1` while advertising `localhost` for SNI.
        let local_advertised = config
            .bolt_advertised_address
            .clone()
            .unwrap_or_else(|| bolt_addr.to_string());
        let route_ctx = Arc::new(bolt::RouteContext {
            local_advertised,
            peers: Arc::new(Membership::new(peers_for_route)),
            raft: raft_handle.clone(),
            multi_raft: components.multi_raft.clone(),
            // Multi-raft has dynamic partition leadership and
            // membership churn, so default to a 30-second TTL —
            // drivers re-fetch the routing table fast enough to
            // observe leader transfers and node-join events without
            // hammering the ROUTE handler. Other modes inherit the
            // historical effectively-infinite TTL unless the
            // operator opts in. The override knob is
            // [`ServerConfig::routing_ttl_seconds`].
            routing_ttl_seconds: config.routing_ttl_seconds.or_else(|| {
                if components.multi_raft.is_some() {
                    Some(30)
                } else {
                    None
                }
            }),
        });
        Some(tokio::spawn(async move {
            if let Err(e) = bolt::run_listener(
                bolt_listener,
                bolt_service,
                bolt_auth_clone,
                tls_acceptor,
                advertised,
                route_ctx,
            )
            .await
            {
                tracing::error!(error = %e, "bolt listener exited");
            }
        }))
    } else {
        None
    };

    // Optional Prometheus metrics endpoint. Same fail-fast bind
    // model as Bolt — port-in-use is fatal at startup rather than
    // a confusing 500 the first time something scrapes.
    // Readiness state shared between the /readyz handler and the
    // shutdown drain path. `mark_draining()` flips the bit when
    // SIGTERM/SIGINT fires; the kubelet sees the next probe
    // return 503 and stops sending traffic before drain_leadership
    // begins.
    let readiness = metrics::ReadinessState {
        is_draining: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
        multi_raft: multi_raft_handle.clone(),
    };

    let metrics_task = if let Some(metrics_addr) = config.metrics_address.as_ref() {
        let metrics_listener = TcpListener::bind(metrics_addr)
            .await
            .with_context(|| format!("binding metrics {}", metrics_addr))?;
        let metrics_local = metrics_listener.local_addr()?;
        tracing::info!(addr = %metrics_local, "meshdb-server metrics listening");
        let readiness_for_listener = readiness.clone();
        Some(tokio::spawn(async move {
            if let Err(e) = metrics::run_listener(metrics_listener, readiness_for_listener).await {
                tracing::error!(error = %e, "metrics listener exited");
            }
        }))
    } else {
        None
    };

    // Server-side TLS identity for the gRPC listener. Either:
    //   * Static (`reload_interval_seconds` unset): tonic's
    //     `ServerTlsConfig` loads the cert once at startup.
    //   * Hot-reload (`reload_interval_seconds = N`): we build a
    //     `tokio_rustls::TlsAcceptor` with a `ResolvesServerCert`
    //     hook and terminate TLS ourselves; tonic gets plaintext
    //     streams via `serve_with_incoming_shutdown`. This is
    //     necessary because tonic 0.12's `ServerTlsConfig` doesn't
    //     expose the cert resolver, so a static-tonic-tls listener
    //     can't rotate without a restart.
    enum GrpcTls {
        Static(tonic::transport::ServerTlsConfig),
        HotReload(tokio_rustls::TlsAcceptor, tokio::task::JoinHandle<()>),
    }
    let grpc_tls = if let Some(tls_cfg) = config.grpc_tls.as_ref() {
        meshdb_rpc::tls::install_default_crypto_provider();
        match tls_cfg.reload_interval_seconds {
            Some(secs) => {
                let (acceptor, handle) = match &tls_cfg.client_ca_path {
                    Some(client_ca) => tls_reload::build_hot_reloading_tls_acceptor_mtls(
                        &tls_cfg.cert_path,
                        &tls_cfg.key_path,
                        client_ca,
                        Duration::from_secs(secs),
                    )
                    .context("building grpc tls hot-reloading acceptor (mtls)")?,
                    None => tls_reload::build_hot_reloading_tls_acceptor(
                        &tls_cfg.cert_path,
                        &tls_cfg.key_path,
                        Duration::from_secs(secs),
                    )
                    .context("building grpc tls hot-reloading acceptor")?,
                };
                Some(GrpcTls::HotReload(acceptor, handle))
            }
            None => Some(GrpcTls::Static(match &tls_cfg.client_ca_path {
                Some(client_ca) => meshdb_rpc::tls::build_server_tls_config_mtls(
                    &tls_cfg.cert_path,
                    &tls_cfg.key_path,
                    client_ca,
                )
                .context("building grpc server tls config (mtls)")?,
                None => {
                    meshdb_rpc::tls::build_server_tls_config(&tls_cfg.cert_path, &tls_cfg.key_path)
                        .context("building grpc server tls config")?
                }
            })),
        }
    } else {
        None
    };
    let grpc_tls_state = match &grpc_tls {
        Some(GrpcTls::Static(_)) => "static",
        Some(GrpcTls::HotReload(_, _)) => "hot-reload",
        None => "disabled",
    };
    if grpc_tls.is_some() {
        tracing::info!(addr = %local_addr, mode = grpc_tls_state, "meshdb-server grpc tls enabled");
    }

    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();

    let server_task = tokio::spawn(async move {
        let services = (
            service.clone().into_query_server(),
            service.clone().into_write_server(),
            raft_service.map(|rs| rs.into_server()),
        );

        let mut builder = Server::builder();
        if let Some(GrpcTls::Static(tls)) = &grpc_tls {
            builder = builder
                .tls_config(tls.clone())
                .context("applying grpc tls config")?;
        }
        let mut router = builder.add_service(services.0).add_service(services.1);
        if let Some(rs) = services.2 {
            router = router.add_service(rs);
        }
        let shutdown = async {
            let _ = shutdown_rx.await;
            tracing::info!("received shutdown signal");
        };

        match grpc_tls {
            Some(GrpcTls::HotReload(acceptor, _reload_handle)) => {
                // Custom incoming: TCP accept + TLS handshake +
                // forward as `TlsStream<TcpStream>`. tonic 0.12's
                // `Connected` impl for `TlsStream<T: Connected>`
                // gives us connection metadata for free. Per-handshake
                // failures log + drop without killing the listener.
                let (tx, rx) = tokio::sync::mpsc::channel::<
                    std::result::Result<
                        tokio_rustls::server::TlsStream<tokio::net::TcpStream>,
                        std::io::Error,
                    >,
                >(32);
                tokio::spawn(async move {
                    loop {
                        match listener.accept().await {
                            Ok((tcp, _peer)) => {
                                let acceptor = acceptor.clone();
                                let tx = tx.clone();
                                tokio::spawn(async move {
                                    match acceptor.accept(tcp).await {
                                        Ok(s) => {
                                            let _ = tx.send(Ok(s)).await;
                                        }
                                        Err(e) => {
                                            tracing::warn!(
                                                error = %e,
                                                "grpc tls handshake failed; dropping connection"
                                            );
                                        }
                                    }
                                });
                            }
                            Err(e) => {
                                tracing::warn!(error = %e, "grpc tcp accept failed; retrying");
                                tokio::time::sleep(Duration::from_millis(50)).await;
                            }
                        }
                    }
                });
                let stream = tokio_stream::wrappers::ReceiverStream::new(rx);
                router
                    .serve_with_incoming_shutdown(stream, shutdown)
                    .await
                    .context("gRPC server error (hot-reload tls)")
            }
            _ => router
                .serve_with_incoming_shutdown(TcpListenerStream::new(listener), shutdown)
                .await
                .context("gRPC server error"),
        }
    });

    // Give the server a moment to bind before we start trying to send Raft
    // RPCs to peers.
    tokio::time::sleep(Duration::from_millis(100)).await;

    if let Some(raft) = &raft_handle {
        if let Err(e) = initialize_if_seed(&config, raft).await {
            // Don't crash the server on bootstrap failure — log and continue.
            // openraft retries network errors, so transient peer-unreachable
            // failures during initial replication eventually resolve.
            tracing::warn!("raft bootstrap failed: {e:#}");
        } else if config.bootstrap {
            tracing::info!(peers = config.peers.len(), "raft cluster bootstrapped");
        }
    }

    if let Some(mr) = &multi_raft_handle {
        if let Err(e) = initialize_multi_raft_if_seed(&config, mr).await {
            tracing::warn!("multi-raft bootstrap failed: {e:#}");
        } else {
            tracing::info!(
                peers = config.peers.len(),
                partitions = mr.partitions_snapshot().len(),
                "multi-raft groups bootstrapped on this peer"
            );
        }
        // In-doubt recovery: any PreparedTx left behind by a prior
        // coordinator crash gets resolved via ResolveTransaction
        // polling across peers. Runs once at startup; the
        // coordinator-log + partition-Raft pair drives it forward.
        // Failures here are logged but don't block the listener —
        // periodic re-runs (future addition) cover transient
        // peer-unreachable cases.
        if let Err(e) = service_for_recovery.recover_multi_raft_in_doubt().await {
            tracing::warn!("multi-raft in-doubt recovery failed: {e:#}");
        }
    }

    // Wait for SIGINT (Ctrl-C in interactive shells) or SIGTERM
    // (k8s / systemd graceful-stop). Whichever fires first wins.
    wait_for_shutdown_signal().await;

    // Flip readiness immediately so the kubelet sees /readyz
    // return 503 on the next probe and stops sending traffic.
    // This happens *before* the drain so the kubelet has time to
    // pull the peer out of its Service endpoints; otherwise
    // requests would still arrive mid-drain and time out.
    readiness.mark_draining();

    // Multi-raft mode: drain leadership before tearing down the
    // listener so partition followers take over in-place rather
    // than electing from cold after we vanish. Bounded by
    // `shutdown_drain_timeout_seconds` (default 30) so a stuck
    // partition can't make the process hang during a planned
    // restart.
    if let Some(_mr) = service_for_recovery.multi_raft_handle() {
        let timeout = config.shutdown_drain_timeout_seconds.unwrap_or(30);
        let drain_deadline = Duration::from_secs(timeout);
        tracing::info!(
            timeout_seconds = timeout,
            "draining partition leadership before shutdown"
        );
        let drain = tokio::time::timeout(
            drain_deadline,
            service_for_recovery.drain_leadership(drain_deadline),
        )
        .await;
        match drain {
            Ok((stepped_down, errors)) => {
                tracing::info!(
                    partitions_drained = stepped_down.len(),
                    errors = errors.len(),
                    "leadership drain complete"
                );
                for (p, e) in &errors {
                    tracing::warn!(partition = ?p, error = %e, "drain error");
                }
            }
            Err(_) => tracing::warn!(
                "leadership drain timed out; some partitions will re-elect from cold"
            ),
        }
    }

    let _ = shutdown_tx.send(());

    staging_sweeper.abort();
    let _ = staging_sweeper.await;

    if let Some(handle) = log_rotator {
        handle.abort();
        let _ = handle.await;
    }

    if let Some(handle) = multi_raft_recovery_loop {
        handle.abort();
        let _ = handle.await;
    }

    if let Some(handle) = multi_raft_metrics_poller {
        handle.abort();
        let _ = handle.await;
    }

    if let Some(handle) = bolt_task {
        handle.abort();
        let _ = handle.await;
    }

    if let Some(handle) = metrics_task {
        handle.abort();
        let _ = handle.await;
    }

    match server_task.await {
        Ok(result) => result?,
        Err(e) => return Err(anyhow!("server task panicked: {e}")),
    }
    Ok(())
}

/// Wait for either a SIGINT (Ctrl-C) or a SIGTERM (the
/// kubelet/systemd graceful-stop signal) and return as soon as
/// either fires. On non-unix platforms only SIGINT is wired.
async fn wait_for_shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        let mut sigterm = match signal(SignalKind::terminate()) {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!(error = %e, "failed to install SIGTERM handler; falling back to SIGINT only");
                let _ = tokio::signal::ctrl_c().await;
                return;
            }
        };
        tokio::select! {
            _ = tokio::signal::ctrl_c() => tracing::info!("received SIGINT"),
            _ = sigterm.recv() => tracing::info!("received SIGTERM"),
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
        tracing::info!("received SIGINT");
    }
}
