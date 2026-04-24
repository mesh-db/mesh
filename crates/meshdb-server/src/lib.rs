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
pub mod value_conv;

use anyhow::{anyhow, Context, Result};
use config::{ClusterMode, ServerConfig};
use meshdb_cluster::raft::{BasicNode, GraphStateMachine, RaftCluster};
use meshdb_cluster::{Cluster, ClusterState, Membership, PartitionMap, Peer, PeerId};
use meshdb_rpc::{
    CoordinatorLog, GrpcNetwork, MeshRaftService, MeshService, ParticipantLog, Routing,
    StoreGraphApplier,
};
use meshdb_storage::{RocksDbStorageEngine, StorageEngine};
use std::collections::BTreeMap;
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
    #[cfg(feature = "apoc-load")]
    let import_cfg = config.apoc_import.clone();
    #[cfg(feature = "apoc-trigger")]
    let trigger_registry = match meshdb_executor::apoc_trigger::TriggerRegistry::from_storage(
        store.clone(),
    ) {
        Ok(r) => Some(r),
        Err(e) => {
            tracing::warn!(error = %e, "loading triggers from storage failed; apoc.trigger.* disabled");
            None
        }
    };
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
        .map(|p| Peer::new(PeerId(p.id), p.address.clone()))
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
    let store_for_triggers = store.clone();
    Ok(apply_apoc_import(
        MeshService::with_routing_and_log(store, routing, Some(log))
            .with_participant_log(Some(participant_log))
            .with_client_tls(client_tls),
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
    let client = meshdb_rpc::tls::build_client_tls_config(&tls_cfg.ca_path)
        .context("building grpc client tls config")?;
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
pub async fn build_components(config: &ServerConfig) -> Result<ServerComponents> {
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
            return Ok(ServerComponents {
                service: apply_apoc_import(MeshService::new(store), config, &store_for_triggers),
                raft: None,
                raft_service: None,
            });
        }
        ClusterMode::Routing => {
            return Ok(ServerComponents {
                service: build_routing_service(config, store)?,
                raft: None,
                raft_service: None,
            });
        }
        ClusterMode::Raft => {
            // Fall through to the Raft bootstrap path below.
        }
    }

    // Build the initial cluster state Raft will manage.
    let peers: Vec<Peer> = config
        .peers
        .iter()
        .map(|p| Peer::new(PeerId(p.id), p.address.clone()))
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
        MeshService::with_raft(store, raft.clone()).with_client_tls(client_tls),
        config,
        &store_for_triggers,
    );

    Ok(ServerComponents {
        service,
        raft: Some(raft),
        raft_service: Some(raft_service),
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
    let ServerComponents {
        service,
        raft: _,
        raft_service,
    } = components;

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
        let tls_acceptor = if let Some(tls_cfg) = config.bolt_tls.as_ref() {
            bolt::install_default_crypto_provider();
            Some(
                bolt::build_tls_acceptor(&tls_cfg.cert_path, &tls_cfg.key_path)
                    .context("building bolt tls acceptor")?,
            )
        } else {
            None
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
        Some(tokio::spawn(async move {
            if let Err(e) = bolt::run_listener(
                bolt_listener,
                bolt_service,
                bolt_auth_clone,
                tls_acceptor,
                advertised,
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
    let metrics_task = if let Some(metrics_addr) = config.metrics_address.as_ref() {
        let metrics_listener = TcpListener::bind(metrics_addr)
            .await
            .with_context(|| format!("binding metrics {}", metrics_addr))?;
        let metrics_local = metrics_listener.local_addr()?;
        tracing::info!(addr = %metrics_local, "meshdb-server metrics listening");
        Some(tokio::spawn(async move {
            if let Err(e) = metrics::run_listener(metrics_listener).await {
                tracing::error!(error = %e, "metrics listener exited");
            }
        }))
    } else {
        None
    };

    // Server-side TLS identity for the gRPC listener. Built up-front
    // so a bad cert path fails before we spawn the server task.
    let grpc_server_tls = if let Some(tls_cfg) = config.grpc_tls.as_ref() {
        meshdb_rpc::tls::install_default_crypto_provider();
        Some(
            meshdb_rpc::tls::build_server_tls_config(&tls_cfg.cert_path, &tls_cfg.key_path)
                .context("building grpc server tls config")?,
        )
    } else {
        None
    };
    if grpc_server_tls.is_some() {
        tracing::info!(addr = %local_addr, "meshdb-server grpc tls enabled");
    }

    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();

    let server_task = tokio::spawn(async move {
        let mut builder = Server::builder();
        if let Some(tls) = grpc_server_tls {
            builder = builder
                .tls_config(tls)
                .context("applying grpc tls config")?;
        }
        let mut router = builder
            .add_service(service.clone().into_query_server())
            .add_service(service.into_write_server());
        if let Some(rs) = raft_service {
            router = router.add_service(rs.into_server());
        }
        let shutdown = async {
            let _ = shutdown_rx.await;
            tracing::info!("received shutdown signal");
        };
        router
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), shutdown)
            .await
            .context("gRPC server error")
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

    tokio::signal::ctrl_c().await.ok();
    let _ = shutdown_tx.send(());

    staging_sweeper.abort();
    let _ = staging_sweeper.await;

    if let Some(handle) = log_rotator {
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
