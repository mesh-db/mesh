pub mod bolt;
pub mod config;
pub mod value_conv;

use anyhow::{anyhow, Context, Result};
use config::ServerConfig;
use mesh_cluster::raft::{BasicNode, GraphStateMachine, RaftCluster};
use mesh_cluster::{Cluster, ClusterState, Membership, PartitionMap, Peer, PeerId};
use mesh_rpc::{GrpcNetwork, MeshRaftService, MeshService, Routing, StoreGraphApplier};
use mesh_storage::Store;
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

/// Build the storage-backed [`MeshService`] for a single-node deployment.
/// Kept as a sync entry point for legacy callers; multi-peer deployments
/// should go through [`build_components`] instead.
pub fn build_service(config: &ServerConfig) -> Result<MeshService> {
    std::fs::create_dir_all(&config.data_dir)
        .with_context(|| format!("creating data dir {}", config.data_dir.display()))?;
    let store = Arc::new(
        Store::open(&config.data_dir)
            .with_context(|| format!("opening store at {}", config.data_dir.display()))?,
    );

    if config.peers.is_empty() {
        return Ok(MeshService::new(store));
    }

    let peers: Vec<Peer> = config
        .peers
        .iter()
        .map(|p| Peer::new(PeerId(p.id), p.address.clone()))
        .collect();

    let cluster = Arc::new(
        Cluster::new(PeerId(config.self_id), config.num_partitions, peers)
            .context("building cluster")?,
    );
    let routing = Arc::new(Routing::new(cluster).context("building routing")?);
    Ok(MeshService::with_routing(store, routing))
}

/// Build the full [`ServerComponents`] bundle. For multi-peer configs this
/// constructs a [`RaftCluster`] (with a [`StoreGraphApplier`] so graph
/// writes replicate via the state machine), wires the [`MeshService`]'s
/// write path through `propose_graph`, and exposes a [`MeshRaftService`]
/// for incoming Raft RPCs. Single-node configs return a local-only service
/// with `raft: None`.
pub async fn build_components(config: &ServerConfig) -> Result<ServerComponents> {
    std::fs::create_dir_all(&config.data_dir)
        .with_context(|| format!("creating data dir {}", config.data_dir.display()))?;
    let store = Arc::new(
        Store::open(&config.data_dir)
            .with_context(|| format!("opening store at {}", config.data_dir.display()))?,
    );

    if config.peers.is_empty() {
        return Ok(ServerComponents {
            service: MeshService::new(store),
            raft: None,
            raft_service: None,
        });
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
    let network = GrpcNetwork::new(peer_map).context("building grpc network")?;

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
    let service = MeshService::with_raft(store, raft.clone());

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
    tracing::info!(addr = %local_addr, "mesh-server listening");

    let raft_handle = components.raft.clone();
    let ServerComponents {
        service,
        raft: _,
        raft_service,
    } = components;

    // Optional Bolt listener. Binds before we start the gRPC server so
    // that a port-in-use error at startup is immediately fatal rather
    // than surfacing only on the first Bolt client connection.
    let service_arc = Arc::new(service.clone());
    let bolt_task = if let Some(bolt_addr) = config.bolt_address.as_ref() {
        let bolt_listener = TcpListener::bind(bolt_addr)
            .await
            .with_context(|| format!("binding bolt {}", bolt_addr))?;
        let bolt_local = bolt_listener.local_addr()?;
        tracing::info!(addr = %bolt_local, "mesh-server bolt listening");
        let bolt_service = service_arc.clone();
        Some(tokio::spawn(async move {
            if let Err(e) = bolt::run_listener(bolt_listener, bolt_service).await {
                tracing::error!(error = %e, "bolt listener exited");
            }
        }))
    } else {
        None
    };

    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();

    let server_task = tokio::spawn(async move {
        let mut router = Server::builder()
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

    if let Some(handle) = bolt_task {
        handle.abort();
        let _ = handle.await;
    }

    match server_task.await {
        Ok(result) => result?,
        Err(e) => return Err(anyhow!("server task panicked: {e}")),
    }
    Ok(())
}
