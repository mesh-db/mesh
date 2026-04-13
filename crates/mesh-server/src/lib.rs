pub mod config;

use anyhow::{Context, Result};
use config::ServerConfig;
use mesh_cluster::{Cluster, Peer, PeerId};
use mesh_rpc::{MeshService, Routing};
use mesh_storage::Store;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;

/// Construct the [`MeshService`] described by `config`: opens the on-disk
/// store and builds a [`Cluster`] + [`Routing`] when peers are configured.
/// Single-node deployments (empty `peers`) return a local-only service.
pub fn build_service(config: &ServerConfig) -> Result<MeshService> {
    std::fs::create_dir_all(&config.data_dir).with_context(|| {
        format!("creating data dir {}", config.data_dir.display())
    })?;
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

/// Bind the configured listen address and serve until Ctrl-C.
pub async fn serve(config: ServerConfig) -> Result<()> {
    let service = build_service(&config)?;
    let listener = TcpListener::bind(&config.listen_address)
        .await
        .with_context(|| format!("binding {}", config.listen_address))?;
    serve_with_listener(service, listener).await
}

/// Serve a pre-built service on a pre-bound listener until Ctrl-C.
/// Exposed so tests can bind `127.0.0.1:0` and capture the actual port.
pub async fn serve_with_listener(
    service: MeshService,
    listener: TcpListener,
) -> Result<()> {
    let local_addr = listener.local_addr()?;
    tracing::info!(addr = %local_addr, "mesh-server listening");

    let shutdown = async {
        let _ = tokio::signal::ctrl_c().await;
        tracing::info!("received shutdown signal");
    };

    Server::builder()
        .add_service(service.clone().into_query_server())
        .add_service(service.into_write_server())
        .serve_with_incoming_shutdown(TcpListenerStream::new(listener), shutdown)
        .await
        .context("gRPC server error")?;

    Ok(())
}
