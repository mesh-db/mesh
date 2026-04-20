use crate::proto::mesh_query_client::MeshQueryClient;
use crate::proto::mesh_write_client::MeshWriteClient;
use mesh_cluster::{Cluster, PeerId};
use std::collections::HashMap;
use std::sync::Arc;
use thiserror::Error;
use tonic::transport::{Channel, ClientTlsConfig, Endpoint};

#[derive(Debug, Error)]
pub enum RoutingError {
    #[error("invalid endpoint address for {peer}: {message}")]
    InvalidEndpoint { peer: PeerId, message: String },
    #[error("applying tls config for {peer}: {message}")]
    TlsConfig { peer: PeerId, message: String },
}

/// Routing layer on top of a [`Cluster`]. Holds a lazy gRPC [`Channel`] per
/// remote peer and hands out fresh [`MeshQueryClient`] handles on demand.
#[derive(Debug, Clone)]
pub struct Routing {
    cluster: Arc<Cluster>,
    channels: HashMap<PeerId, Channel>,
}

impl Routing {
    pub fn new(cluster: Arc<Cluster>) -> Result<Self, RoutingError> {
        Self::build(cluster, None)
    }

    /// Same as [`Routing::new`] but wraps every peer channel in a TLS
    /// configuration, switching the URI scheme from `http://` to
    /// `https://`. Use this in clusters where the gRPC listener
    /// terminates TLS.
    pub fn with_tls(cluster: Arc<Cluster>, tls: ClientTlsConfig) -> Result<Self, RoutingError> {
        Self::build(cluster, Some(tls))
    }

    fn build(cluster: Arc<Cluster>, tls: Option<ClientTlsConfig>) -> Result<Self, RoutingError> {
        let mut channels = HashMap::new();
        let scheme = if tls.is_some() { "https" } else { "http" };
        for (peer_id, addr) in cluster.membership().iter() {
            if peer_id == cluster.self_id() {
                continue;
            }
            let uri = format!("{scheme}://{addr}");
            let mut endpoint =
                Endpoint::from_shared(uri).map_err(|e| RoutingError::InvalidEndpoint {
                    peer: peer_id,
                    message: e.to_string(),
                })?;
            if let Some(tls) = tls.clone() {
                endpoint = endpoint
                    .tls_config(tls)
                    .map_err(|e| RoutingError::TlsConfig {
                        peer: peer_id,
                        message: e.to_string(),
                    })?;
            }
            channels.insert(peer_id, endpoint.connect_lazy());
        }
        Ok(Self { cluster, channels })
    }

    pub fn cluster(&self) -> &Cluster {
        &self.cluster
    }

    /// Returns the raw [`Channel`] to `peer`, or `None` if not registered
    /// (either the local peer or an unknown member).
    pub fn channel_for(&self, peer: PeerId) -> Option<Channel> {
        self.channels.get(&peer).cloned()
    }

    /// Query-side client backed by the cached channel.
    pub fn query_client(&self, peer: PeerId) -> Option<MeshQueryClient<Channel>> {
        self.channel_for(peer).map(MeshQueryClient::new)
    }

    /// Write-side client backed by the cached channel.
    pub fn write_client(&self, peer: PeerId) -> Option<MeshWriteClient<Channel>> {
        self.channel_for(peer).map(MeshWriteClient::new)
    }
}
