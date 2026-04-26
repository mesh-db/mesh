//! Cluster-shared-secret authentication for inter-peer + admin gRPC.
//!
//! When a `cluster_auth.token` is set in the server config, every
//! inbound MeshWrite / MeshQuery / MeshRaftService RPC must carry
//! `authorization: Bearer <token>` metadata; mismatches are
//! rejected with `Unauthenticated`. Outgoing RPCs to peers get the
//! same token injected automatically.
//!
//! Bolt traffic isn't affected — Bolt has its own auth surface
//! (`bolt_auth`).
//!
//! Token = `None` is the no-auth fallback (the historical default,
//! suitable only for trusted-network deployments). Production
//! clusters must set a token. This module is a no-op on `None`
//! both client- and server-side, so existing test harnesses work
//! without changes.
//!
//! Wire format: standard HTTP `authorization: Bearer <token>`.
//! Constant-time compare on the server side to avoid timing
//! oracles that would leak the token byte-by-byte under load.

use tonic::{Request, Status};

const AUTH_HEADER: &str = "authorization";
const BEARER_PREFIX: &str = "Bearer ";

/// Holder for the cluster's shared secret. Cheap to clone and pass
/// into tonic interceptors. `None` disables both the inbound check
/// and the outbound injection.
#[derive(Debug, Clone, Default)]
pub struct ClusterAuth {
    token: Option<std::sync::Arc<String>>,
}

impl ClusterAuth {
    pub fn new(token: Option<String>) -> Self {
        Self {
            token: token.map(std::sync::Arc::new),
        }
    }

    /// Whether the validator / injector is active. `false` ⇒ both
    /// directions become no-ops; useful for the "trusted network"
    /// dev/test default.
    pub fn is_enabled(&self) -> bool {
        self.token.is_some()
    }

    /// Validate the bearer token in `req`. Used by the server-side
    /// interceptor on every inbound RPC. `None`-token disables.
    pub fn validate(&self, req: &Request<()>) -> Result<(), Status> {
        let Some(expected) = &self.token else {
            return Ok(());
        };
        let header = req
            .metadata()
            .get(AUTH_HEADER)
            .ok_or_else(|| Status::unauthenticated("missing authorization header"))?;
        let header_str = header
            .to_str()
            .map_err(|_| Status::unauthenticated("authorization header is not ASCII"))?;
        let token = header_str
            .strip_prefix(BEARER_PREFIX)
            .ok_or_else(|| Status::unauthenticated("malformed authorization header"))?;
        if !constant_time_eq(token.as_bytes(), expected.as_bytes()) {
            return Err(Status::unauthenticated("invalid cluster token"));
        }
        Ok(())
    }

    /// Inject the bearer token into `req`'s metadata. Used by the
    /// client-side interceptor on every outbound RPC. `None`-token
    /// disables.
    pub fn inject(&self, req: &mut Request<()>) -> Result<(), Status> {
        let Some(token) = &self.token else {
            return Ok(());
        };
        let header_value = format!("{BEARER_PREFIX}{token}")
            .parse()
            .map_err(|e| Status::internal(format!("invalid cluster token: {e}")))?;
        req.metadata_mut().insert(AUTH_HEADER, header_value);
        Ok(())
    }

    /// Build a tonic-compatible inbound interceptor closure that
    /// validates every request. Plug into
    /// `InterceptedService::new(server, server_interceptor())`.
    pub fn server_interceptor(
        &self,
    ) -> impl FnMut(Request<()>) -> Result<Request<()>, Status> + Clone + Send + Sync + 'static
    {
        let auth = self.clone();
        move |req: Request<()>| -> Result<Request<()>, Status> {
            auth.validate(&req)?;
            Ok(req)
        }
    }

    /// Build a tonic-compatible outbound interceptor closure that
    /// injects the bearer token into every request. Plug into
    /// `<Client>::with_interceptor(channel, client_interceptor())`.
    pub fn client_interceptor(
        &self,
    ) -> impl FnMut(Request<()>) -> Result<Request<()>, Status> + Clone + Send + Sync + 'static
    {
        let auth = self.clone();
        move |mut req: Request<()>| -> Result<Request<()>, Status> {
            auth.inject(&mut req)?;
            Ok(req)
        }
    }
}

/// Constant-time byte comparison. Both buffers must be the same
/// length to start; mismatched lengths fast-fail. Used for the
/// bearer-token check so a partial-match can't leak the token via
/// timing.
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff: u8 = 0;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    fn req() -> Request<()> {
        Request::new(())
    }

    #[test]
    fn disabled_passes_through() {
        let auth = ClusterAuth::new(None);
        assert!(!auth.is_enabled());
        let mut r = req();
        auth.inject(&mut r).unwrap();
        auth.validate(&r).unwrap();
    }

    #[test]
    fn injected_token_validates() {
        let auth = ClusterAuth::new(Some("hunter2".into()));
        let mut r = req();
        auth.inject(&mut r).unwrap();
        auth.validate(&r).unwrap();
    }

    #[test]
    fn missing_header_rejected_when_enabled() {
        let auth = ClusterAuth::new(Some("hunter2".into()));
        let r = req();
        let err = auth.validate(&r).unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unauthenticated);
        assert!(err.message().contains("missing"));
    }

    #[test]
    fn wrong_token_rejected() {
        let auth_a = ClusterAuth::new(Some("hunter2".into()));
        let auth_b = ClusterAuth::new(Some("password1".into()));
        let mut r = req();
        auth_a.inject(&mut r).unwrap();
        let err = auth_b.validate(&r).unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unauthenticated);
        assert!(err.message().contains("invalid"));
    }

    #[test]
    fn malformed_header_rejected() {
        let auth = ClusterAuth::new(Some("hunter2".into()));
        let mut r = req();
        r.metadata_mut()
            .insert(AUTH_HEADER, "Basic abc".parse().unwrap());
        let err = auth.validate(&r).unwrap_err();
        assert!(err.message().contains("malformed"));
    }

    #[test]
    fn constant_time_eq_equal_lengths() {
        assert!(constant_time_eq(b"abc", b"abc"));
        assert!(!constant_time_eq(b"abc", b"abd"));
        assert!(!constant_time_eq(b"abc", b"ABC"));
        assert!(!constant_time_eq(b"abc", b"abcd"));
        assert!(!constant_time_eq(b"", b"x"));
    }
}
