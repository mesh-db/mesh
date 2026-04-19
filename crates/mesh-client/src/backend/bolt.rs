//! Bolt backend built on `mesh-bolt`. Speaks Bolt 5.4 / 5.0 / 4.4
//! (best available), handles the 5.1+ HELLO→LOGON split, and works
//! against any Bolt server — Mesh today, Neo4j once we add TLS.

use anyhow::{anyhow, bail, Context, Result};
use async_trait::async_trait;
use mesh_bolt::{
    perform_client_handshake, read_message, write_message, BoltMessage, BoltValue, BOLT_4_4,
    BOLT_5_0, BOLT_5_4, TAG_NODE, TAG_PATH, TAG_RELATIONSHIP, TAG_UNBOUND_RELATIONSHIP,
};
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::{ClientConfig, DigitallySignedStruct, RootCertStore, SignatureScheme};
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;

use super::{GraphBackend, GraphEdge, GraphNode, QueryResult, Schema, Value};
use crate::config::Profile;

/// A stream we can run the Bolt protocol over. We hold it behind a
/// trait object so plaintext `TcpStream` and `TlsStream<TcpStream>` can
/// live in the same field without pushing a generic through every
/// caller.
pub trait AsyncIo: AsyncRead + AsyncWrite + Send + Unpin {}
impl<T: AsyncRead + AsyncWrite + Send + Unpin + ?Sized> AsyncIo for T {}

pub struct BoltBackend {
    sock: Box<dyn AsyncIo>,
    #[allow(dead_code)]
    version: [u8; 4],
    label: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ConnMode {
    Plain,
    /// TLS with full verification against the system / Mozilla root
    /// bundle.
    Tls,
    /// TLS with certificate verification disabled — for self-signed
    /// dev servers. Do not use against production endpoints.
    TlsInsecure,
}

impl BoltBackend {
    pub async fn connect(profile: &Profile) -> Result<Self> {
        let parsed = parse_bolt_uri(&profile.uri)?;
        let tcp = TcpStream::connect(format!("{}:{}", parsed.host, parsed.port))
            .await
            .with_context(|| format!("tcp connect {}:{}", parsed.host, parsed.port))?;

        let mut sock: Box<dyn AsyncIo> = match parsed.mode {
            ConnMode::Plain => Box::new(tcp),
            ConnMode::Tls => Box::new(tls_connect(tcp, &parsed.host, false).await?),
            ConnMode::TlsInsecure => Box::new(tls_connect(tcp, &parsed.host, true).await?),
        };

        let prefs = [BOLT_5_4, BOLT_5_0, BOLT_4_4, [0; 4]];
        let version = perform_client_handshake(&mut sock, &prefs)
            .await
            .map_err(|e| anyhow!("bolt handshake failed: {e}"))?;

        let auth_in_hello = !is_5_1_plus(version);

        let mut hello_extra: Vec<(String, BoltValue)> = vec![
            ("user_agent".into(), BoltValue::String("mesh-client/0.1".into())),
        ];
        if auth_in_hello {
            push_auth(&mut hello_extra, profile);
        }
        let hello = BoltMessage::Hello {
            extra: BoltValue::Map(hello_extra),
        };
        send_expect_success(&mut sock, &hello, "HELLO").await?;

        if !auth_in_hello {
            let mut auth: Vec<(String, BoltValue)> = Vec::new();
            push_auth(&mut auth, profile);
            let logon = BoltMessage::Logon {
                auth: BoltValue::Map(auth),
            };
            send_expect_success(&mut sock, &logon, "LOGON").await?;
        }

        Ok(Self {
            sock,
            version,
            label: profile.kind.label().to_string(),
        })
    }

    async fn run_pull(&mut self, cypher: &str) -> Result<QueryResult> {
        let run = BoltMessage::Run {
            query: cypher.to_string(),
            params: BoltValue::Map(Vec::new()),
            extra: BoltValue::Map(Vec::new()),
        };
        write_message(&mut self.sock, &run.encode())
            .await
            .context("writing RUN")?;

        let run_reply = read_decoded(&mut self.sock).await?;
        let columns: Vec<String> = match run_reply {
            BoltMessage::Success { metadata } => extract_fields(&metadata),
            BoltMessage::Failure { metadata } => {
                let msg = metadata
                    .get("message")
                    .and_then(BoltValue::as_str)
                    .unwrap_or("unknown")
                    .to_string();
                let _ = self.reset().await;
                bail!("query failed: {msg}");
            }
            other => bail!("unexpected RUN reply: {other:?}"),
        };

        let pull = BoltMessage::Pull {
            extra: BoltValue::map([("n", BoltValue::Int(-1))]),
        };
        write_message(&mut self.sock, &pull.encode())
            .await
            .context("writing PULL")?;

        let mut rows: Vec<Vec<Value>> = Vec::new();
        let mut nodes: Vec<GraphNode> = Vec::new();
        let mut edges: Vec<GraphEdge> = Vec::new();
        let summary;
        loop {
            let msg = read_decoded(&mut self.sock).await?;
            match msg {
                BoltMessage::Record { fields } => {
                    let row: Vec<Value> = fields
                        .iter()
                        .map(|f| from_bolt(f, &mut nodes, &mut edges))
                        .collect();
                    rows.push(row);
                }
                BoltMessage::Success { metadata } => {
                    summary = summarize(&metadata, rows.len());
                    break;
                }
                BoltMessage::Failure { metadata } => {
                    let msg = metadata
                        .get("message")
                        .and_then(BoltValue::as_str)
                        .unwrap_or("unknown")
                        .to_string();
                    let _ = self.reset().await;
                    bail!("pull failed: {msg}");
                }
                other => bail!("unexpected PULL reply: {other:?}"),
            }
        }

        Ok(QueryResult {
            columns,
            rows,
            nodes,
            edges,
            summary: Some(summary),
        })
    }

    async fn reset(&mut self) -> Result<()> {
        write_message(&mut self.sock, &BoltMessage::Reset.encode()).await?;
        // Drain any IGNORED messages until the server acks with SUCCESS.
        loop {
            let msg = read_decoded(&mut self.sock).await?;
            match msg {
                BoltMessage::Success { .. } => break,
                BoltMessage::Ignored => continue,
                other => bail!("unexpected RESET reply: {other:?}"),
            }
        }
        Ok(())
    }
}

#[async_trait]
impl GraphBackend for BoltBackend {
    fn label(&self) -> &str {
        &self.label
    }

    async fn query(&mut self, cypher: &str) -> Result<QueryResult> {
        self.run_pull(cypher).await
    }

    async fn schema(&mut self) -> Result<Schema> {
        let labels = self
            .run_pull("MATCH (n) UNWIND labels(n) AS l RETURN DISTINCT l AS v ORDER BY v")
            .await
            .map(|r| column_strings(&r))
            .unwrap_or_default();
        let rels = self
            .run_pull("MATCH ()-[r]->() RETURN DISTINCT type(r) AS v ORDER BY v")
            .await
            .map(|r| column_strings(&r))
            .unwrap_or_default();
        let keys = self
            .run_pull("MATCH (n) UNWIND keys(n) AS k RETURN DISTINCT k AS v ORDER BY v")
            .await
            .map(|r| column_strings(&r))
            .unwrap_or_default();
        Ok(Schema {
            labels,
            relationship_types: rels,
            property_keys: keys,
        })
    }
}

fn is_5_1_plus(v: [u8; 4]) -> bool {
    // version slot is [0, range, minor, major]
    v[3] > 5 || (v[3] == 5 && v[2] >= 1)
}

fn push_auth(extra: &mut Vec<(String, BoltValue)>, profile: &Profile) {
    let has_creds = profile.username.is_some() || profile.password.is_some();
    if !has_creds {
        extra.push(("scheme".into(), BoltValue::String("none".into())));
        return;
    }
    extra.push(("scheme".into(), BoltValue::String("basic".into())));
    if let Some(u) = &profile.username {
        extra.push(("principal".into(), BoltValue::String(u.clone())));
    }
    if let Some(p) = &profile.password {
        extra.push(("credentials".into(), BoltValue::String(p.clone())));
    }
}

async fn send_expect_success<S>(sock: &mut S, msg: &BoltMessage, label: &str) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    write_message(sock, &msg.encode())
        .await
        .with_context(|| format!("writing {label}"))?;
    let reply = read_decoded(sock).await?;
    match reply {
        BoltMessage::Success { .. } => Ok(()),
        BoltMessage::Failure { metadata } => {
            let err = metadata
                .get("message")
                .and_then(BoltValue::as_str)
                .unwrap_or("unknown");
            bail!("{label} failed: {err}")
        }
        other => bail!("unexpected {label} reply: {other:?}"),
    }
}

async fn read_decoded<S>(sock: &mut S) -> Result<BoltMessage>
where
    S: AsyncRead + Unpin,
{
    let raw = read_message(sock).await.context("reading bolt message")?;
    BoltMessage::decode(&raw).map_err(|e| anyhow!("decoding bolt message: {e}"))
}

struct ParsedUri {
    mode: ConnMode,
    host: String,
    port: u16,
}

/// Parse a Bolt URI. Accepts:
///
/// - `bolt://`, `neo4j://`           → plaintext
/// - `bolt+s://`, `neo4j+s://`       → TLS, verified against webpki roots
/// - `bolt+ssc://`, `neo4j+ssc://`   → TLS, any cert accepted (dev only)
///
/// The `neo4j://` schemes are treated the same as `bolt://` for our
/// purposes — we don't implement server-side routing, so we just
/// connect to whatever host/port is in the URI.
fn parse_bolt_uri(uri: &str) -> Result<ParsedUri> {
    let (mode, rest) = if let Some(r) = uri.strip_prefix("bolt+s://") {
        (ConnMode::Tls, r)
    } else if let Some(r) = uri.strip_prefix("neo4j+s://") {
        (ConnMode::Tls, r)
    } else if let Some(r) = uri.strip_prefix("bolt+ssc://") {
        (ConnMode::TlsInsecure, r)
    } else if let Some(r) = uri.strip_prefix("neo4j+ssc://") {
        (ConnMode::TlsInsecure, r)
    } else if let Some(r) = uri.strip_prefix("bolt://") {
        (ConnMode::Plain, r)
    } else if let Some(r) = uri.strip_prefix("neo4j://") {
        (ConnMode::Plain, r)
    } else {
        (ConnMode::Plain, uri)
    };

    // Strip any trailing `/database` segment before splitting host:port.
    let authority = rest
        .trim_end_matches('/')
        .split('/')
        .next()
        .unwrap_or(rest);
    if authority.is_empty() {
        bail!("empty host in uri {uri}");
    }

    let (host, port) = match authority.rsplit_once(':') {
        Some((h, p)) => (
            h.to_string(),
            p.parse::<u16>()
                .with_context(|| format!("bad port in {uri}"))?,
        ),
        None => (authority.to_string(), 7687),
    };

    Ok(ParsedUri { mode, host, port })
}

async fn tls_connect(
    tcp: TcpStream,
    hostname: &str,
    insecure: bool,
) -> Result<tokio_rustls::client::TlsStream<TcpStream>> {
    let config = if insecure {
        ClientConfig::builder()
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(NoVerifier))
            .with_no_client_auth()
    } else {
        let mut roots = RootCertStore::empty();
        roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
        ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth()
    };

    let connector = TlsConnector::from(Arc::new(config));
    let server_name = ServerName::try_from(hostname.to_string())
        .with_context(|| format!("invalid DNS name {hostname}"))?;
    connector
        .connect(server_name, tcp)
        .await
        .context("TLS handshake")
}

/// Accept any server certificate. Used for `bolt+ssc://` / `neo4j+ssc://`
/// — useful against a self-signed mesh dev server, never against the
/// public internet.
#[derive(Debug)]
struct NoVerifier;

impl ServerCertVerifier for NoVerifier {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> std::result::Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> std::result::Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> std::result::Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        vec![
            SignatureScheme::RSA_PKCS1_SHA256,
            SignatureScheme::RSA_PKCS1_SHA384,
            SignatureScheme::RSA_PKCS1_SHA512,
            SignatureScheme::ECDSA_NISTP256_SHA256,
            SignatureScheme::ECDSA_NISTP384_SHA384,
            SignatureScheme::ECDSA_NISTP521_SHA512,
            SignatureScheme::RSA_PSS_SHA256,
            SignatureScheme::RSA_PSS_SHA384,
            SignatureScheme::RSA_PSS_SHA512,
            SignatureScheme::ED25519,
        ]
    }
}

fn extract_fields(metadata: &BoltValue) -> Vec<String> {
    match metadata.get("fields") {
        Some(BoltValue::List(items)) => items
            .iter()
            .filter_map(|v| match v {
                BoltValue::String(s) => Some(s.clone()),
                _ => None,
            })
            .collect(),
        _ => Vec::new(),
    }
}

fn column_strings(qr: &QueryResult) -> Vec<String> {
    qr.rows
        .iter()
        .filter_map(|row| row.first().map(|v| v.render()))
        .collect()
}

fn summarize(metadata: &BoltValue, row_count: usize) -> String {
    let mut parts = vec![format!("{row_count} row(s)")];
    if let Some(t) = metadata.get("t_last").and_then(BoltValue::as_int) {
        parts.push(format!("t_last={t}ms"));
    }
    if let Some(BoltValue::String(ty)) = metadata.get("type") {
        parts.push(format!("type={ty}"));
    }
    parts.join(" · ")
}

fn from_bolt(
    v: &BoltValue,
    nodes: &mut Vec<GraphNode>,
    edges: &mut Vec<GraphEdge>,
) -> Value {
    match v {
        BoltValue::Null => Value::Null,
        BoltValue::Bool(b) => Value::Bool(*b),
        BoltValue::Int(i) => Value::Int(*i),
        BoltValue::Float(f) => Value::Float(*f),
        BoltValue::String(s) => Value::String(s.clone()),
        BoltValue::Bytes(b) => Value::String(format!("bytes[{}]", b.len())),
        BoltValue::List(items) => Value::List(
            items
                .iter()
                .map(|x| from_bolt(x, nodes, edges))
                .collect(),
        ),
        BoltValue::Map(entries) => {
            let m: BTreeMap<String, Value> = entries
                .iter()
                .map(|(k, v)| (k.clone(), from_bolt(v, nodes, edges)))
                .collect();
            Value::Map(m)
        }
        BoltValue::Struct { tag, fields } => match *tag {
            TAG_NODE => match parse_node(fields) {
                Some(node) => {
                    nodes.push(node.clone());
                    Value::Node(node)
                }
                None => Value::String("<malformed node>".into()),
            },
            TAG_RELATIONSHIP => match parse_rel(fields) {
                Some(edge) => {
                    edges.push(edge.clone());
                    Value::Edge(edge)
                }
                None => Value::String("<malformed relationship>".into()),
            },
            TAG_UNBOUND_RELATIONSHIP => match parse_unbound_rel(fields) {
                Some(edge) => {
                    edges.push(edge.clone());
                    Value::Edge(edge)
                }
                None => Value::String("<malformed unbound relationship>".into()),
            },
            TAG_PATH => parse_path(fields, nodes, edges),
            other => Value::String(format!("<struct 0x{other:02X}>")),
        },
    }
}

fn parse_node(fields: &[BoltValue]) -> Option<GraphNode> {
    // Bolt 4.4: [id:Int, labels:List<String>, properties:Map]
    // Bolt 5.0: [id:Int, labels:List<String>, properties:Map, element_id:String]
    // Prefer element_id when present, since Bolt 5+ id is deprecated.
    let id = element_or_int_id(fields, 0, 3)?;
    let labels = match fields.get(1)? {
        BoltValue::List(items) => items
            .iter()
            .filter_map(|v| match v {
                BoltValue::String(s) => Some(s.clone()),
                _ => None,
            })
            .collect(),
        _ => Vec::new(),
    };
    let properties = properties_map(fields.get(2));
    Some(GraphNode {
        id,
        labels,
        properties,
    })
}

fn parse_rel(fields: &[BoltValue]) -> Option<GraphEdge> {
    // Bolt 4.4: [id, start, end, type, properties]
    // Bolt 5.0: [id, start, end, type, properties, element_id, start_elem_id, end_elem_id]
    let id = element_or_int_id(fields, 0, 5)?;
    let source = element_or_int_id(fields, 1, 6)?;
    let target = element_or_int_id(fields, 2, 7)?;
    let edge_type = match fields.get(3)? {
        BoltValue::String(s) => s.clone(),
        _ => return None,
    };
    let properties = properties_map(fields.get(4));
    Some(GraphEdge {
        id,
        edge_type,
        source,
        target,
        properties,
    })
}

fn parse_unbound_rel(fields: &[BoltValue]) -> Option<GraphEdge> {
    // 4.4: [id, type, properties] · 5.0: [..., element_id]
    let id = element_or_int_id(fields, 0, 3)?;
    let edge_type = match fields.get(1)? {
        BoltValue::String(s) => s.clone(),
        _ => return None,
    };
    let properties = properties_map(fields.get(2));
    Some(GraphEdge {
        id,
        edge_type,
        source: String::new(),
        target: String::new(),
        properties,
    })
}

fn parse_path(
    fields: &[BoltValue],
    nodes: &mut Vec<GraphNode>,
    _edges: &mut Vec<GraphEdge>,
) -> Value {
    // [nodes:List<Node>, rels:List<UnboundRel>, sequence:List<Int>]
    let mut out: Vec<Value> = Vec::new();
    if let Some(BoltValue::List(ns)) = fields.first() {
        for n in ns {
            if let BoltValue::Struct { tag, fields } = n {
                if *tag == TAG_NODE {
                    if let Some(node) = parse_node(fields) {
                        nodes.push(node.clone());
                        out.push(Value::Node(node));
                    }
                }
            }
        }
    }
    Value::Path(out)
}

fn element_or_int_id(fields: &[BoltValue], int_idx: usize, elem_idx: usize) -> Option<String> {
    if let Some(BoltValue::String(s)) = fields.get(elem_idx) {
        return Some(s.clone());
    }
    match fields.get(int_idx)? {
        BoltValue::Int(i) => Some(i.to_string()),
        BoltValue::String(s) => Some(s.clone()),
        _ => None,
    }
}

fn properties_map(v: Option<&BoltValue>) -> BTreeMap<String, Value> {
    match v {
        Some(BoltValue::Map(entries)) => entries
            .iter()
            .map(|(k, v)| {
                // Properties should only hold scalars/lists/maps — we pass
                // through empty graph collectors to avoid polluting the
                // node/edge buffers from nested structs.
                let mut stub_n = Vec::new();
                let mut stub_e = Vec::new();
                (k.clone(), from_bolt(v, &mut stub_n, &mut stub_e))
            })
            .collect(),
        _ => BTreeMap::new(),
    }
}
