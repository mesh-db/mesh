// Shared connection helpers — mirrors drivers/python/conftest.py. The
// matrix orchestrator launches Mesh in a subprocess and exports
// connection details as env vars; tests pull them from here so a new
// axis doesn't need a per-test code change.

import neo4j from 'neo4j-driver';

let host = process.env.MESH_BOLT_HOST ?? '127.0.0.1';
const port = process.env.MESH_BOLT_PORT ?? '7687';
const tls = process.env.MESH_BOLT_TLS ?? 'off';
const auth = process.env.MESH_BOLT_AUTH ?? 'none';

// JS driver refuses to set the TLS ServerName (SNI) to an IP address
// — Python is lenient here, JS (via Node.js tls) is strict. The
// matrix cert's SAN covers `DNS:localhost` + `IP:127.0.0.1`, so
// swapping to `localhost` for the TLS cell keeps verification happy
// without reissuing certs.
if (tls === 'on' && host === '127.0.0.1') host = 'localhost';

// `bolt+ssc://` = self-signed-cert TLS: connect over TLS but skip
// chain validation. Matches the Python driver's equivalent scheme
// and the matrix harness's auto-generated cert.
const scheme = tls === 'on' ? 'bolt+ssc' : 'bolt';
export const uri = `${scheme}://${host}:${port}`;

export const authToken = auth === 'basic'
  ? neo4j.auth.basic('neo4j', 'password')
  : neo4j.auth.none();

// One driver per test file — cheaper than per-test since the driver
// pools connections. Tests await `driver.close()` in a cleanup hook.
export function openDriver() {
  return neo4j.driver(uri, authToken);
}
