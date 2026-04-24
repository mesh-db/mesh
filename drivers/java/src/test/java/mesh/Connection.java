// Shared connection helpers — mirrors drivers/python/conftest.py,
// drivers/js/test/helpers.js, and drivers/go/helpers_test.go. The
// matrix orchestrator launches Mesh in a subprocess and exports
// connection details as env vars; tests pull them from here so a
// new axis doesn't need a per-test change.

package mesh;

import org.neo4j.driver.AuthToken;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;

final class Connection {
    private Connection() {}

    static String uri() {
        String host = envOr("MESH_BOLT_HOST", "127.0.0.1");
        String port = envOr("MESH_BOLT_PORT", "7687");
        String tls = envOr("MESH_BOLT_TLS", "off");

        // JSSE (Java's TLS stack) is strict about SNI on IP literals,
        // same as Go and the Node tls layer. The matrix cert's SAN
        // covers `localhost` + `127.0.0.1`, so swap to `localhost` for
        // the TLS cell.
        if ("on".equals(tls) && "127.0.0.1".equals(host)) {
            host = "localhost";
        }

        String scheme = "on".equals(tls) ? "bolt+ssc" : "bolt";
        return scheme + "://" + host + ":" + port;
    }

    static AuthToken authToken() {
        if ("basic".equals(envOr("MESH_BOLT_AUTH", "none"))) {
            return AuthTokens.basic("neo4j", "password");
        }
        return AuthTokens.none();
    }

    static Driver openDriver() {
        return GraphDatabase.driver(uri(), authToken(), Config.defaultConfig());
    }

    private static String envOr(String key, String fallback) {
        String v = System.getenv(key);
        return (v == null || v.isEmpty()) ? fallback : v;
    }
}
