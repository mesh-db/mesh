// Smoke tests — proves the handshake + a single RUN/PULL roundtrip
// work via the official neo4j-java-driver. Mirrors Python's
// test_smoke.py, JS's smoke.test.js, and Go's smoke_test.go.

package mesh;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class SmokeTest {
    private Driver driver;

    @BeforeAll
    void openDriver() {
        driver = Connection.openDriver();
    }

    @AfterAll
    void closeDriver() {
        driver.close();
    }

    @Test
    void returnOne() {
        try (Session session = driver.session()) {
            Result result = session.run("RETURN 1 AS n");
            assertEquals(1L, result.single().get("n").asLong());
        }
    }

    @Test
    void helloCompletes() {
        // If the handshake or HELLO phase failed, the session/run
        // would throw. This test just asserts we get a summary
        // back — makes a regression bisect's top-level failure
        // point explicit.
        try (Session session = driver.session()) {
            Result result = session.run("RETURN 1");
            assertNotNull(result.consume());
        }
    }

    @Test
    void routingSchemeConnectsAndRuns() {
        // `neo4j://` triggers the driver's cluster-aware path: it
        // sends ROUTE on first connect, parses the routing table,
        // and opens role-specific connection pools. Validates
        // Mesh's ROUTE response is spec-compliant.
        String host = System.getenv().getOrDefault("MESH_BOLT_HOST", "127.0.0.1");
        String port = System.getenv().getOrDefault("MESH_BOLT_PORT", "7687");
        String tls = System.getenv().getOrDefault("MESH_BOLT_TLS", "off");
        String auth = System.getenv().getOrDefault("MESH_BOLT_AUTH", "none");
        // JSSE refuses SNI on IP literals — swap for the TLS cell.
        if ("on".equals(tls) && "127.0.0.1".equals(host)) {
            host = "localhost";
        }
        String scheme = "on".equals(tls) ? "neo4j+ssc" : "neo4j";
        String uri = scheme + "://" + host + ":" + port;
        org.neo4j.driver.AuthToken token = "basic".equals(auth)
            ? org.neo4j.driver.AuthTokens.basic("neo4j", "password")
            : org.neo4j.driver.AuthTokens.none();
        try (org.neo4j.driver.Driver routing =
                 org.neo4j.driver.GraphDatabase.driver(uri, token);
             Session session = routing.session()) {
            long n = session.run("RETURN 1 AS n").single().get("n").asLong();
            assertEquals(1L, n);
        }
    }
}
