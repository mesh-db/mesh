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
}
