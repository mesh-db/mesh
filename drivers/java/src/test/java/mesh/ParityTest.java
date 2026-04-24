// Wire-compat parity tests — mirrors drivers/{python,js,go}. Covers
// the packstream surface a Bolt regression is likeliest to break:
// scalar round-trip, CREATE with params, Node + Relationship structs,
// explicit transactions, failure + RESET recovery.

package mesh;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.Value;
import org.neo4j.driver.exceptions.Neo4jException;
import org.neo4j.driver.types.Node;
import org.neo4j.driver.types.Point;
import org.neo4j.driver.types.Relationship;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ParityTest {
    private Driver driver;

    @BeforeAll
    void openDriver() {
        driver = Connection.openDriver();
    }

    @AfterAll
    void closeDriver() {
        driver.close();
    }

    private Value runReturn(Object value) {
        // Can't use Map.of here — it rejects null values. singletonMap
        // accepts them (null means Cypher `null` for the param).
        try (Session session = driver.session()) {
            Result result = session.run(
                "RETURN $v AS v", Collections.singletonMap("v", value));
            return result.single().get("v");
        }
    }

    // --- Scalar round-trip -------------------------------------------

    @Test
    void scalarRoundTripInt() {
        for (long v : new long[] {42, -17, 0}) {
            assertEquals(v, runReturn(v).asLong(), "int " + v);
        }
    }

    @Test
    void scalarRoundTripFloat() {
        for (double v : new double[] {3.14, -0.5, 0.0}) {
            assertEquals(v, runReturn(v).asDouble(), "float " + v);
        }
    }

    @Test
    void scalarRoundTripString() {
        for (String v : new String[] {"hello", "", "unicode-✓-snowman-☃"}) {
            assertEquals(v, runReturn(v).asString(), "string");
        }
    }

    @Test
    void scalarRoundTripBool() {
        assertEquals(true, runReturn(true).asBoolean());
        assertEquals(false, runReturn(false).asBoolean());
    }

    @Test
    void scalarRoundTripNull() {
        assertTrue(runReturn(null).isNull());
    }

    @Test
    void scalarRoundTripList() {
        List<Long> sent = List.of(1L, 2L, 3L);
        List<Object> got = runReturn(sent).asList();
        assertEquals(sent, got);
    }

    @Test
    void scalarRoundTripMap() {
        Map<String, Object> sent = Map.of("k", "v", "n", 1L);
        Map<String, Object> got = runReturn(sent).asMap();
        assertEquals(sent, got);
    }

    // --- Temporal round-trip -----------------------------------------

    @Test
    void dateTimeRoundTripUTC() {
        OffsetDateTime sent = OffsetDateTime.of(
            2024, 6, 15, 12, 30, 45, 0, ZoneOffset.UTC);
        OffsetDateTime back = runReturn(sent).asOffsetDateTime();
        assertEquals(sent.toInstant(), back.toInstant());
    }

    @Test
    void dateTimeRoundTripNonUTCOffset() {
        // +02:00 exercises the Bolt 4.4 local-wall-clock adjustment.
        // The Java driver, like Go's, gets tag 0x46 wrong — hydrates
        // seconds as UTC and drops the offset. Mesh's encoder is
        // verified correct in-tree (value_conv_roundtrip); skip on
        // 4.4 and re-run on 5.0+ where both sides agree.
        assumeFalse("4.4".equals(System.getenv("MESH_BOLT_VERSION")),
            "neo4j-java-driver 4.4 DateTime hydrator drops offset field");
        OffsetDateTime sent = OffsetDateTime.of(
            2024, 6, 15, 14, 30, 45, 0, ZoneOffset.ofHours(2));
        OffsetDateTime back = runReturn(sent).asOffsetDateTime();
        assertEquals(sent.toInstant(), back.toInstant());
    }

    @Test
    void pointCartesian2DRoundTrip() {
        Point sent = org.neo4j.driver.Values.point(7203, 1.5, 2.5).asPoint();
        Point back = runReturn(sent).asPoint();
        assertEquals(sent.srid(), back.srid());
        assertEquals(sent.x(), back.x());
        assertEquals(sent.y(), back.y());
    }

    @Test
    void pointWGS84RoundTrip() {
        Point sent = org.neo4j.driver.Values.point(4326, -122.4194, 37.7749).asPoint();
        Point back = runReturn(sent).asPoint();
        assertEquals(sent.srid(), back.srid());
        assertEquals(sent.x(), back.x());
        assertEquals(sent.y(), back.y());
    }

    // --- CREATE with parameter + MATCH ------------------------------

    @Test
    void createNodeWithParamAndMatch() {
        try (Session session = driver.session()) {
            session.run(
                "CREATE (n:DriverParityJava {marker: $m, idx: $i})",
                Map.of("m", "phase2-java", "i", 42L)).consume();
            Record record = session.run(
                "MATCH (n:DriverParityJava {marker: $m}) RETURN n",
                Map.of("m", "phase2-java")).single();
            Node n = record.get("n").asNode();
            assertTrue(n.hasLabel("DriverParityJava"));
            assertEquals("phase2-java", n.get("marker").asString());
            assertEquals(42L, n.get("idx").asLong());
        }
    }

    @Test
    void createRelationshipAndMatchPattern() {
        try (Session session = driver.session()) {
            session.run(
                "CREATE (a:DriverParityJava {role: $ar}),"
                + " (b:DriverParityJava {role: $br}),"
                + " (a)-[:KNOWS_JAVA {since: $s}]->(b)",
                Map.of("ar", "alice-java", "br", "bob-java", "s", 2024L)).consume();
            Record record = session.run(
                "MATCH (a:DriverParityJava {role: $ar})-[r:KNOWS_JAVA]->(b:DriverParityJava {role: $br}) "
                + "RETURN a, r, b",
                Map.of("ar", "alice-java", "br", "bob-java")).single();
            Node a = record.get("a").asNode();
            Relationship r = record.get("r").asRelationship();
            Node b = record.get("b").asNode();
            assertEquals("alice-java", a.get("role").asString());
            assertEquals("bob-java", b.get("role").asString());
            assertEquals("KNOWS_JAVA", r.type());
            assertEquals(2024L, r.get("since").asLong());
        }
    }

    // --- Explicit transactions --------------------------------------

    @Test
    void explicitTransactionCommit() {
        try (Session session = driver.session()) {
            try (Transaction tx = session.beginTransaction()) {
                tx.run("CREATE (:DriverParityJava {marker: $m})",
                    Map.of("m", "tx-commit-java")).consume();
                tx.commit();
            }
            long count = session.run(
                "MATCH (n:DriverParityJava {marker: $m}) RETURN count(n) AS c",
                Map.of("m", "tx-commit-java")).single().get("c").asLong();
            assertEquals(1L, count);
        }
    }

    @Test
    void explicitTransactionRollback() {
        try (Session session = driver.session()) {
            try (Transaction tx = session.beginTransaction()) {
                tx.run("CREATE (:DriverParityJava {marker: $m})",
                    Map.of("m", "tx-rollback-java")).consume();
                tx.rollback();
            }
            long count = session.run(
                "MATCH (n:DriverParityJava {marker: $m}) RETURN count(n) AS c",
                Map.of("m", "tx-rollback-java")).single().get("c").asLong();
            assertEquals(0L, count);
        }
    }

    // --- Failure + RESET recovery -----------------------------------

    @Test
    void syntaxErrorThenRecovers() {
        try (Session session = driver.session()) {
            assertThrows(Neo4jException.class,
                () -> session.run("THIS IS NOT VALID CYPHER").consume());
            // Subsequent query proves the driver issued RESET and
            // the session is usable again.
            long v = session.run("RETURN 1 AS v").single().get("v").asLong();
            assertEquals(1L, v);
        }
    }
}
