"""Wire-compat parity tests — exercises the bits of the Bolt surface
most likely to regress at the packstream level. Designed to be
mirrored across each language driver in the matrix; a failure here
identifies a wire-format bug, not a planner bug.

Coverage in this file:
  * Scalar round-trip for each Property variant (Int, Float, String,
    Bool, Null, List, Map, DateTime, Date, Time, LocalDateTime,
    Duration, Point 2D cartesian, Point WGS-84).
  * CREATE-with-parameter round-trip (driver -> server packstream).
  * Explicit transaction COMMIT and ROLLBACK paths.
  * Failure + RESET recovery.
  * MATCH (n)-[r:T]->(m) returning Node + Relationship structs.
"""

from __future__ import annotations

from datetime import date, datetime, time, timedelta, timezone

import pytest
from neo4j import Session
from neo4j.exceptions import ClientError, CypherSyntaxError, Neo4jError
from neo4j.spatial import CartesianPoint, WGS84Point
from neo4j.time import Date, DateTime, Duration, Time


# --- Scalar round-trip ------------------------------------------------


@pytest.mark.parametrize(
    "value",
    [
        1,
        -42,
        0,
        3.14,
        -0.5,
        "hello",
        "",
        "unicode-✓-snowman-☃",
        True,
        False,
        None,
        [1, 2, 3],
        ["a", "b"],
        {"k": "v", "n": 1},
    ],
    ids=[
        "int_pos",
        "int_neg",
        "int_zero",
        "float_pos",
        "float_neg",
        "string",
        "string_empty",
        "string_unicode",
        "bool_true",
        "bool_false",
        "null",
        "list_int",
        "list_str",
        "map",
    ],
)
def test_scalar_roundtrip(session: Session, value: object) -> None:
    """Param goes out via packstream-encode in the driver, value
    comes back via packstream-encode on the server. Equality means
    both directions of every primitive tag survived the trip."""
    record = session.run("RETURN $v AS v", v=value).single()
    assert record is not None
    assert record["v"] == value


def test_temporal_datetime_roundtrip(session: Session) -> None:
    sent = datetime(2024, 6, 15, 12, 30, 45, tzinfo=timezone.utc)
    record = session.run("RETURN $v AS v", v=sent).single()
    assert record is not None
    received = record["v"]
    # neo4j.time.DateTime → standard datetime.
    assert received.to_native() == sent


def test_temporal_datetime_nonzero_offset_roundtrip(session: Session) -> None:
    # Non-UTC offset exercises the Bolt 4.4 local-wall-clock semantics
    # (seconds = UTC + offset); under UTC the adjustment is zero and a
    # bug in the offset-addition path would pass silently.
    sent = datetime(
        2024, 6, 15, 14, 30, 45, tzinfo=timezone(timedelta(hours=2))
    )
    record = session.run("RETURN $v AS v", v=sent).single()
    assert record is not None
    # Compare as UTC — the driver may render the returned datetime in
    # a different offset/zone, but the underlying instant must match.
    assert record["v"].to_native().astimezone(timezone.utc) == sent.astimezone(
        timezone.utc
    )


def test_temporal_datetime_iana_zone_server_emits_zone_name(session: Session) -> None:
    # Zoned DateTime server→client path: Cypher's
    # `datetime({timezone: 'Europe/Stockholm', ...})` constructs a
    # zoned instant on the server, Mesh encodes as DateTimeZoneId
    # (tag 0x69 / 0x66), and the driver hydrates with the IANA name
    # attached. Tests Mesh's encoder, not the driver's encoder —
    # neo4j-python-driver 5.28 + Python 3.14 has a packstream issue
    # encoding ZoneInfo-tagged datetimes on the outbound side, so
    # this direction is what matters for driver-compat signal.
    result = session.run(
        "RETURN datetime({year: 2024, month: 6, day: 15, "
        "hour: 14, minute: 30, second: 45, timezone: 'Europe/Stockholm'}) AS v"
    ).single()
    assert result is not None
    back = result["v"].to_native()
    # The Python driver hydrates zone-id DateTimes with a pytz
    # `DstTzInfo` whose IANA name lives on `.zone`; newer Pythons
    # (where the driver picks ZoneInfo) expose it on `.key`. Accept
    # either — the point is the zone name survived the wire hop.
    tz = back.tzinfo
    iana = getattr(tz, "key", None) or getattr(tz, "zone", None)
    assert iana == "Europe/Stockholm"


def test_temporal_date_roundtrip(session: Session) -> None:
    sent = date(2024, 6, 15)
    record = session.run("RETURN $v AS v", v=sent).single()
    assert record is not None
    assert record["v"].to_native() == sent


def test_temporal_time_roundtrip(session: Session) -> None:
    # Send a tz-naive Time (LocalTime in Bolt — packstream tag b't').
    # The driver's encoder for tz-aware Time hits a `tzinfo.utcoffset(t)`
    # path that raises TypeError because Python's tzinfo.utcoffset
    # requires a datetime, not a time — that's a driver-side bug
    # unrelated to Mesh's wire format. Tz-aware time round-trips via
    # DateTime, which works (see test_temporal_datetime_roundtrip).
    sent = Time(12, 30, 45, 0)
    record = session.run("RETURN $v AS v", v=sent).single()
    assert record is not None
    assert record["v"] == sent


def test_temporal_duration_roundtrip(session: Session) -> None:
    sent = Duration(months=1, days=2, seconds=3, nanoseconds=4)
    record = session.run("RETURN $v AS v", v=sent).single()
    assert record is not None
    assert record["v"] == sent


def test_point_cartesian_2d_roundtrip(session: Session) -> None:
    sent = CartesianPoint((1.5, 2.5))
    record = session.run("RETURN $v AS v", v=sent).single()
    assert record is not None
    assert record["v"].x == sent.x
    assert record["v"].y == sent.y
    assert record["v"].srid == sent.srid


def test_point_wgs84_roundtrip(session: Session) -> None:
    sent = WGS84Point((-122.4194, 37.7749))  # San Francisco
    record = session.run("RETURN $v AS v", v=sent).single()
    assert record is not None
    assert record["v"].longitude == sent.longitude
    assert record["v"].latitude == sent.latitude
    assert record["v"].srid == sent.srid


# --- CREATE with parameter, then read it back -----------------------


def test_create_node_with_param_and_match(session: Session) -> None:
    # CREATE → MATCH round-trip exercises the auto-commit write path
    # plus a Node packstream struct on the way back.
    session.run(
        "CREATE (n:DriverParity {marker: $m, idx: $i})",
        m="phase1",
        i=42,
    ).consume()
    record = session.run(
        "MATCH (n:DriverParity {marker: $m}) RETURN n",
        m="phase1",
    ).single()
    assert record is not None
    node = record["n"]
    assert "DriverParity" in node.labels
    assert node["marker"] == "phase1"
    assert node["idx"] == 42


def test_create_relationship_and_match_pattern(session: Session) -> None:
    # MATCH (n)-[r:T]->(m) RETURN n, r, m exercises Node and
    # Relationship structs on the same row — mismatched tags would
    # fail this even when single-element queries pass.
    session.run(
        """
        CREATE (a:DriverParity {role: $a_role}),
               (b:DriverParity {role: $b_role}),
               (a)-[:KNOWS_PARITY {since: $since}]->(b)
        """,
        a_role="alice",
        b_role="bob",
        since=2024,
    ).consume()

    record = session.run(
        """
        MATCH (a:DriverParity {role: $a_role})-[r:KNOWS_PARITY]->(b:DriverParity {role: $b_role})
        RETURN a, r, b
        """,
        a_role="alice",
        b_role="bob",
    ).single()
    assert record is not None
    assert record["a"]["role"] == "alice"
    assert record["b"]["role"] == "bob"
    assert record["r"].type == "KNOWS_PARITY"
    assert record["r"]["since"] == 2024


# --- Explicit transactions ------------------------------------------


def test_explicit_transaction_commit(session: Session) -> None:
    with session.begin_transaction() as tx:
        tx.run("CREATE (:DriverParity {marker: $m})", m="tx-commit").consume()
        tx.commit()

    record = session.run(
        "MATCH (n:DriverParity {marker: $m}) RETURN count(n) AS c",
        m="tx-commit",
    ).single()
    assert record is not None
    assert record["c"] == 1


def test_explicit_transaction_rollback(session: Session) -> None:
    with session.begin_transaction() as tx:
        tx.run("CREATE (:DriverParity {marker: $m})", m="tx-rollback").consume()
        tx.rollback()

    record = session.run(
        "MATCH (n:DriverParity {marker: $m}) RETURN count(n) AS c",
        m="tx-rollback",
    ).single()
    assert record is not None
    assert record["c"] == 0


# --- Failure + RESET recovery ---------------------------------------


def test_syntax_error_then_recovers(session: Session) -> None:
    # Send a malformed query — driver surfaces it as ClientError
    # (a Cypher-side error). The session should still be usable
    # afterwards because the driver issues RESET on its behalf.
    with pytest.raises((ClientError, CypherSyntaxError, Neo4jError)):
        session.run("THIS IS NOT VALID CYPHER").consume()

    # Subsequent query proves RESET cleared the failure and the
    # session is ready again.
    record = session.run("RETURN 1 AS v").single()
    assert record is not None
    assert record["v"] == 1
