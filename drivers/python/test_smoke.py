"""Smoke test — proves the handshake + a single RUN/PULL roundtrip
work via the official Python driver. If this fails the whole matrix
cell is broken; everything in test_parity.py builds on this passing.
"""

from neo4j import Session


def test_return_one(session: Session) -> None:
    record = session.run("RETURN 1 AS n").single()
    assert record is not None
    assert record["n"] == 1


def test_handshake_picks_a_supported_version(session: Session) -> None:
    # The driver records the negotiated Bolt protocol version on
    # the underlying connection. We just want to confirm we got
    # *some* version, which means the handshake completed and the
    # server's advertised list had at least one overlap with the
    # driver's preferences.
    info = session.run("RETURN 1").consume()
    # The server-side metadata isn't surfaced by the driver in a
    # version-portable way, so we just check we got a result
    # summary back at all — which means HELLO succeeded.
    assert info is not None
