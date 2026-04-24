"""Smoke test — proves the handshake + a single RUN/PULL roundtrip
work via the official Python driver. If this fails the whole matrix
cell is broken; everything in test_parity.py builds on this passing.
"""

import os

import pytest
from neo4j import GraphDatabase, Session


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


def test_routing_scheme_connects_and_runs() -> None:
    # `neo4j://` (as opposed to `bolt://`) triggers the driver's
    # cluster-aware path: it sends ROUTE on first connect, parses
    # the routing table, opens separate connection pools per role
    # (READ / WRITE / ROUTE), and dispatches queries accordingly.
    # Validates Mesh's ROUTE response is spec-compliant — the
    # earlier single-address-list ROUTE-only entry caused drivers
    # to fail to resolve any endpoint at all.
    #
    # Separate driver here (the shared `driver` fixture uses
    # `bolt://` which bypasses ROUTE entirely).
    host = os.environ.get("MESH_BOLT_HOST", "127.0.0.1")
    port = os.environ.get("MESH_BOLT_PORT", "7687")
    tls = os.environ.get("MESH_BOLT_TLS", "off")
    auth = os.environ.get("MESH_BOLT_AUTH", "none")
    scheme = "neo4j+ssc" if tls == "on" else "neo4j"
    uri = f"{scheme}://{host}:{port}"
    kwargs = {}
    if auth == "basic":
        kwargs["auth"] = ("neo4j", "password")
    drv = GraphDatabase.driver(uri, **kwargs)
    try:
        with drv.session() as s:
            record = s.run("RETURN 1 AS n").single()
            assert record is not None
            assert record["n"] == 1
    finally:
        drv.close()
