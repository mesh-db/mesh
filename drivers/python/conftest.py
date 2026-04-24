"""Shared pytest fixtures for the driver-matrix Python suite.

The matrix orchestrator (drivers/run-matrix.sh) launches Mesh in a
subprocess and exports the connection details as env vars before
invoking pytest. We read those here so individual tests stay
configuration-agnostic — they just take a `driver` or `session`
fixture and exercise the wire.
"""

from __future__ import annotations

import os
import ssl
from typing import Iterator

import pytest
from neo4j import Driver, GraphDatabase, Session


def _connection_uri() -> str:
    host = os.environ.get("MESH_BOLT_HOST", "127.0.0.1")
    port = os.environ.get("MESH_BOLT_PORT", "7687")
    tls = os.environ.get("MESH_BOLT_TLS", "off")
    # `bolt+ssc` = self-signed-cert TLS; the Python driver skips chain
    # validation on that scheme, which matches the matrix harness's
    # auto-generated cert.
    scheme = "bolt+ssc" if tls == "on" else "bolt"
    return f"{scheme}://{host}:{port}"


def _auth() -> tuple[str, str] | None:
    if os.environ.get("MESH_BOLT_AUTH", "none") == "basic":
        return ("neo4j", "password")
    return None


@pytest.fixture(scope="session")
def driver() -> Iterator[Driver]:
    """One Driver per test session — the driver pools connections, so
    sharing it across tests is the documented pattern."""
    auth = _auth()
    kwargs = {}
    if auth is not None:
        kwargs["auth"] = auth
    drv = GraphDatabase.driver(_connection_uri(), **kwargs)
    try:
        yield drv
    finally:
        drv.close()


@pytest.fixture
def session(driver: Driver) -> Iterator[Session]:
    """Fresh Session per test. Closing it returns the underlying
    connection to the pool — the driver fixture's close() ultimately
    tears it down."""
    with driver.session() as s:
        yield s
