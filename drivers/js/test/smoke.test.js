// Smoke test — proves the handshake + a single RUN/PULL roundtrip
// work via the official Neo4j JS driver. Mirrors Python's test_smoke.py.

import { after, before, test } from 'node:test';
import assert from 'node:assert/strict';
import neo4j from 'neo4j-driver';

import { openDriver } from './helpers.js';

let driver;

before(() => {
  driver = openDriver();
});

after(async () => {
  await driver.close();
});

test('RETURN 1 round-trips', async () => {
  const session = driver.session();
  try {
    const result = await session.run('RETURN 1 AS n');
    assert.equal(result.records.length, 1);
    // Integer handling: neo4j-driver wraps ints in a BigInt-ish class
    // by default to preserve 64-bit precision. `toNumber` is the
    // documented way to recover a JS number when the value fits in
    // IEEE 754 double precision.
    assert.equal(result.records[0].get('n').toNumber(), 1);
  } finally {
    await session.close();
  }
});

test('HELLO completed — metadata comes back on consume()', async () => {
  // If the handshake or HELLO phase failed, the session.run above
  // would throw. This test just reasserts a successful consume() to
  // make regression bisects easier to read.
  const session = driver.session();
  try {
    const summary = await session.run('RETURN 1').then((r) => r.summary);
    assert.ok(summary !== null);
  } finally {
    await session.close();
  }
});

test('neo4j:// routing scheme connects and runs', async () => {
  // `neo4j://` triggers the driver's cluster-aware path: it sends
  // ROUTE on first connect, parses the routing table, and opens
  // role-specific pools. Validates Mesh's ROUTE response is
  // spec-compliant — the earlier single-entry empty-address
  // version caused drivers to fail to resolve any endpoint.
  const host = process.env.MESH_BOLT_HOST ?? '127.0.0.1';
  const port = process.env.MESH_BOLT_PORT ?? '7687';
  const tls = process.env.MESH_BOLT_TLS ?? 'off';
  const auth = process.env.MESH_BOLT_AUTH ?? 'none';
  // JS driver refuses TLS SNI on IP literals (same as Go), so swap
  // 127.0.0.1 → localhost for the TLS cell; matrix cert covers both.
  const effectiveHost = tls === 'on' && host === '127.0.0.1' ? 'localhost' : host;
  const scheme = tls === 'on' ? 'neo4j+ssc' : 'neo4j';
  const uri = `${scheme}://${effectiveHost}:${port}`;
  const authToken = auth === 'basic'
    ? neo4j.auth.basic('neo4j', 'password')
    : neo4j.auth.none();
  const routing = neo4j.driver(uri, authToken);
  try {
    const session = routing.session();
    try {
      const result = await session.run('RETURN 1 AS n');
      assert.equal(result.records[0].get('n').toNumber(), 1);
    } finally {
      await session.close();
    }
  } finally {
    await routing.close();
  }
});
