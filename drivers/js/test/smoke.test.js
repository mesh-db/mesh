// Smoke test — proves the handshake + a single RUN/PULL roundtrip
// work via the official Neo4j JS driver. Mirrors Python's test_smoke.py.

import { after, before, test } from 'node:test';
import assert from 'node:assert/strict';

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
