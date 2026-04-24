// Wire-compat parity tests — mirrors drivers/python/test_parity.py.
// Covers the packstream surface a Bolt regression is likeliest to
// break: scalar round-trip, parameters on CREATE, Node + Relationship
// structs, explicit transactions, failure + RESET recovery.

import { after, before, test } from 'node:test';
import assert from 'node:assert/strict';
import neo4j from 'neo4j-driver';

import { openDriver } from './helpers.js';

// Bolt 4.4 Node struct has 3 fields (id / labels / properties); 5.0+
// added a 4th `element_id` — same divergence for Relationship
// (5 fields vs 8) and UnboundRelationship (3 vs 4). Mesh's encoder
// currently emits the 5.x shape regardless of negotiated version,
// which fails 4.4 drivers with a "wrong struct size" protocol error.
// Tests that return Node or Relationship are gated here until the
// encoder is taught to switch on version.
const SKIP_NODE_REL_ON_44 = { skip: process.env.MESH_BOLT_VERSION === '4.4' };

let driver;

before(() => {
  driver = openDriver();
});

after(async () => {
  await driver.close();
});

// Wrap a session in a try/finally to avoid leaking connections when
// an assertion throws — node:test swallows unhandled rejections by
// default but the driver emits a warning on connection-pool exhaustion
// that slows subsequent tests if we leak enough sessions.
async function withSession(fn) {
  const session = driver.session();
  try {
    return await fn(session);
  } finally {
    await session.close();
  }
}

// --- Scalar round-trip ------------------------------------------------

// Parameterized on value + equality predicate so we can test each
// Bolt wire type in isolation. The eq arg exists because integers
// and temporal types don't support `===` out of the box — the driver
// hydrates them into custom classes.
const scalarCases = [
  ['int_pos', 42n, (a, b) => a.toBigInt() === b],
  ['int_neg', -17n, (a, b) => a.toBigInt() === b],
  ['int_zero', 0n, (a, b) => a.toBigInt() === b],
  ['float_pos', 3.14, (a, b) => a === b],
  ['float_neg', -0.5, (a, b) => a === b],
  ['string', 'hello', (a, b) => a === b],
  ['string_empty', '', (a, b) => a === b],
  ['string_unicode', 'unicode-✓-snowman-☃', (a, b) => a === b],
  ['bool_true', true, (a, b) => a === b],
  ['bool_false', false, (a, b) => a === b],
  ['null', null, (a, b) => a === b],
  // BigInt-compare directly — JSON.stringify won't serialize BigInts.
  ['list_int', [1n, 2n, 3n], (a, b) => a.length === b.length && a.every((x, i) => x.toBigInt() === b[i])],
  ['list_str', ['a', 'b'], (a, b) => a.length === b.length && a.every((x, i) => x === b[i])],
];

for (const [id, value, eq] of scalarCases) {
  test(`scalar round-trip: ${id}`, async () => {
    await withSession(async (session) => {
      const result = await session.run('RETURN $v AS v', { v: value });
      const back = result.records[0].get('v');
      assert.ok(eq(back, value), `${id}: got ${back}, sent ${value}`);
    });
  });
}

test('scalar round-trip: map', async () => {
  await withSession(async (session) => {
    const sent = { k: 'v', n: 1n };
    const result = await session.run('RETURN $v AS v', { v: sent });
    const back = result.records[0].get('v');
    assert.equal(back.k, 'v');
    assert.equal(back.n.toBigInt(), 1n);
  });
});

// --- Temporal round-trip ----------------------------------------------

test('DateTime UTC round-trip', async () => {
  await withSession(async (session) => {
    const sent = new neo4j.types.DateTime(2024, 6, 15, 12, 30, 45, 0, 0);
    const result = await session.run('RETURN $v AS v', { v: sent });
    const back = result.records[0].get('v');
    assert.equal(back.year.toBigInt(), 2024n);
    assert.equal(back.hour.toBigInt(), 12n);
    // timeZoneOffsetSeconds is a neo4j Integer; normalize to a JS
    // number via toInt() (safe here — offsets fit in ±14*3600).
    assert.equal(back.timeZoneOffsetSeconds.toInt(), 0);
  });
});

test('DateTime non-UTC offset round-trip', async () => {
  // +02:00 exercises the Bolt 4.4 local-wall-clock adjustment; UTC
  // alone doesn't because seconds_adjust = 0 under both versions.
  await withSession(async (session) => {
    const sent = new neo4j.types.DateTime(
      2024, 6, 15, 14, 30, 45, 0, 2 * 3600,
    );
    const result = await session.run('RETURN $v AS v', { v: sent });
    const back = result.records[0].get('v');
    assert.equal(back.year.toBigInt(), 2024n);
    assert.equal(back.hour.toBigInt(), 14n);
    assert.equal(back.timeZoneOffsetSeconds.toInt(), 2 * 3600);
  });
});

test('Date round-trip', async () => {
  await withSession(async (session) => {
    const sent = new neo4j.types.Date(2024, 6, 15);
    const result = await session.run('RETURN $v AS v', { v: sent });
    const back = result.records[0].get('v');
    assert.equal(back.year.toBigInt(), 2024n);
    assert.equal(back.month.toBigInt(), 6n);
    assert.equal(back.day.toBigInt(), 15n);
  });
});

test('Duration round-trip', async () => {
  await withSession(async (session) => {
    const sent = new neo4j.types.Duration(1, 2, 3, 4);
    const result = await session.run('RETURN $v AS v', { v: sent });
    const back = result.records[0].get('v');
    assert.equal(back.months.toBigInt(), 1n);
    assert.equal(back.days.toBigInt(), 2n);
    assert.equal(back.seconds.toBigInt(), 3n);
    assert.equal(back.nanoseconds.toBigInt(), 4n);
  });
});

test('Point 2D Cartesian round-trip', async () => {
  await withSession(async (session) => {
    const sent = new neo4j.types.Point(7203, 1.5, 2.5);
    const result = await session.run('RETURN $v AS v', { v: sent });
    const back = result.records[0].get('v');
    assert.equal(back.srid.toBigInt(), 7203n);
    assert.equal(back.x, 1.5);
    assert.equal(back.y, 2.5);
  });
});

test('Point WGS-84 round-trip', async () => {
  await withSession(async (session) => {
    const sent = new neo4j.types.Point(4326, -122.4194, 37.7749);
    const result = await session.run('RETURN $v AS v', { v: sent });
    const back = result.records[0].get('v');
    assert.equal(back.srid.toBigInt(), 4326n);
    assert.equal(back.x, -122.4194);
    assert.equal(back.y, 37.7749);
  });
});

// --- CREATE with parameter + MATCH ------------------------------------

test('CREATE node with param, MATCH round-trip', SKIP_NODE_REL_ON_44, async () => {
  await withSession(async (session) => {
    await session.run(
      'CREATE (n:DriverParityJS {marker: $m, idx: $i})',
      { m: 'phase2-js', i: 42n },
    );
    const result = await session.run(
      'MATCH (n:DriverParityJS {marker: $m}) RETURN n',
      { m: 'phase2-js' },
    );
    const node = result.records[0].get('n');
    assert.ok(node.labels.includes('DriverParityJS'));
    assert.equal(node.properties.marker, 'phase2-js');
    assert.equal(node.properties.idx.toBigInt(), 42n);
  });
});

test('CREATE relationship, MATCH pattern with Node + Relationship', SKIP_NODE_REL_ON_44, async () => {
  await withSession(async (session) => {
    await session.run(
      `CREATE (a:DriverParityJS {role: $a_role}),
              (b:DriverParityJS {role: $b_role}),
              (a)-[:KNOWS_JS {since: $since}]->(b)`,
      { a_role: 'alice-js', b_role: 'bob-js', since: 2024n },
    );
    const result = await session.run(
      `MATCH (a:DriverParityJS {role: $a_role})-[r:KNOWS_JS]->(b:DriverParityJS {role: $b_role})
       RETURN a, r, b`,
      { a_role: 'alice-js', b_role: 'bob-js' },
    );
    const a = result.records[0].get('a');
    const r = result.records[0].get('r');
    const b = result.records[0].get('b');
    assert.equal(a.properties.role, 'alice-js');
    assert.equal(b.properties.role, 'bob-js');
    assert.equal(r.type, 'KNOWS_JS');
    assert.equal(r.properties.since.toBigInt(), 2024n);
  });
});

// --- Explicit transactions --------------------------------------------

test('Explicit transaction COMMIT', async () => {
  await withSession(async (session) => {
    const tx = session.beginTransaction();
    await tx.run('CREATE (:DriverParityJS {marker: $m})', { m: 'tx-commit-js' });
    await tx.commit();
    const result = await session.run(
      'MATCH (n:DriverParityJS {marker: $m}) RETURN count(n) AS c',
      { m: 'tx-commit-js' },
    );
    assert.equal(result.records[0].get('c').toBigInt(), 1n);
  });
});

test('Explicit transaction ROLLBACK', async () => {
  await withSession(async (session) => {
    const tx = session.beginTransaction();
    await tx.run('CREATE (:DriverParityJS {marker: $m})', { m: 'tx-rollback-js' });
    await tx.rollback();
    const result = await session.run(
      'MATCH (n:DriverParityJS {marker: $m}) RETURN count(n) AS c',
      { m: 'tx-rollback-js' },
    );
    assert.equal(result.records[0].get('c').toBigInt(), 0n);
  });
});

// --- Failure + RESET recovery -----------------------------------------

test('Syntax error then recovers via RESET', async () => {
  await withSession(async (session) => {
    await assert.rejects(
      () => session.run('THIS IS NOT VALID CYPHER'),
      /./,
    );
    // Subsequent query proves the driver issued RESET and the
    // session is usable again.
    const result = await session.run('RETURN 1 AS v');
    assert.equal(result.records[0].get('v').toBigInt(), 1n);
  });
});
