# Driver matrix tests

End-to-end tests that drive Mesh's Bolt listener with the official
Neo4j drivers. Catches wire-level regressions that the in-tree Bolt
tests (which use Mesh's own framing crate) can't see — packstream tag
drift, struct-shape changes, version-conditional encoding bugs,
authentication / TLS plumbing.

## Layout

```
drivers/
  common/                Server TOML configs + cert generator.
                         server.toml         — default (auth=off, tls=off, all bolt versions)
                         server-auth.toml    — basic auth (neo4j/password)
                         server-tls.toml     — self-signed TLS
                         server-bolt44.toml  — clamped to Bolt 4.4
                         server-bolt50.toml  — clamped to Bolt 5.0
                         server-bolt54.toml  — clamped to Bolt 5.4
                         gen-cert.sh         — writes cert.pem / key.pem
  python/                Phase 1: official neo4j-python-driver tests.
  run-matrix.sh          Orchestrator. One invocation = one matrix cell.
```

## Run a single cell

```sh
cargo build --release -p meshdb-server
PATH="$(pwd)/drivers/python/.venv/bin:$PATH" \
  drivers/run-matrix.sh --lang=py --bolt=5.4
```

See `drivers/python/README.md` for the per-language venv bootstrap.

## Phase status

- **Phase 1 (current)**: Python driver + smoke + parity tests +
  baseline CI cell. Lands the harness, the configurable Bolt
  version axis (`bolt_advertised_versions`), and the Neo4j-prefixed
  server agent string needed to connect with official drivers at all.
- **Phase 2**: JS, Java, Go drivers.
- **Phase 3**: PR matrix (per-driver × auth × TLS) + nightly full-axis.
- **Phase 4**: Routing mode + ROUTE message compat.

## Known gaps

- **RETURN field order is alphabetical, not declaration-order** —
  `meshdb_server::value_conv::field_names_from_rows` collects
  column names into a `BTreeSet`, so `RETURN a, r, b` comes back
  on the wire as `[a, b, r]`. Drivers that access by name (Python
  / JS) are unaffected; drivers that access positionally (Go
  `record.Values[i]`) see columns in the wrong slot. Neo4j's spec
  preserves RETURN declaration order. Fix: have the executor's
  Project operator emit a field-name list in declaration order,
  thread it through to `fields_success` instead of deriving from
  BTreeSet-sorted row keys.
- **Zoned DateTime with `tz_name`** — `Property::DateTime` carries an
  optional IANA region name (`Europe/Stockholm`, etc.) but the
  encoder currently ignores it and emits offset-only DateTime
  (`0x49` / `0x46`) rather than DateTimeZoneId (`0x69` / `0x66`).
  Offset-aware tz-aware values round-trip correctly; zone names are
  silently dropped. Fix: teach `property_to_bolt`'s DateTime arm to
  switch on `tz_name.is_some()`.
