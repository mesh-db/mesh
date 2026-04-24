// Wire-compat parity tests — mirrors drivers/python/test_parity.py
// and drivers/js/test/parity.test.js. Covers the packstream surface
// a Bolt regression is likeliest to break: scalar round-trip, CREATE
// with params, Node + Relationship structs, explicit transactions,
// failure + RESET recovery.

package drivertests

import (
	"context"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/dbtype"
)

// One driver shared across the parity suite — Go's testing package
// doesn't have a session-scoped fixture equivalent, so we lazily
// init via testMain's setup and share through package-private state.
// A leaked connection here just costs one socket; the orchestrator
// tears down the server process between cells regardless.
var testDriver neo4j.DriverWithContext

func TestMain(m *testing.M) {
	ctx := context.Background()
	var err error
	testDriver, err = neo4j.NewDriverWithContext(connectionURI(), authToken())
	if err != nil {
		panic("open driver: " + err.Error())
	}
	defer testDriver.Close(ctx)
	m.Run()
}

func withSession(t *testing.T, fn func(context.Context, neo4j.SessionWithContext)) {
	t.Helper()
	ctx := context.Background()
	session := testDriver.NewSession(ctx, neo4j.SessionConfig{})
	defer session.Close(ctx)
	fn(ctx, session)
}

func runReturn(t *testing.T, ctx context.Context, session neo4j.SessionWithContext, v any) any {
	t.Helper()
	result, err := session.Run(ctx, "RETURN $v AS v", map[string]any{"v": v})
	if err != nil {
		t.Fatalf("RUN RETURN $v: %v", err)
	}
	record, err := result.Single(ctx)
	if err != nil {
		t.Fatalf("Single: %v", err)
	}
	got, ok := record.Get("v")
	if !ok {
		t.Fatal("field `v` missing from record")
	}
	return got
}

// --- Scalar round-trip --------------------------------------------------

func TestScalarRoundTripInt(t *testing.T) {
	for _, v := range []int64{42, -17, 0} {
		v := v
		t.Run("", func(t *testing.T) {
			withSession(t, func(ctx context.Context, s neo4j.SessionWithContext) {
				if got := runReturn(t, ctx, s, v); got.(int64) != v {
					t.Errorf("got %v, want %v", got, v)
				}
			})
		})
	}
}

func TestScalarRoundTripFloat(t *testing.T) {
	for _, v := range []float64{3.14, -0.5, 0.0} {
		v := v
		t.Run("", func(t *testing.T) {
			withSession(t, func(ctx context.Context, s neo4j.SessionWithContext) {
				if got := runReturn(t, ctx, s, v); got.(float64) != v {
					t.Errorf("got %v, want %v", got, v)
				}
			})
		})
	}
}

func TestScalarRoundTripString(t *testing.T) {
	for _, v := range []string{"hello", "", "unicode-✓-snowman-☃"} {
		v := v
		t.Run("", func(t *testing.T) {
			withSession(t, func(ctx context.Context, s neo4j.SessionWithContext) {
				if got := runReturn(t, ctx, s, v); got.(string) != v {
					t.Errorf("got %v, want %v", got, v)
				}
			})
		})
	}
}

func TestScalarRoundTripBool(t *testing.T) {
	for _, v := range []bool{true, false} {
		v := v
		t.Run("", func(t *testing.T) {
			withSession(t, func(ctx context.Context, s neo4j.SessionWithContext) {
				if got := runReturn(t, ctx, s, v); got.(bool) != v {
					t.Errorf("got %v, want %v", got, v)
				}
			})
		})
	}
}

func TestScalarRoundTripNull(t *testing.T) {
	withSession(t, func(ctx context.Context, s neo4j.SessionWithContext) {
		if got := runReturn(t, ctx, s, nil); got != nil {
			t.Errorf("got %v, want nil", got)
		}
	})
}

func TestScalarRoundTripList(t *testing.T) {
	withSession(t, func(ctx context.Context, s neo4j.SessionWithContext) {
		sent := []any{int64(1), int64(2), int64(3)}
		got := runReturn(t, ctx, s, sent).([]any)
		if !reflect.DeepEqual(got, sent) {
			t.Errorf("got %v, want %v", got, sent)
		}
	})
}

func TestScalarRoundTripMap(t *testing.T) {
	withSession(t, func(ctx context.Context, s neo4j.SessionWithContext) {
		sent := map[string]any{"k": "v", "n": int64(1)}
		got := runReturn(t, ctx, s, sent).(map[string]any)
		if !reflect.DeepEqual(got, sent) {
			t.Errorf("got %v, want %v", got, sent)
		}
	})
}

// --- Temporal round-trip -----------------------------------------------

func TestDateTimeRoundTripUTC(t *testing.T) {
	withSession(t, func(ctx context.Context, s neo4j.SessionWithContext) {
		sent := time.Date(2024, 6, 15, 12, 30, 45, 0, time.UTC)
		got := runReturn(t, ctx, s, sent)
		back := got.(time.Time)
		if !back.Equal(sent) {
			t.Errorf("got %v, want %v", back, sent)
		}
	})
}

func TestDateTimeRoundTripNonUTCOffset(t *testing.T) {
	// +02:00 exercises the Bolt 4.4 local-wall-clock adjustment.
	// The Go driver's 4.4 legacy-DateTime hydrator drops the offset
	// and treats tag 0x46 seconds as UTC, which loses the ±offset
	// between the UTC instant in/out — a driver-side bug unrelated
	// to Mesh's wire encoding (verified correct in-tree via
	// tests/value_conv_roundtrip.rs). Skip on 4.4; all other
	// negotiated versions roundtrip correctly.
	if os.Getenv("MESH_BOLT_VERSION") == "4.4" {
		t.Skip("neo4j-go-driver 4.4 DateTime hydrator ignores offset field")
	}
	withSession(t, func(ctx context.Context, s neo4j.SessionWithContext) {
		sent := time.Date(2024, 6, 15, 14, 30, 45, 0, time.FixedZone("UTC+2", 2*3600))
		got := runReturn(t, ctx, s, sent)
		back := got.(time.Time)
		if !back.Equal(sent) {
			t.Errorf("got %v, want %v (UTC: got %v want %v)",
				back, sent, back.UTC(), sent.UTC())
		}
	})
}

func TestPointCartesian2DRoundTrip(t *testing.T) {
	withSession(t, func(ctx context.Context, s neo4j.SessionWithContext) {
		sent := dbtype.Point2D{X: 1.5, Y: 2.5, SpatialRefId: 7203}
		got := runReturn(t, ctx, s, sent)
		back := got.(dbtype.Point2D)
		if back != sent {
			t.Errorf("got %+v, want %+v", back, sent)
		}
	})
}

func TestPointWGS84RoundTrip(t *testing.T) {
	withSession(t, func(ctx context.Context, s neo4j.SessionWithContext) {
		sent := dbtype.Point2D{X: -122.4194, Y: 37.7749, SpatialRefId: 4326}
		got := runReturn(t, ctx, s, sent)
		back := got.(dbtype.Point2D)
		if back != sent {
			t.Errorf("got %+v, want %+v", back, sent)
		}
	})
}

// --- CREATE with parameter + MATCH ------------------------------------

func TestCreateNodeWithParamAndMatch(t *testing.T) {
	withSession(t, func(ctx context.Context, s neo4j.SessionWithContext) {
		_, err := s.Run(ctx,
			"CREATE (n:DriverParityGo {marker: $m, idx: $i})",
			map[string]any{"m": "phase2-go", "i": int64(42)})
		if err != nil {
			t.Fatalf("CREATE: %v", err)
		}
		result, err := s.Run(ctx,
			"MATCH (n:DriverParityGo {marker: $m}) RETURN n",
			map[string]any{"m": "phase2-go"})
		if err != nil {
			t.Fatalf("MATCH: %v", err)
		}
		record, err := result.Single(ctx)
		if err != nil {
			t.Fatalf("Single: %v", err)
		}
		nv, _ := record.Get("n")
		n := nv.(dbtype.Node)
		if !containsString(n.Labels, "DriverParityGo") {
			t.Errorf("labels %v missing DriverParityGo", n.Labels)
		}
		if n.Props["marker"] != "phase2-go" {
			t.Errorf("marker: got %v, want phase2-go", n.Props["marker"])
		}
		if n.Props["idx"].(int64) != 42 {
			t.Errorf("idx: got %v, want 42", n.Props["idx"])
		}
	})
}

func TestCreateRelationshipAndMatchPattern(t *testing.T) {
	withSession(t, func(ctx context.Context, s neo4j.SessionWithContext) {
		_, err := s.Run(ctx, `
			CREATE (a:DriverParityGo {role: $ar}),
			       (b:DriverParityGo {role: $br}),
			       (a)-[:KNOWS_GO {since: $s}]->(b)`,
			map[string]any{"ar": "alice-go", "br": "bob-go", "s": int64(2024)})
		if err != nil {
			t.Fatalf("CREATE: %v", err)
		}
		result, err := s.Run(ctx, `
			MATCH (a:DriverParityGo {role: $ar})-[r:KNOWS_GO]->(b:DriverParityGo {role: $br})
			RETURN a, r, b`,
			map[string]any{"ar": "alice-go", "br": "bob-go"})
		if err != nil {
			t.Fatalf("MATCH: %v", err)
		}
		record, err := result.Single(ctx)
		if err != nil {
			t.Fatalf("Single: %v", err)
		}
		// Positional access works now — Mesh preserves RETURN
		// declaration order on the wire. Values[0..2] match the
		// RETURN a, r, b order regardless of the alphabetical
		// sort that used to happen on the server side.
		a := record.Values[0].(dbtype.Node)
		r := record.Values[1].(dbtype.Relationship)
		b := record.Values[2].(dbtype.Node)
		if a.Props["role"] != "alice-go" {
			t.Errorf("a.role: got %v, want alice-go", a.Props["role"])
		}
		if b.Props["role"] != "bob-go" {
			t.Errorf("b.role: got %v, want bob-go", b.Props["role"])
		}
		if r.Type != "KNOWS_GO" {
			t.Errorf("r.type: got %v, want KNOWS_GO", r.Type)
		}
		if r.Props["since"].(int64) != 2024 {
			t.Errorf("r.since: got %v, want 2024", r.Props["since"])
		}
	})
}

// --- Explicit transactions -------------------------------------------

func TestExplicitTransactionCommit(t *testing.T) {
	withSession(t, func(ctx context.Context, s neo4j.SessionWithContext) {
		tx, err := s.BeginTransaction(ctx)
		if err != nil {
			t.Fatalf("BEGIN: %v", err)
		}
		if _, err := tx.Run(ctx,
			"CREATE (:DriverParityGo {marker: $m})",
			map[string]any{"m": "tx-commit-go"}); err != nil {
			t.Fatalf("tx.Run: %v", err)
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatalf("COMMIT: %v", err)
		}
		result, err := s.Run(ctx,
			"MATCH (n:DriverParityGo {marker: $m}) RETURN count(n) AS c",
			map[string]any{"m": "tx-commit-go"})
		if err != nil {
			t.Fatalf("count MATCH: %v", err)
		}
		rec, _ := result.Single(ctx)
		if rec.Values[0].(int64) != 1 {
			t.Errorf("count: got %v, want 1", rec.Values[0])
		}
	})
}

func TestExplicitTransactionRollback(t *testing.T) {
	withSession(t, func(ctx context.Context, s neo4j.SessionWithContext) {
		tx, err := s.BeginTransaction(ctx)
		if err != nil {
			t.Fatalf("BEGIN: %v", err)
		}
		if _, err := tx.Run(ctx,
			"CREATE (:DriverParityGo {marker: $m})",
			map[string]any{"m": "tx-rollback-go"}); err != nil {
			t.Fatalf("tx.Run: %v", err)
		}
		if err := tx.Rollback(ctx); err != nil {
			t.Fatalf("ROLLBACK: %v", err)
		}
		result, err := s.Run(ctx,
			"MATCH (n:DriverParityGo {marker: $m}) RETURN count(n) AS c",
			map[string]any{"m": "tx-rollback-go"})
		if err != nil {
			t.Fatalf("count MATCH: %v", err)
		}
		rec, _ := result.Single(ctx)
		if rec.Values[0].(int64) != 0 {
			t.Errorf("count: got %v, want 0", rec.Values[0])
		}
	})
}

// --- Failure + RESET recovery --------------------------------------

func TestSyntaxErrorThenRecovers(t *testing.T) {
	withSession(t, func(ctx context.Context, s neo4j.SessionWithContext) {
		result, err := s.Run(ctx, "THIS IS NOT VALID CYPHER", nil)
		// The driver may surface the error from Run or from
		// consuming the result — either is a legit FAILURE path.
		if err == nil {
			if _, err = result.Consume(ctx); err == nil {
				t.Fatal("expected error on invalid Cypher, got none")
			}
		}
		// Subsequent query proves the driver issued RESET and
		// the session is usable again.
		result, err = s.Run(ctx, "RETURN 1 AS v", nil)
		if err != nil {
			t.Fatalf("recovery RUN: %v", err)
		}
		rec, _ := result.Single(ctx)
		if rec.Values[0].(int64) != 1 {
			t.Errorf("recovery RETURN 1: got %v, want 1", rec.Values[0])
		}
	})
}

func containsString(haystack []string, needle string) bool {
	for _, h := range haystack {
		if h == needle {
			return true
		}
	}
	return false
}
