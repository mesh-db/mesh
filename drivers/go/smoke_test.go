// Smoke tests — proves the handshake + a single RUN/PULL roundtrip
// work via the official neo4j-go-driver/v5. Mirrors Python's
// test_smoke.py and JS's smoke.test.js.

package drivertests

import (
	"context"
	"testing"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

func TestReturnOne(t *testing.T) {
	ctx := context.Background()
	drv, err := neo4j.NewDriverWithContext(connectionURI(), authToken())
	if err != nil {
		t.Fatalf("open driver: %v", err)
	}
	defer drv.Close(ctx)

	session := drv.NewSession(ctx, neo4j.SessionConfig{})
	defer session.Close(ctx)

	result, err := session.Run(ctx, "RETURN 1 AS n", nil)
	if err != nil {
		t.Fatalf("RUN: %v", err)
	}
	record, err := result.Single(ctx)
	if err != nil {
		t.Fatalf("Single: %v", err)
	}
	got, ok := record.Get("n")
	if !ok {
		t.Fatal("field `n` missing from record")
	}
	// Go driver represents Bolt Int as int64 — no wrapper class.
	if got.(int64) != 1 {
		t.Errorf("RETURN 1 got %v, want 1", got)
	}
}

func TestHelloCompletes(t *testing.T) {
	// If the handshake or HELLO phase failed, NewDriverWithContext
	// + VerifyConnectivity returns an error. This test just
	// reasserts that succeeds so a regression bisect starts with a
	// clear top-level indicator.
	ctx := context.Background()
	drv, err := neo4j.NewDriverWithContext(connectionURI(), authToken())
	if err != nil {
		t.Fatalf("open driver: %v", err)
	}
	defer drv.Close(ctx)

	if err := drv.VerifyConnectivity(ctx); err != nil {
		t.Fatalf("VerifyConnectivity: %v", err)
	}
}

func TestRoutingSchemeConnectsAndRuns(t *testing.T) {
	// `neo4j://` triggers the driver's cluster-aware path: it sends
	// ROUTE on first connect, parses the routing table, and opens
	// role-specific connection pools. Validates Mesh's ROUTE
	// response is spec-compliant (3-entry ROUTE/READ/WRITE table).
	ctx := context.Background()
	host := envOr("MESH_BOLT_HOST", "127.0.0.1")
	port := envOr("MESH_BOLT_PORT", "7687")
	tls := envOr("MESH_BOLT_TLS", "off")
	// Go's crypto/tls refuses SNI on IP literals; swap to localhost
	// for the TLS cell (cert's SAN covers both).
	if tls == "on" && host == "127.0.0.1" {
		host = "localhost"
	}
	scheme := "neo4j"
	if tls == "on" {
		scheme = "neo4j+ssc"
	}
	uri := scheme + "://" + host + ":" + port
	drv, err := neo4j.NewDriverWithContext(uri, authToken())
	if err != nil {
		t.Fatalf("open routing driver: %v", err)
	}
	defer drv.Close(ctx)

	session := drv.NewSession(ctx, neo4j.SessionConfig{})
	defer session.Close(ctx)
	result, err := session.Run(ctx, "RETURN 1 AS n", nil)
	if err != nil {
		t.Fatalf("routing RUN: %v", err)
	}
	record, err := result.Single(ctx)
	if err != nil {
		t.Fatalf("routing Single: %v", err)
	}
	got, _ := record.Get("n")
	if got.(int64) != 1 {
		t.Errorf("routing RETURN 1 got %v, want 1", got)
	}
}
