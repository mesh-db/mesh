// Shared connection helpers — mirrors drivers/python/conftest.py and
// drivers/js/test/helpers.js. The matrix orchestrator launches Mesh
// in a subprocess and exports connection details as env vars; tests
// pull them from here so a new axis doesn't need a per-test change.

package drivertests

import (
	"os"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

func connectionURI() string {
	host := envOr("MESH_BOLT_HOST", "127.0.0.1")
	port := envOr("MESH_BOLT_PORT", "7687")
	tls := envOr("MESH_BOLT_TLS", "off")

	// `bolt+ssc://` = self-signed-cert TLS: connect over TLS but
	// skip chain validation. Go's TLS layer (like JS) refuses SNI
	// on IP literals, so swap to `localhost` for the TLS cell —
	// the matrix cert's SAN covers both.
	if tls == "on" && host == "127.0.0.1" {
		host = "localhost"
	}

	scheme := "bolt"
	if tls == "on" {
		scheme = "bolt+ssc"
	}
	return scheme + "://" + host + ":" + port
}

func authToken() neo4j.AuthToken {
	if os.Getenv("MESH_BOLT_AUTH") == "basic" {
		return neo4j.BasicAuth("neo4j", "password", "")
	}
	return neo4j.NoAuth()
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
