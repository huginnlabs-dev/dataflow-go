package dataflow

import (
	"runtime/debug"
	"testing"
)

func TestBuildManifest(t *testing.T) {
	bi := &debug.BuildInfo{
		GoVersion: "go1.27.1",
		Deps: []*debug.Module{
			{Path: "github.com/jackc/pgx/v5", Version: "v5.7.6"},
			{Path: "github.com/gin-gonic/gin", Version: "v1.11.0"},
			{Path: "github.com/labstack/echo/v4", Version: "v4.13.0"},
		},
	}
	m := buildManifest(bi, "payments")
	if m.ServiceName != "payments" {
		t.Fatalf("service = %q", m.ServiceName)
	}
	if m.Language != "go" || m.SDKVersion != SDKVersion {
		t.Fatalf("lang/sdk = %q/%q", m.Language, m.SDKVersion)
	}
	// First framework match wins (gin precedes echo in the known list).
	if m.Framework != "gin" {
		t.Fatalf("framework = %q, want gin", m.Framework)
	}
	if len(m.Dependencies) != 3 {
		t.Fatalf("deps = %d, want 3", len(m.Dependencies))
	}
	if m.Dependencies[0].Name != "github.com/jackc/pgx/v5" || m.Dependencies[0].Version != "v5.7.6" {
		t.Fatalf("dep[0] = %+v", m.Dependencies[0])
	}
}

func TestBuildManifestFallbackFramework(t *testing.T) {
	bi := &debug.BuildInfo{Deps: []*debug.Module{{Path: "golang.org/x/net", Version: "v0.57.0"}}}
	if m := buildManifest(bi, "svc"); m.Framework != "net/http" {
		t.Fatalf("framework = %q, want net/http", m.Framework)
	}
}

func TestHTTPBaseURL(t *testing.T) {
	t.Setenv("DATAFLOW_HTTP_URL", "http://api:8080/")
	if got := httpBaseURL("api:9090", false); got != "http://api:8080" {
		t.Fatalf("override = %q", got)
	}
	t.Setenv("DATAFLOW_HTTP_URL", "")
	if got := httpBaseURL("https://ingest.example.com", false); got != "https://ingest.example.com" {
		t.Fatalf("url-form = %q", got)
	}
	if got := httpBaseURL("api.example.com:443", true); got != "https://api.example.com:443" {
		t.Fatalf("tls host:port = %q", got)
	}
	// Bare gRPC endpoint without an HTTP override: nothing to report to.
	if got := httpBaseURL("api:9090", false); got != "" {
		t.Fatalf("bare endpoint = %q, want empty", got)
	}
}
