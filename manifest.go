package dataflow

// Service manifest: one best-effort HTTP POST at startup describing this
// service (framework, runtime, dependency inventory from the build). The
// server turns it into the project's service catalog. Failures are silent
// — tracing never depends on the manifest reaching the server.

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"runtime"
	"runtime/debug"
	"strings"
	"time"
)

// knownFrameworks maps well-known module paths to a display name; the
// first match wins. Everything else reports as net/http (the SDK middleware
// requires it anyway).
var knownFrameworks = []struct{ module, name string }{
	{"github.com/gin-gonic/gin", "gin"},
	{"github.com/labstack/echo", "echo"},
	{"github.com/go-chi/chi", "chi"},
	{"github.com/gofiber/fiber", "fiber"},
	{"github.com/gorilla/mux", "gorilla/mux"},
	{"github.com/valyala/fasthttp", "fasthttp"},
}

// maxManifestDeps caps the reported dependency list; the header-free REST
// body has no hard limit, but real services rarely exceed this.
const maxManifestDeps = 500

// Manifest is the startup profile reported to the server.
type Manifest struct {
	ServiceName    string        `json:"service_name"`
	Language       string        `json:"language"`
	SDKVersion     string        `json:"sdk_version"`
	RuntimeVersion string        `json:"runtime_version"`
	Framework      string        `json:"framework"`
	OSArch         string        `json:"os_arch"`
	AppVersion     string        `json:"app_version"`
	Dependencies   []manifestDep `json:"dependencies"`
}

type manifestDep struct {
	Name    string `json:"name"`
	Version string `json:"version"`
}

// buildManifest derives the manifest from the process build info.
func buildManifest(bi *debug.BuildInfo, serviceName string) *Manifest {
	deps := make([]manifestDep, 0, len(bi.Deps))
	framework := ""
	for _, d := range bi.Deps {
		if d == nil || len(deps) >= maxManifestDeps {
			continue
		}
		deps = append(deps, manifestDep{Name: d.Path, Version: d.Version})
		if framework == "" {
			for _, f := range knownFrameworks {
				if d.Path == f.module || strings.HasPrefix(d.Path, f.module+"/") {
					framework = f.name
					break
				}
			}
		}
	}
	if framework == "" {
		framework = "net/http"
	}
	return &Manifest{
		ServiceName:    serviceName,
		Language:       "go",
		SDKVersion:     SDKVersion,
		RuntimeVersion: runtime.Version(),
		Framework:      framework,
		OSArch:         runtime.GOOS + "/" + runtime.GOARCH,
		AppVersion:     os.Getenv("DATAFLOW_APP_VERSION"),
		Dependencies:   deps,
	}
}

// httpBaseURL resolves the HTTP API base for manifest reporting: an
// explicit DATAFLOW_HTTP_URL wins (needed when the gRPC DATAFLOW_ENDPOINT
// is a bare host:port); URL-form endpoints map directly; a bare gRPC
// endpoint with no override has no derivable HTTP base and reporting is
// skipped.
func httpBaseURL(endpoint string, useTLS bool) string {
	if v := strings.TrimSpace(os.Getenv("DATAFLOW_HTTP_URL")); v != "" {
		return strings.TrimRight(v, "/")
	}
	switch {
	case strings.HasPrefix(endpoint, "https://"), strings.HasPrefix(endpoint, "http://"):
		return strings.TrimRight(endpoint, "/")
	case useTLS:
		return "https://" + endpoint
	default:
		return ""
	}
}

// sendManifest reports the build manifest once per process. Best-effort:
// short timeout, silent failures, runs on its own goroutine so startup is
// never delayed.
func sendManifest(s *settings) {
	base := httpBaseURL(s.endpoint, s.useTLS)
	if base == "" || s.cfg.APIKey == "" {
		return
	}
	go func() {
		bi, ok := debug.ReadBuildInfo()
		if !ok {
			return
		}
		body, err := json.Marshal(buildManifest(bi, s.cfg.ServiceName))
		if err != nil {
			return
		}
		req, err := http.NewRequest(http.MethodPost, base+"/api/v1/manifest", bytes.NewReader(body))
		if err != nil {
			return
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-Api-Key", s.cfg.APIKey)
		client := &http.Client{Timeout: 5 * time.Second}
		if s.cfg.Insecure {
			client.Transport = &http.Transport{
				TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, //nolint:gosec // explicit dev opt-in
			}
		}
		resp, err := client.Do(req)
		if err != nil {
			return
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}()
}
