package scan

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

const ginSrc = `package server

import "github.com/gin-gonic/gin"

func Register(r *gin.Engine) {
	r.GET("/healthz", Health)
	g := r.Group("/api")
	{
		g.GET("/orders/:id", GetOrder)
		g.POST("/orders", CreateOrder)
		g.Any("/ping", Ping)
	}
}
`

const chiSrc = `package server

import "github.com/go-chi/chi/v5"

func Register(r chi.Router) {
	r.Get("/readyz", Health)
	r.Route("/api", func(api chi.Router) {
		api.Get("/users", ListUsers)
		api.Route("/users", func(u chi.Router) {
			u.Get("/{id}", GetUser)
		})
	})
}
`

const muxSrc = `package server

import (
	"net/http"

	"github.com/gorilla/mux"
)

func Register() {
	r := mux.NewRouter()
	r.HandleFunc("/healthz", Health)
	r.HandleFunc("/orders", ListOrders).Methods("GET")
	r.HandleFunc("/orders", CreateOrder).Methods(http.MethodPost)
}
`

const stdSrc = `package server

import "net/http"

func Register() {
	http.HandleFunc("GET /orders/{id}", GetOrder)
	http.HandleFunc("/readyz", Health)
	mux := http.NewServeMux()
	mux.HandleFunc("POST /users", CreateUser)
	mux.HandleFunc("/items", ListItems)
}
`

// dynamicSrc only registers dynamic paths — nothing may be extracted.
const dynamicSrc = `package server

import "github.com/gin-gonic/gin"

func Register(r *gin.Engine) {
	p := "/dynamic"
	r.GET(p, Dynamic)
	r.POST("/pre"+p, Dynamic)
}
`

const skippedSrc = `package server

import "github.com/gin-gonic/gin"

func Register(r *gin.Engine) {
	r.GET("/skipped", Skipped)
}
`

// writeTree materializes name → source files under a fresh temp dir.
func writeTree(t *testing.T, files map[string]string) string {
	t.Helper()
	dir := t.TempDir()
	for name, src := range files {
		path := filepath.Join(dir, filepath.FromSlash(name))
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(src), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

func TestRoutes(t *testing.T) {
	dir := writeTree(t, map[string]string{
		"routes_gin.go":        ginSrc,
		"routes_chi.go":        chiSrc,
		"routes_mux.go":        muxSrc,
		"routes_std.go":        stdSrc,
		"routes_extra_test.go": skippedSrc, // _test.go is skipped
		"vendor/x/v.go":        skippedSrc, // vendor/ is skipped
		"testdata/data.go":     skippedSrc, // testdata/ is skipped
		"notes.txt":            skippedSrc, // non-.go is skipped
	})
	got, err := Routes(dir)
	if err != nil {
		t.Fatal(err)
	}
	want := []Route{
		{Method: "ANY", Path: "/api/ping", Handler: "Ping", SourceFile: "routes_gin.go"},
		{Method: "ANY", Path: "/healthz", Handler: "Health", SourceFile: "routes_mux.go"},
		{Method: "ANY", Path: "/items", Handler: "ListItems", SourceFile: "routes_std.go"},
		{Method: "ANY", Path: "/readyz", Handler: "Health", SourceFile: "routes_std.go"},
		{Method: "GET", Path: "/api/orders/:id", Handler: "GetOrder", SourceFile: "routes_gin.go"},
		{Method: "GET", Path: "/api/users", Handler: "ListUsers", SourceFile: "routes_chi.go"},
		{Method: "GET", Path: "/api/users/{id}", Handler: "GetUser", SourceFile: "routes_chi.go"},
		{Method: "GET", Path: "/healthz", Handler: "Health", SourceFile: "routes_gin.go"},
		{Method: "GET", Path: "/orders", Handler: "ListOrders", SourceFile: "routes_mux.go"},
		{Method: "GET", Path: "/orders/{id}", Handler: "GetOrder", SourceFile: "routes_std.go"},
		{Method: "GET", Path: "/readyz", Handler: "Health", SourceFile: "routes_chi.go"},
		{Method: "POST", Path: "/api/orders", Handler: "CreateOrder", SourceFile: "routes_gin.go"},
		{Method: "POST", Path: "/orders", Handler: "CreateOrder", SourceFile: "routes_mux.go"},
		{Method: "POST", Path: "/users", Handler: "CreateUser", SourceFile: "routes_std.go"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("routes mismatch:\n got: %v\nwant: %v", got, want)
	}
}

func TestRoutesSkipsDynamicPaths(t *testing.T) {
	dir := writeTree(t, map[string]string{"routes.go": dynamicSrc})
	got, err := Routes(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Fatalf("got %d routes, want 0: %v", len(got), got)
	}
}

func TestRoutesMissingDir(t *testing.T) {
	if _, err := Routes(filepath.Join(t.TempDir(), "nope")); err == nil {
		t.Fatal("expected an error for a missing directory")
	}
	dir := writeTree(t, map[string]string{"main.go": "package main\n"})
	file := filepath.Join(dir, "main.go")
	if _, err := Routes(file); err == nil || !strings.Contains(err.Error(), "not a directory") {
		t.Fatalf("err = %v, want not-a-directory failure", err)
	}
}

func TestFinalizeDedupesSortsAndCaps(t *testing.T) {
	in := []Route{
		{Method: "POST", Path: "/b", Handler: "H", SourceFile: "f.go"},
		{Method: "GET", Path: "/a", Handler: "H", SourceFile: "f.go"},
		{Method: "POST", Path: "/b", Handler: "H", SourceFile: "g.go"}, // dup method+path
	}
	want := []Route{
		{Method: "GET", Path: "/a", Handler: "H", SourceFile: "f.go"},
		{Method: "POST", Path: "/b", Handler: "H", SourceFile: "f.go"},
	}
	if got := finalize(in); !reflect.DeepEqual(got, want) {
		t.Fatalf("finalize = %v, want %v", got, want)
	}
	many := make([]Route, 0, maxRoutes+10)
	for i := 0; i < maxRoutes+10; i++ {
		many = append(many, Route{Method: "GET", Path: fmt.Sprintf("/p%04d", i), Handler: "H", SourceFile: "f.go"})
	}
	if got := finalize(many); len(got) != maxRoutes {
		t.Fatalf("len(finalize) = %d, want %d", len(got), maxRoutes)
	}
}

func TestPost(t *testing.T) {
	var (
		calls    int
		gotPath  string
		gotKey   string
		gotBody  Catalog
		gotCType string
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		gotPath = r.URL.Path
		gotKey = r.Header.Get("X-Api-Key")
		gotCType = r.Header.Get("Content-Type")
		if err := json.NewDecoder(r.Body).Decode(&gotBody); err != nil {
			t.Errorf("decode request body: %v", err)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	routes := []Route{
		{Method: "GET", Path: "/orders/:id", Handler: "GetOrder", SourceFile: "server/routes.go"},
	}
	if err := Post(srv.URL, "secret-key", "shop", routes); err != nil {
		t.Fatalf("Post: %v", err)
	}
	if calls != 1 {
		t.Fatalf("calls = %d, want 1", calls)
	}
	if gotPath != "/api/v1/catalog" {
		t.Fatalf("path = %q, want /api/v1/catalog", gotPath)
	}
	if gotKey != "secret-key" {
		t.Fatalf("X-Api-Key = %q, want secret-key", gotKey)
	}
	if !strings.HasPrefix(gotCType, "application/json") {
		t.Fatalf("Content-Type = %q", gotCType)
	}
	if gotBody.ServiceName != "shop" {
		t.Fatalf("service_name = %q, want shop", gotBody.ServiceName)
	}
	if !reflect.DeepEqual(gotBody.Routes, routes) {
		t.Fatalf("routes = %+v, want %+v", gotBody.Routes, routes)
	}
}

func TestPostErrors(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "nope", http.StatusForbidden)
	}))
	defer srv.Close()

	err := Post(srv.URL, "k", "shop", nil)
	if err == nil || !strings.Contains(err.Error(), "403") {
		t.Fatalf("err = %v, want a 403 failure", err)
	}
	// Unreachable base (nothing listens on port 1).
	if err := Post("http://127.0.0.1:1", "k", "shop", nil); err == nil {
		t.Fatal("expected a transport error for an unreachable base")
	}
}

func TestResolveHTTPBase(t *testing.T) {
	t.Setenv("DATAFLOW_HTTP_URL", "")
	t.Setenv("DATAFLOW_ENDPOINT", "")
	if got := ResolveHTTPBase(" https://scan.example.com/ "); got != "https://scan.example.com" {
		t.Fatalf("flag = %q", got)
	}

	t.Setenv("DATAFLOW_HTTP_URL", "https://override.example.com/")
	if got := ResolveHTTPBase(""); got != "https://override.example.com" {
		t.Fatalf("env override = %q", got)
	}
	// The explicit flag beats the env override.
	if got := ResolveHTTPBase("http://flag.example.com"); got != "http://flag.example.com" {
		t.Fatalf("flag precedence = %q", got)
	}

	t.Setenv("DATAFLOW_HTTP_URL", "")
	t.Setenv("DATAFLOW_ENDPOINT", "https://ingest.example.com")
	if got := ResolveHTTPBase(""); got != "https://ingest.example.com" {
		t.Fatalf("url-form endpoint = %q", got)
	}

	// A bare host:port gRPC endpoint has no derivable HTTP base.
	t.Setenv("DATAFLOW_ENDPOINT", "ingest.example.com:9090")
	if got := ResolveHTTPBase(""); got != "" {
		t.Fatalf("bare endpoint = %q, want empty", got)
	}
}
