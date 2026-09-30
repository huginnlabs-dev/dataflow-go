// Package scan statically extracts HTTP routes from a Go service's source
// tree and publishes them to the Dataflow server catalog (POST
// /api/v1/catalog). Extraction walks every non-test .go file with go/parser
// and recognizes route registrations from the common Go routers — gin, echo,
// chi, fiber, gorilla/mux and the net/http ServeMux (Go 1.22 patterns).
// Only literal string paths are considered; anything dynamic is skipped.
package scan

import (
	"bytes"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

// maxRoutes matches the server's per-service catalog limit.
const maxRoutes = 1000

// Route is one HTTP endpoint extracted from source. The JSON field names
// match the POST /api/v1/catalog wire contract.
type Route struct {
	Method     string `json:"method"`
	Path       string `json:"path"`
	Handler    string `json:"handler"`
	SourceFile string `json:"source_file"`
}

// Catalog is the request body of POST /api/v1/catalog.
type Catalog struct {
	ServiceName string  `json:"service_name"`
	Routes      []Route `json:"routes"`
}

// routeMethods maps the registration method names we recognize to the HTTP
// verb. Upper-case names are gin/echo, mixed-case are chi/fiber, Any/All are
// the catch-all registrations. HandleFunc/Handle are handled separately
// (mux-style path patterns).
var routeMethods = map[string]string{
	"GET": "GET", "POST": "POST", "PUT": "PUT", "PATCH": "PATCH",
	"DELETE": "DELETE", "HEAD": "HEAD", "OPTIONS": "OPTIONS",
	"Get": "GET", "Post": "POST", "Put": "PUT", "Patch": "PATCH",
	"Delete": "DELETE", "Head": "HEAD", "Options": "OPTIONS",
	"Any": "ANY", "All": "ANY",
}

// stdlibMethods are the verbs allowed in a Go 1.22 ServeMux pattern such as
// "GET /items/{id}".
var stdlibMethods = map[string]bool{
	"GET": true, "POST": true, "PUT": true, "PATCH": true, "DELETE": true,
	"HEAD": true, "OPTIONS": true, "CONNECT": true, "TRACE": true,
}

// skipDirs are pruned during the source walk.
var skipDirs = map[string]bool{"vendor": true, ".git": true, "testdata": true}

// Routes walks dir and extracts the HTTP routes declared in its .go source
// (skipping _test.go files, vendor/, .git/ and testdata/). The result is
// deduplicated by (method, path), sorted for deterministic output and capped
// at 1000 entries. Files that fail to parse are skipped — the scan is
// best-effort by design.
func Routes(dir string) ([]Route, error) {
	info, err := os.Stat(dir)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("%s: not a directory", dir)
	}
	ex := &extractor{}
	fset := token.NewFileSet()
	werr := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if path != dir && skipDirs[d.Name()] {
				return fs.SkipDir
			}
			return nil
		}
		name := d.Name()
		if !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			return nil
		}
		rel, rerr := filepath.Rel(dir, path)
		if rerr != nil {
			rel = path
		}
		ex.extractFile(fset, path, filepath.ToSlash(rel))
		return nil
	})
	if werr != nil {
		return nil, werr
	}
	return finalize(ex.routes), nil
}

// finalize dedupes by (method, path), sorts for determinism and caps the
// list at maxRoutes.
func finalize(routes []Route) []Route {
	out := make([]Route, 0, len(routes))
	seen := make(map[string]struct{}, len(routes))
	for _, r := range routes {
		key := r.Method + " " + r.Path
		if _, dup := seen[key]; dup {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, r)
	}
	sort.Slice(out, func(i, j int) bool {
		a, b := out[i], out[j]
		switch {
		case a.Method != b.Method:
			return a.Method < b.Method
		case a.Path != b.Path:
			return a.Path < b.Path
		case a.SourceFile != b.SourceFile:
			return a.SourceFile < b.SourceFile
		default:
			return a.Handler < b.Handler
		}
	})
	if len(out) > maxRoutes {
		out = out[:maxRoutes]
	}
	return out
}

// extractor accumulates the routes found in one directory walk.
type extractor struct {
	routes []Route
	file   string // repo-relative path of the file being scanned
}

// scope tracks the route prefixes bound to receiver variable names. A child
// scope shadows its parent: chi's r.Route("/api", func(r chi.Router) {...})
// rebinds the parameter name inside the callback.
type scope struct {
	parent *scope
	prefix map[string]string
}

func (s *scope) lookup(name string) (string, bool) {
	for sc := s; sc != nil; sc = sc.parent {
		if p, ok := sc.prefix[name]; ok {
			return p, true
		}
	}
	return "", false
}

func (s *scope) bind(name, prefix string) {
	if s.prefix == nil {
		s.prefix = make(map[string]string)
	}
	s.prefix[name] = prefix
}

// extractFile parses one .go file and appends the routes it declares.
func (ex *extractor) extractFile(fset *token.FileSet, path, rel string) {
	f, err := parser.ParseFile(fset, path, nil, 0)
	if err != nil {
		return
	}
	prev := ex.file
	ex.file = rel
	defer func() { ex.file = prev }()
	root := &scope{}
	for _, decl := range f.Decls {
		switch d := decl.(type) {
		case *ast.FuncDecl:
			if d.Body != nil {
				ex.walkStmts(d.Body.List, root)
			}
		case *ast.GenDecl:
			for _, spec := range d.Specs {
				if vs, ok := spec.(*ast.ValueSpec); ok {
					for _, val := range vs.Values {
						ex.walkExpr(val, root)
					}
				}
			}
		}
	}
}

func (ex *extractor) walkStmts(stmts []ast.Stmt, sc *scope) {
	for _, s := range stmts {
		ex.walkStmt(s, sc)
	}
}

func (ex *extractor) walkStmt(stmt ast.Stmt, sc *scope) {
	switch s := stmt.(type) {
	case *ast.ExprStmt:
		ex.walkExpr(s.X, sc)
	case *ast.AssignStmt:
		// g := r.Group("/api") — remember the group prefix for g.
		if len(s.Lhs) == 1 && len(s.Rhs) == 1 {
			if name, ok := s.Lhs[0].(*ast.Ident); ok {
				if prefix, ok := groupPrefix(s.Rhs[0], sc); ok {
					sc.bind(name.Name, prefix)
				}
			}
		}
		for _, rhs := range s.Rhs {
			ex.walkExpr(rhs, sc)
		}
	case *ast.IfStmt:
		ex.walkExpr(s.Cond, sc)
		ex.walkStmts(s.Body.List, sc)
		if s.Else != nil {
			ex.walkStmt(s.Else, sc)
		}
	case *ast.ForStmt:
		ex.walkStmts(s.Body.List, sc)
	case *ast.RangeStmt:
		ex.walkStmts(s.Body.List, sc)
	case *ast.GoStmt:
		ex.walkExpr(s.Call, sc)
	case *ast.DeferStmt:
		ex.walkExpr(s.Call, sc)
	case *ast.ReturnStmt:
		for _, r := range s.Results {
			ex.walkExpr(r, sc)
		}
	case *ast.BlockStmt:
		ex.walkStmts(s.List, sc)
	case *ast.SwitchStmt:
		ex.walkStmts(s.Body.List, sc)
	case *ast.TypeSwitchStmt:
		ex.walkStmts(s.Body.List, sc)
	case *ast.SelectStmt:
		ex.walkStmts(s.Body.List, sc)
	case *ast.CaseClause:
		ex.walkStmts(s.Body, sc)
	case *ast.CommClause:
		ex.walkStmts(s.Body, sc)
	case *ast.LabeledStmt:
		ex.walkStmt(s.Stmt, sc)
	case *ast.IncDecStmt:
		ex.walkExpr(s.X, sc)
	case *ast.SendStmt:
		ex.walkExpr(s.Chan, sc)
		ex.walkExpr(s.Value, sc)
	case *ast.DeclStmt:
		if d, ok := s.Decl.(*ast.GenDecl); ok {
			for _, spec := range d.Specs {
				if vs, ok := spec.(*ast.ValueSpec); ok {
					for _, val := range vs.Values {
						ex.walkExpr(val, sc)
					}
				}
			}
		}
	}
}

func (ex *extractor) walkExpr(expr ast.Expr, sc *scope) {
	switch e := expr.(type) {
	case *ast.CallExpr:
		ex.call(e, sc)
	case *ast.FuncLit:
		ex.walkStmts(e.Body.List, &scope{parent: sc})
	case *ast.ParenExpr:
		ex.walkExpr(e.X, sc)
	case *ast.UnaryExpr:
		ex.walkExpr(e.X, sc)
	case *ast.BinaryExpr:
		ex.walkExpr(e.X, sc)
		ex.walkExpr(e.Y, sc)
	case *ast.StarExpr:
		ex.walkExpr(e.X, sc)
	case *ast.CompositeLit:
		for _, elt := range e.Elts {
			ex.walkExpr(elt, sc)
		}
	case *ast.KeyValueExpr:
		ex.walkExpr(e.Value, sc)
	case *ast.IndexExpr:
		ex.walkExpr(e.X, sc)
	case *ast.IndexListExpr:
		ex.walkExpr(e.X, sc)
	case *ast.SliceExpr:
		ex.walkExpr(e.X, sc)
	case *ast.SelectorExpr:
		ex.walkExpr(e.X, sc)
	case *ast.TypeAssertExpr:
		ex.walkExpr(e.X, sc)
	}
}

// call inspects one call expression for a route registration and keeps
// walking its arguments for inline handler literals.
func (ex *extractor) call(ce *ast.CallExpr, sc *scope) {
	sel, ok := ce.Fun.(*ast.SelectorExpr)
	if !ok {
		for _, a := range ce.Args {
			ex.walkExpr(a, sc)
		}
		return
	}
	recv, _ := receiverIdent(sel.X)
	switch sel.Sel.Name {
	case "Route": // chi: r.Route("/base", func(r chi.Router) { ... })
		if len(ce.Args) == 2 {
			if base, ok := stringLit(ce.Args[0]); ok && strings.HasPrefix(base, "/") {
				if fl, ok := ce.Args[1].(*ast.FuncLit); ok {
					if names := paramIdents(fl.Type.Params); len(names) > 0 {
						inner := &scope{parent: sc}
						prefix := joinPath(prefixOf(sc, recv), base)
						for _, n := range names {
							inner.bind(n, prefix)
						}
						ex.walkStmts(fl.Body.List, inner)
						return
					}
				}
			}
		}
	case "Group":
		// chi-style r.Group(func(r chi.Router) { ... }): the callback
		// inherits the receiver's prefix. The gin/echo/fiber form
		// g := r.Group("/api") is handled by groupPrefix on assignment.
		if len(ce.Args) > 0 {
			if fl, ok := ce.Args[0].(*ast.FuncLit); ok {
				if names := paramIdents(fl.Type.Params); len(names) > 0 {
					inner := &scope{parent: sc}
					prefix := prefixOf(sc, recv)
					for _, n := range names {
						inner.bind(n, prefix)
					}
					ex.walkStmts(fl.Body.List, inner)
					return
				}
			}
		}
	case "HandleFunc", "Handle":
		if r, ok := ex.muxRoute(ce, sc, "", prefixOf(sc, recv)); ok {
			ex.routes = append(ex.routes, r)
		}
	case "Methods":
		// gorilla/mux chain: r.HandleFunc("/orders", h).Methods("GET", ...)
		if inner, ok := sel.X.(*ast.CallExpr); ok {
			if isel, ok := inner.Fun.(*ast.SelectorExpr); ok &&
				(isel.Sel.Name == "HandleFunc" || isel.Sel.Name == "Handle") {
				irecv, _ := receiverIdent(isel.X)
				if m, ok := firstMethod(ce.Args); ok {
					if r, ok := ex.muxRoute(inner, sc, m, prefixOf(sc, irecv)); ok {
						ex.routes = append(ex.routes, r)
					}
				}
				return // the inner call is consumed either way
			}
		}
		return
	default:
		if method, ok := routeMethods[sel.Sel.Name]; ok && len(ce.Args) >= 2 {
			if path, ok := stringLit(ce.Args[0]); ok && strings.HasPrefix(path, "/") {
				ex.routes = append(ex.routes, Route{
					Method:     method,
					Path:       joinPath(prefixOf(sc, recv), path),
					Handler:    handlerName(ce.Args[len(ce.Args)-1]),
					SourceFile: ex.file,
				})
			}
		}
	}
	// Keep walking: inline handler literals may register routes of their own.
	ex.walkExpr(sel.X, sc)
	for _, a := range ce.Args {
		ex.walkExpr(a, sc)
	}
}

// groupPrefix recognizes `g := r.Group("/api")` and returns the full prefix
// bound to g (the receiver's own prefix plus the group base).
func groupPrefix(rhs ast.Expr, sc *scope) (string, bool) {
	call, ok := rhs.(*ast.CallExpr)
	if !ok || len(call.Args) != 1 {
		return "", false
	}
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok || sel.Sel.Name != "Group" {
		return "", false
	}
	base, ok := stringLit(call.Args[0])
	if !ok || !strings.HasPrefix(base, "/") {
		return "", false
	}
	name, _ := receiverIdent(sel.X)
	return joinPath(prefixOf(sc, name), base), true
}

// muxRoute extracts a route from a HandleFunc/Handle call. The pattern
// follows net/http ServeMux syntax: an optional upper-case method prefix
// ("GET /items/{id}"); a bare path means any method. A non-empty
// chainedMethod (from a gorilla .Methods("GET") chain) overrides both.
func (ex *extractor) muxRoute(ce *ast.CallExpr, sc *scope, chainedMethod, prefix string) (Route, bool) {
	if len(ce.Args) < 2 {
		return Route{}, false
	}
	pattern, ok := stringLit(ce.Args[0])
	if !ok {
		return Route{}, false
	}
	method, path := "ANY", pattern
	if i := strings.IndexByte(pattern, ' '); i > 0 {
		if m := pattern[:i]; stdlibMethods[m] {
			if rest := strings.TrimSpace(pattern[i+1:]); strings.HasPrefix(rest, "/") {
				method, path = m, rest
			}
		}
	}
	if chainedMethod != "" {
		method = chainedMethod
	}
	if !strings.HasPrefix(path, "/") {
		return Route{}, false
	}
	return Route{
		Method:     method,
		Path:       joinPath(prefix, path),
		Handler:    handlerName(ce.Args[len(ce.Args)-1]),
		SourceFile: ex.file,
	}, true
}

// firstMethod returns the first method argument of a gorilla .Methods(...)
// call: a string literal ("GET") or an http.MethodX selector.
func firstMethod(args []ast.Expr) (string, bool) {
	if len(args) == 0 {
		return "", false
	}
	switch m := args[0].(type) {
	case *ast.BasicLit:
		if s, err := strconv.Unquote(m.Value); err == nil && s != "" {
			return strings.ToUpper(s), true
		}
	case *ast.SelectorExpr: // http.MethodGet, http.MethodPost, ...
		if name := strings.TrimPrefix(m.Sel.Name, "Method"); name != m.Sel.Name && name != "" {
			return strings.ToUpper(name), true
		}
	}
	return "", false
}

// handlerName reduces a handler expression to its last identifier
// (GetOrder, h.GetOrder → "GetOrder"; an inline func literal → "<func>").
func handlerName(e ast.Expr) string {
	switch h := e.(type) {
	case *ast.Ident:
		return h.Name
	case *ast.SelectorExpr:
		return h.Sel.Name
	case *ast.FuncLit:
		return "<func>"
	default:
		return ""
	}
}

// receiverIdent returns the identifier name a method is called on, if it is
// a plain identifier. Chained or computed receivers yield "".
func receiverIdent(x ast.Expr) (string, bool) {
	if id, ok := x.(*ast.Ident); ok && id.Name != "_" {
		return id.Name, true
	}
	return "", false
}

// paramIdents returns the names of a function literal's parameters; unnamed
// parameters yield nothing.
func paramIdents(params *ast.FieldList) []string {
	if params == nil {
		return nil
	}
	var names []string
	for _, field := range params.List {
		for _, n := range field.Names {
			if n.Name != "_" {
				names = append(names, n.Name)
			}
		}
	}
	return names
}

// stringLit returns the unquoted value of a string literal expression.
func stringLit(e ast.Expr) (string, bool) {
	lit, ok := e.(*ast.BasicLit)
	if !ok || lit.Kind != token.STRING {
		return "", false
	}
	s, err := strconv.Unquote(lit.Value)
	if err != nil {
		return "", false
	}
	return s, true
}

// prefixOf resolves a receiver variable's group prefix; unknown receivers
// have none.
func prefixOf(sc *scope, name string) string {
	if name == "" {
		return ""
	}
	p, _ := sc.lookup(name)
	return p
}

// joinPath concatenates a group prefix and a route path ("/api" + "/x").
func joinPath(prefix, path string) string {
	switch {
	case prefix == "":
		return path
	case path == "/":
		return prefix
	default:
		return prefix + path
	}
}

// ResolveHTTPBase resolves the HTTP API base for the catalog upload: an
// explicit --url wins, then DATAFLOW_HTTP_URL (needed when the gRPC
// DATAFLOW_ENDPOINT is a bare host:port), then a URL-form endpoint. A bare
// gRPC endpoint with no override has no derivable HTTP base and yields "".
func ResolveHTTPBase(flagURL string) string {
	if v := strings.TrimSpace(flagURL); v != "" {
		return strings.TrimRight(v, "/")
	}
	if v := strings.TrimSpace(os.Getenv("DATAFLOW_HTTP_URL")); v != "" {
		return strings.TrimRight(v, "/")
	}
	endpoint := strings.TrimSpace(os.Getenv("DATAFLOW_ENDPOINT"))
	if strings.HasPrefix(endpoint, "http://") || strings.HasPrefix(endpoint, "https://") {
		return strings.TrimRight(endpoint, "/")
	}
	return ""
}

// Post publishes the extracted routes to the Dataflow server catalog at
// {base}/api/v1/catalog, authenticated with the project API key.
func Post(base, apiKey, service string, routes []Route) error {
	body, err := json.Marshal(Catalog{ServiceName: service, Routes: routes})
	if err != nil {
		return fmt.Errorf("encode catalog: %w", err)
	}
	req, err := http.NewRequest(http.MethodPost, base+"/api/v1/catalog", bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("build catalog request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Api-Key", apiKey)
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("post catalog: %w", err)
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return fmt.Errorf("post catalog: unexpected status %s", resp.Status)
	}
	return nil
}
