// Command dataflow-scan statically extracts the HTTP endpoints a Go service
// declares in its source and posts them to the Dataflow server catalog, so
// routes appear in the dashboard before the service ever runs.
//
// Usage:
//
//	dataflow-scan --url https://dataflow.example.com --api-key KEY \
//	    --service shop --dir ./cmd/shop
//
// Configuration mirrors the SDK: --url beats DATAFLOW_HTTP_URL, which beats
// a URL-form DATAFLOW_ENDPOINT; a bare host:port endpoint is the gRPC ingest
// address with no derivable HTTP base, so the upload is skipped. The API key
// comes from --api-key or DATAFLOW_API_KEY; the service name from --service,
// DATAFLOW_SERVICE_NAME or the scanned directory's base name.
//
// Exit codes: 0 ok, 1 scan failure or skipped upload, 2 catalog POST failed.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/huginnlabs-dev/dataflow-go/scan"
)

func main() {
	log.SetFlags(0)
	log.SetPrefix("dataflow-scan: ")

	dir := flag.String("dir", ".", "Go service directory to scan")
	service := flag.String("service", "", "service name (default: DATAFLOW_SERVICE_NAME, then the directory base name)")
	urlFlag := flag.String("url", "", "Dataflow HTTP base URL (default: DATAFLOW_HTTP_URL, then URL-form DATAFLOW_ENDPOINT)")
	apiKey := flag.String("api-key", "", "project API key (default: DATAFLOW_API_KEY)")
	printJSON := flag.Bool("print", false, "print the extracted routes as JSON and exit without posting")
	flag.Parse()

	// --print is a dry run: extraction only, no endpoint or key required.
	if *printJSON {
		routes, nfiles, err := scanDir(*dir)
		if err != nil {
			log.Fatal(err) // exit 1: scan failed
		}
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		if err := enc.Encode(routes); err != nil {
			log.Fatal(err)
		}
		log.Print(summary(len(routes), nfiles, *dir))
		return
	}

	base := scan.ResolveHTTPBase(*urlFlag)
	if base == "" {
		log.Print("no HTTP base URL — pass --url or set DATAFLOW_HTTP_URL " +
			"(a bare DATAFLOW_ENDPOINT is the gRPC ingest address with no derivable " +
			"HTTP base); skipping the catalog upload")
		os.Exit(1) // skipped
	}
	key := firstNonEmpty(*apiKey, os.Getenv("DATAFLOW_API_KEY"))
	if key == "" {
		log.Print("no API key — pass --api-key or set DATAFLOW_API_KEY")
		os.Exit(1) // skipped
	}

	routes, nfiles, err := scanDir(*dir)
	if err != nil {
		log.Fatal(err) // exit 1: scan failed
	}
	log.Print(summary(len(routes), nfiles, *dir))
	if err := scan.Post(base, key, serviceName(*service, *dir), routes); err != nil {
		log.Print(err)
		os.Exit(2) // post failure
	}
	log.Printf("catalog published to %s/api/v1/catalog", base)
}

// scanDir runs the extraction and counts the files routes came from.
func scanDir(dir string) ([]scan.Route, int, error) {
	routes, err := scan.Routes(dir)
	if err != nil {
		return nil, 0, err
	}
	files := make(map[string]struct{}, len(routes))
	for _, r := range routes {
		files[r.SourceFile] = struct{}{}
	}
	return routes, len(files), nil
}

// serviceName resolves the --service > DATAFLOW_SERVICE_NAME > dir basename
// precedence.
func serviceName(flagService, dir string) string {
	if s := firstNonEmpty(flagService, os.Getenv("DATAFLOW_SERVICE_NAME")); s != "" {
		return s
	}
	clean := filepath.Clean(dir)
	if abs, err := filepath.Abs(clean); err == nil {
		clean = abs
	}
	return filepath.Base(clean)
}

func summary(n, nfiles int, dir string) string {
	return fmt.Sprintf("%d routes across %d files in %s", n, nfiles, dir)
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}
