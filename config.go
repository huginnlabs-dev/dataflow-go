// Package dataflow is the HuginnLabs Dataflow SDK for Go.
//
// A single blank import auto-instruments the host application:
//
//	import _ "github.com/huginnlabs-dev/dataflow-go"
//
// The SDK reads its configuration from DATAFLOW_* environment variables and
// starts a background gRPC streaming client that ships trace events to the
// HuginnLabs SaaS ingestion endpoint. Applications that prefer explicit
// configuration call dataflow.Configure from their own init.
package dataflow

import (
	"crypto/rand"
	"encoding/hex"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Config controls how the SDK instruments the host process and where it
// ships events. The zero value is valid: every field has a sensible default
// resolved from the environment when Configure is not called.
type Config struct {
	// APIKey authenticates the process against a SaaS project. Required for
	// events to be accepted; the server resolves the project from the key.
	APIKey string
	// Endpoint is the gRPC ingestion address, e.g. "api.huginnlabs.com:9090"
	// or "https://api.huginnlabs.com" (TLS implied, :443 default).
	Endpoint string
	// ServiceName labels every event emitted by this process. Defaults to
	// the OS hostname.
	ServiceName string
	// EncryptionKey, when set, encrypts captured payloads client-side with
	// AES-256-GCM (key derived via PBKDF2). Empty means payloads travel as
	// plaintext JSON — a warning is logged once at startup.
	EncryptionKey string
	// SensitivePaths lists dotted attribute paths whose values are redacted
	// before capture, e.g. "http.headers.authorization".
	SensitivePaths []string
	// SampleRatio is the fraction of traced spans shipped, in [0,1]. Zero
	// means 1 (full fidelity).
	SampleRatio float64
	// BufferSize bounds the in-memory replay buffer; the oldest events are
	// dropped on overflow. Defaults to 10k.
	BufferSize int
	// MaxBodyBytes caps how many bytes of an HTTP request body are captured
	// per span. Defaults to 4096.
	MaxBodyBytes int
	// Insecure disables TLS even for https:// endpoints (local dev only).
	Insecure bool
	// Logger receives SDK diagnostics; defaults to the standard logger.
	Logger *log.Logger
}

type settings struct {
	cfg          Config
	sensitive    map[string]struct{}
	sampleRatio  float64
	encrypted    bool
	endpoint     string
	useTLS       bool
	maxBodyBytes int
}

var (
	configured atomic.Pointer[settings]
	active     atomic.Bool
	startOnce  sync.Once
)

// Configure applies cfg and starts the SDK. Safe to call multiple times;
// the last configuration wins. Called implicitly by the package init with
// values from DATAFLOW_* environment variables.
func Configure(cfg Config) {
	s := resolve(cfg)
	configured.Store(s)
	startOnce.Do(func() { startSender(s) })
	active.Store(true)
}

// Enabled reports whether the SDK is shipping events.
func Enabled() bool { return active.Load() }

func current() *settings {
	if s := configured.Load(); s != nil {
		return s
	}
	s := resolve(Config{})
	configured.Store(s)
	return s
}

// logger falls back to the standard logger when none is configured.
func (s *settings) logger() *log.Logger {
	if s.cfg.Logger != nil {
		return s.cfg.Logger
	}
	return log.Default()
}

// loadEnvConfig builds a Config from the DATAFLOW_* environment.
func loadEnvConfig() Config {
	return Config{
		APIKey:         os.Getenv("DATAFLOW_API_KEY"),
		Endpoint:       firstNonEmpty(os.Getenv("DATAFLOW_ENDPOINT"), "api.huginnlabs.com:9090"),
		ServiceName:    firstNonEmpty(os.Getenv("DATAFLOW_SERVICE_NAME"), defaultServiceName()),
		EncryptionKey:  os.Getenv("DATAFLOW_ENCRYPTION_KEY"),
		SensitivePaths: splitCSV(os.Getenv("DATAFLOW_SENSITIVE_PATHS")),
		SampleRatio:    parseFloat(os.Getenv("DATAFLOW_SAMPLE_RATIO"), 1),
		BufferSize:     int(parseInt(os.Getenv("DATAFLOW_BUFFER_SIZE"), 10000)),
		MaxBodyBytes:   int(parseInt(os.Getenv("DATAFLOW_MAX_BODY_BYTES"), 4096)),
		Insecure:       parseBool(os.Getenv("DATAFLOW_INSECURE")),
	}
}

func resolve(cfg Config) *settings {
	s := &settings{
		cfg:          cfg,
		sampleRatio:  clampRatio(cfg.SampleRatio),
		encrypted:    cfg.EncryptionKey != "",
		maxBodyBytes: orDefault(cfg.MaxBodyBytes, 4096),
		sensitive:    map[string]struct{}{},
	}
	for _, p := range cfg.SensitivePaths {
		s.sensitive[strings.ToLower(p)] = struct{}{}
	}
	s.endpoint, s.useTLS = parseEndpoint(cfg.Endpoint, cfg.Insecure)
	if s.cfg.ServiceName == "" {
		s.cfg.ServiceName = defaultServiceName()
	}
	if s.cfg.Logger == nil {
		s.cfg.Logger = log.Default()
	}
	return s
}

func parseEndpoint(endpoint string, insecure bool) (string, bool) {
	endpoint = strings.TrimSpace(endpoint)
	switch {
	case endpoint == "":
		return "", false
	case strings.HasPrefix(endpoint, "https://"):
		host := ensurePort(strings.TrimPrefix(endpoint, "https://"), "443")
		return host, !insecure
	case strings.HasPrefix(endpoint, "http://"):
		host := ensurePort(strings.TrimPrefix(endpoint, "http://"), "80")
		return host, false
	default:
		return endpoint, false
	}
}

func ensurePort(host, port string) string {
	if _, _, err := net.SplitHostPort(host); err != nil {
		return host + ":" + port
	}
	return host
}

func defaultServiceName() string {
	h, err := os.Hostname()
	if err != nil || h == "" {
		return "unknown-service"
	}
	return h
}

func clampRatio(r float64) float64 {
	if r <= 0 {
		return 1
	}
	return min(r, 1)
}

// newID returns a random 16-char hex identifier.
func newID() string {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return strconv.FormatInt(time.Now().UnixNano(), 36)
	}
	return hex.EncodeToString(b[:])
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}

func orDefault(v, def int) int {
	if v == 0 {
		return def
	}
	return v
}

func splitCSV(s string) []string {
	if s == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func parseFloat(s string, def float64) float64 {
	if s == "" {
		return def
	}
	v, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return def
	}
	return v
}

func parseInt(s string, def int64) int64 {
	if s == "" {
		return def
	}
	v, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return def
	}
	return v
}

func parseBool(s string) bool {
	v, err := strconv.ParseBool(s)
	return err == nil && v
}
