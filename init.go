package dataflow

import (
	"os"
	"strings"
)

// The blank import auto-starts the SDK from the environment:
//
//	DATAFLOW_API_KEY          required to ship events
//	DATAFLOW_ENDPOINT         gRPC ingestion address (default api.huginnlabs.com:9090)
//	DATAFLOW_SERVICE_NAME     service label (default hostname)
//	DATAFLOW_ENCRYPTION_KEY   enables AES-256-GCM payload encryption
//	DATAFLOW_SAMPLE_RATIO     fraction of spans shipped, 0..1 (default 1)
//	DATAFLOW_BUFFER_SIZE      replay buffer capacity (default 10000)
//	DATAFLOW_MAX_BODY_BYTES   request body capture cap (default 4096)
//	DATAFLOW_SENSITIVE_PATHS  comma-separated attribute paths to redact
//	DATAFLOW_INSECURE         disable TLS for https:// endpoints (dev only)
//	DATAFLOW_DISABLED         set truthy to keep the SDK off entirely
func init() {
	if parseBool(os.Getenv("DATAFLOW_DISABLED")) ||
		strings.EqualFold(os.Getenv("DATAFLOW_ENABLED"), "false") {
		return
	}
	Configure(loadEnvConfig())
}
