package dataflow

import (
	"os"
	"runtime"
	"strconv"
	"sync"
	"time"
)

// SDKVersion is stamped into every service's agent metadata.
const SDKVersion = "0.1.0"

var (
	agentOnce     sync.Once
	agentAttrs    [][2]string
	processStart  = time.Now()
)

// agentInfo builds the host/process descriptor once per process: OS, arch,
// runtime version, CPU budget, pid and uptime. Optional DATAFLOW_ENV and
// DATAFLOW_APP_VERSION let users tag deployments.
func agentInfo() [][2]string {
	agentOnce.Do(func() {
		add := func(k, v string) {
			if v != "" {
				agentAttrs = append(agentAttrs, [2]string{k, v})
			}
		}
		add("agent.os", runtime.GOOS+"/"+runtime.GOARCH)
		add("agent.runtime", "go "+runtime.Version())
		add("agent.sdk", "go-sdk/"+SDKVersion)
		add("agent.cpu", strconv.Itoa(runtime.GOMAXPROCS(0)))
		add("agent.pid", strconv.Itoa(os.Getpid()))
		add("agent.started", strconv.FormatInt(processStart.UnixMilli(), 10))
		add("agent.env", os.Getenv("DATAFLOW_ENV"))
		add("agent.app_version", os.Getenv("DATAFLOW_APP_VERSION"))
	})
	return agentAttrs
}

// stampAgent attaches the host descriptor to a root span (called by the
// HTTP middleware on entry-point spans).
func stampAgent(span *Span) {
	for _, kv := range agentInfo() {
		span.SetAttr(kv[0], kv[1])
	}
}
