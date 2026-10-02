package dataflow

import "testing"

// A process that imports the SDK with no DATAFLOW_* environment gets a
// passive start (no endpoint/key -> startSender bails). A later explicit
// Configure with a key must revive the pipeline — this is how the Dataflow
// server self-instruments from main().
func TestConfigureRevivesPassiveStart(t *testing.T) {
	if globalPipeline.Load() != nil {
		t.Skip("pipeline already started by another test; run with -count=1 isolation")
	}
	resetForTest()

	// Passive start: no endpoint, no key.
	Configure(Config{ServiceName: "passive"})
	if globalPipeline.Load() != nil {
		t.Fatal("pipeline started without endpoint/key")
	}

	// Revival: the same one-shot start path runs again, now with a key.
	Configure(Config{Endpoint: "localhost:9099", APIKey: "df_test", ServiceName: "revived"})
	if globalPipeline.Load() == nil {
		t.Fatal("Configure with endpoint+key did not revive the passive start")
	}
	if !Enabled() {
		t.Fatal("Enabled must report true after a successful start")
	}
}

// resetForTest restores the package-level start state so the revival path
// can run on a fresh instance.
func resetForTest() {
	startMu.Lock()
	started = false
	startMu.Unlock()
	globalPipeline.Store(nil)
}
