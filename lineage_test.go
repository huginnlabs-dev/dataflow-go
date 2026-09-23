package dataflow

import (
	"context"
	"testing"
)

// End-to-end unit: a span with SetData must carry field-name lineage in its
// metadata when it is ended (the sender serializes ev.Metadata verbatim).
func TestDataFieldsMetadata(t *testing.T) {
	span := StartSpan(context.Background(), "test.Span")
	span.SetData("alpha", "1")
	span.SetData("beta", "2")
	span.End()

	span.mu.Lock()
	defer span.mu.Unlock()
	got := span.ev.Metadata["data.fields"]
	if got != "alpha,beta" {
		t.Fatalf("data.fields = %q, want %q", got, "alpha,beta")
	}
}

func TestFieldLineageAbsentWithoutData(t *testing.T) {
	span := StartSpan(context.Background(), "test.Plain")
	span.End()

	span.mu.Lock()
	defer span.mu.Unlock()
	if _, ok := span.ev.Metadata["data.fields"]; ok {
		t.Fatal("data.fields must be absent for spans without SetData")
	}
}
