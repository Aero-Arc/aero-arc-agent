package wal

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
)

func TestDurableOperationCommandNamespaceAndLegacyFence(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "commands.db")
	w, err := New(ctx, path, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	record := CommandRecord{Digest: "digest", Payload: []byte{}, Evidence: []byte{}}
	if _, err = w.ClearOperationContext(ctx, "operation-first", ""); err != nil {
		t.Fatal(err)
	}
	if err = w.AdmitCommand(ctx, "operation-first", record); !errors.Is(err, ErrCommandIdentityConflict) {
		t.Fatalf("operation then C2: %v", err)
	}
	if err = w.AdmitCommand(ctx, "c2-first", record); err != nil {
		t.Fatal(err)
	}
	if _, err = w.ClearOperationContext(ctx, "c2-first", ""); !errors.Is(err, ErrOperationCommandConflict) {
		t.Fatalf("C2 then operation: %v", err)
	}
	if ok, err := w.BeginCommandEffect(ctx, "c2-first", "digest"); !ok || err != nil {
		t.Fatalf("effect: %v %v", ok, err)
	}
	if err = w.AdmitCommand(ctx, "queued", record); err != nil {
		t.Fatal(err)
	}
	if err = w.RecordLegacyAircraftEffect(ctx); err != nil {
		t.Fatal(err)
	}
	if err = w.Close(); err != nil {
		t.Fatal(err)
	}
	w, err = New(ctx, path, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if ok, err := w.CommandIsLatest(ctx, "c2-first"); ok || err != nil {
		t.Fatalf("legacy effect lost across restart: %v %v", ok, err)
	}
	if ok, err := w.BeginCommandEffect(ctx, "queued", "digest"); ok || !errors.Is(err, ErrCommandSuperseded) {
		t.Fatalf("old queued authority survived legacy effect: %v %v", ok, err)
	}
	if err = w.AdmitCommand(ctx, "new", record); err != nil {
		t.Fatal(err)
	}
	if ok, err := w.BeginCommandEffect(ctx, "new", "digest"); !ok || err != nil {
		t.Fatalf("new authority blocked: %v %v", ok, err)
	}
}
