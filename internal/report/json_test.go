package report

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/lame13/pgvector-index-manager/internal/config"
	"github.com/lame13/pgvector-index-manager/internal/reconciler"
)

func TestWriteJSONUsesPrivatePermissionsAndRedactsConnection(t *testing.T) {
	outputDir := t.TempDir()
	path := filepath.Join(outputDir, "report.json")
	if err := os.WriteFile(path, []byte("old"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, 0644); err != nil {
		t.Fatal(err)
	}

	cfg := &config.Config{
		Connection: config.ConnectionConfig{DSN: "postgres://secret@database"},
		Index:      config.IndexConfig{Name: "secret_idx", Type: "vector", Metric: "cosine", Dimensions: 128},
		Reconcile:  config.ReconcileConfig{OwnershipTag: "test"},
		Report: config.ReportConfig{
			OutputDir:        outputDir,
			TargetLabel:      "target",
			RedactConnection: true,
		},
	}

	result := &reconciler.ApplyResult{
		Table:     "private.documents",
		DryRun:    false,
		StartTime: time.Unix(1, 0),
		Actions: []reconciler.ActionResult{
			{Kind: reconciler.ActionBuild, Description: "Create secret_idx"},
		},
	}

	if err := WriteJSON("0.1.0", result, cfg); err != nil {
		t.Fatalf("WriteJSON() error = %v", err)
	}

	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := info.Mode().Perm(); got != 0600 {
		t.Fatalf("report permissions = %o, want 600", got)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	text := string(data)
	if strings.Contains(text, "postgres://secret") {
		t.Fatalf("report leaked connection string: %s", text)
	}
	if !strings.Contains(text, `"connection": "[redacted]"`) {
		t.Fatalf("report did not redact connection: %s", text)
	}
	if !strings.Contains(text, `"total_actions": 1`) {
		t.Fatalf("report did not count actions: %s", text)
	}
	for _, secret := range []string{"private.documents", "secret_idx", `"ownership_tag"`} {
		if strings.Contains(text, secret) {
			t.Fatalf("privacy-safe report leaked %q: %s", secret, text)
		}
	}
	if !strings.Contains(text, `"description": "Build and verify replacement index"`) {
		t.Fatalf("report did not generalize action description: %s", text)
	}
}

func TestWriteJSONForTargetWritesOneFilePerTarget(t *testing.T) {
	outputDir := t.TempDir()
	cfg := &config.Config{
		Connection: config.ConnectionConfig{DSN: "postgres://secret@database"},
		Index:      config.IndexConfig{Name: "documents_active_idx", Type: "vector", Metric: "cosine", Dimensions: 128},
		Reconcile:  config.ReconcileConfig{OwnershipTag: "test"},
		Report: config.ReportConfig{
			OutputDir:        outputDir,
			TargetLabel:      "active",
			RedactConnection: true,
		},
	}
	result := &reconciler.ApplyResult{StartTime: time.Unix(1, 0)}

	if err := os.WriteFile(filepath.Join(outputDir, "report.json"), []byte("legacy report"), 0600); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"active", "premium"} {
		cfg.Report.TargetLabel = name
		if err := WriteJSONForTarget("0.2.0", result, cfg, name); err != nil {
			t.Fatalf("WriteJSONForTarget(%s) error = %v", name, err)
		}
		info, err := os.Stat(filepath.Join(outputDir, "report-"+name+".json"))
		if err != nil || info.Mode().Perm() != 0600 {
			t.Fatalf("report %s must exist with private permissions: %v/%v", name, info, err)
		}
	}

	data, err := os.ReadFile(filepath.Join(outputDir, "report-active.json"))
	if err != nil {
		t.Fatalf("reading per-target report: %v", err)
	}
	if !strings.Contains(string(data), `"target": "active"`) {
		t.Fatalf("per-target report lost its label: %s", data)
	}
	data, err = os.ReadFile(filepath.Join(outputDir, "report.json"))
	if err != nil || string(data) != "legacy report" {
		t.Fatalf("per-target report overwrote report.json: %q/%v", data, err)
	}
}
