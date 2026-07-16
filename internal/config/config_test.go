package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const minimalConfig = `
connection:
  dsn: postgres://localhost/testdb
table:
  name: documents
  vector_column: embedding
index:
  name: documents_embedding_idx
  dimensions: 128
report:
  output_dir: ./reports
`

func TestLoadDefaultsAndExplicitZeroValues(t *testing.T) {
	path := writeTestConfig(t, `
connection:
  dsn: postgres://localhost/testdb
table:
  name: documents
  vector_column: embedding
index:
  name: documents_embedding_idx
  dimensions: 128
reconcile:
  ownership_tag: test-tool
report:
  output_dir: ./reports
`)

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.Connection.StatementTimeout != "30s" || cfg.Connection.LockTimeout != "5s" {
		t.Fatalf("timeout defaults = %q/%q, want 30s/5s", cfg.Connection.StatementTimeout, cfg.Connection.LockTimeout)
	}
	if cfg.Index.M != 16 || cfg.Index.EFConstruction != 64 {
		t.Fatalf("HNSW defaults = %d/%d, want 16/64", cfg.Index.M, cfg.Index.EFConstruction)
	}
	if cfg.Reconcile.OwnershipTag != "test-tool" {
		t.Fatalf("OwnershipTag = %q, want test-tool", cfg.Reconcile.OwnershipTag)
	}
}

func TestLoadRejectsUnknownFieldsAndInvalidValues(t *testing.T) {
	tests := []struct {
		name    string
		config  string
		wantErr string
	}{
		{name: "unknown field", config: minimalConfig + "unknown: true\n", wantErr: "field unknown not found"},
		{name: "invalid metric", config: `
connection:
  dsn: postgres://localhost/testdb
table:
  name: documents
  vector_column: embedding
index:
  name: documents_embedding_idx
  dimensions: 128
  metric: invalid
report:
  output_dir: ./reports
`, wantErr: "index.metric must be one of"},
		{name: "invalid type", config: `
connection:
  dsn: postgres://localhost/testdb
table:
  name: documents
  vector_column: embedding
index:
  name: documents_embedding_idx
  dimensions: 128
  type: invalid
report:
  output_dir: ./reports
`, wantErr: "index.type must be one of"},
		{name: "m too large", config: `
connection:
  dsn: postgres://localhost/testdb
table:
  name: documents
  vector_column: embedding
index:
  name: documents_embedding_idx
  dimensions: 128
  m: 101
report:
  output_dir: ./reports
`, wantErr: "index.m must be between 1 and 100"},
		{name: "ef_construction too large", config: `
connection:
  dsn: postgres://localhost/testdb
table:
  name: documents
  vector_column: embedding
index:
  name: documents_embedding_idx
  dimensions: 128
  ef_construction: 1001
report:
  output_dir: ./reports
`, wantErr: "index.ef_construction must be between 1 and 1000"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Load(writeTestConfig(t, tt.config))
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("Load() error = %v, want containing %q", err, tt.wantErr)
			}
		})
	}
}

func TestSQLIdentifiersAreQuoted(t *testing.T) {
	cfg := &Config{
		Table: TableConfig{Schema: "odd.schema", Name: `doc"uments`, VectorCol: "embedding"},
		Index: IndexConfig{Name: "test_idx", Type: "vector", Metric: "cosine"},
	}
	if got, want := cfg.TableSQL(), `"odd.schema"."doc""uments"`; got != want {
		t.Fatalf("TableSQL() = %q, want %q", got, want)
	}
}

func TestOpClass(t *testing.T) {
	cfg := &Config{
		Index: IndexConfig{Type: "halfvec", Metric: "l2"},
	}
	if got, want := cfg.OpClass(), "halfvec_l2_ops"; got != want {
		t.Fatalf("OpClass() = %q, want %q", got, want)
	}
}

func TestPopulationPredicateAndSpecHashAreOrderIndependent(t *testing.T) {
	cfg := &Config{
		Table: TableConfig{Schema: "public", Name: "documents", VectorCol: "embedding"},
		Index: IndexConfig{Name: "documents_embedding_idx", Type: "vector", Metric: "cosine", Dimensions: 128, M: 16, EFConstruction: 64},
		Population: PopulationConfig{Filters: []FilterConfig{
			{Column: "tier", Values: []string{"standard", "premium"}},
			{Column: "status", Values: []string{"active"}},
		}},
	}
	wantPredicate := `"status" = E'active' AND "tier" IN (E'premium', E'standard')`
	if got := cfg.PopulationPredicateSQL(); got != wantPredicate {
		t.Fatalf("PopulationPredicateSQL() = %q, want %q", got, wantPredicate)
	}
	firstHash := cfg.SpecHash()
	cfg.Population.Filters[0], cfg.Population.Filters[1] = cfg.Population.Filters[1], cfg.Population.Filters[0]
	cfg.Population.Filters[1].Values[0], cfg.Population.Filters[1].Values[1] = cfg.Population.Filters[1].Values[1], cfg.Population.Filters[1].Values[0]
	if got := cfg.SpecHash(); got != firstHash {
		t.Fatalf("SpecHash() changed after semantically neutral reordering: %s != %s", got, firstHash)
	}
}

func TestQuoteLiteralEscapesDDLValues(t *testing.T) {
	if got, want := QuoteLiteral(`a'b\c`), `E'a\'b\\c'`; got != want {
		t.Fatalf("QuoteLiteral() = %q, want %q", got, want)
	}
}

func TestLoadRejectsUnsafeOwnershipTagAndDimensionLimit(t *testing.T) {
	tests := []struct {
		name     string
		contents string
		wantErr  string
	}{
		{name: "unsafe ownership tag", contents: minimalConfig + "reconcile:\n  ownership_tag: \"x'; DROP TABLE documents; --\"\n", wantErr: "reconcile.ownership_tag"},
		{name: "vector dimensions too large", contents: strings.Replace(minimalConfig, "dimensions: 128", "dimensions: 2001", 1), wantErr: "between 1 and 2000"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Load(writeTestConfig(t, tt.contents))
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("Load() error = %v, want containing %q", err, tt.wantErr)
			}
		})
	}
}

func TestSampleConfigsLoad(t *testing.T) {
	for _, name := range []string{"sample-config.yaml", "sample-config-docker.yaml"} {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join("..", "..", "testdata", name)
			if _, err := Load(path); err != nil {
				t.Fatalf("Load(%s) error = %v", name, err)
			}
		})
	}
}

func writeTestConfig(t *testing.T, contents string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(contents), 0600); err != nil {
		t.Fatalf("writing config: %v", err)
	}
	return path
}
