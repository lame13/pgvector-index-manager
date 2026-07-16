package catalog

import (
	"strings"
	"testing"

	"github.com/lame13/pgvector-index-manager/internal/config"
)

func TestDetectDriftNoIndexes(t *testing.T) {
	cfg := testConfig()
	drift, reasons := detectDrift(nil, cfg)
	if !drift || len(reasons) == 0 || !strings.Contains(reasons[0], "not found") {
		t.Fatalf("detectDrift() = %t, %v", drift, reasons)
	}
}

func TestDetectDriftUnownedDesired(t *testing.T) {
	cfg := testConfig()
	indexes := []IndexInfo{{
		Name: cfg.Index.Name, IsDesired: true, IsHealthy: true,
		Valid: true, Ready: true, Live: true,
		OpClass: cfg.OpClass(), M: cfg.Index.M, EFConstruction: cfg.Index.EFConstruction,
		Predicate: "status = 'active'::text",
	}}
	drift, reasons := detectDrift(indexes, cfg)
	if !drift || !containsReason(reasons, "unowned") {
		t.Fatalf("detectDrift() = %t, %v, want unowned drift", drift, reasons)
	}
}

func TestDetectDriftNoDrift(t *testing.T) {
	cfg := testConfig()
	indexes := []IndexInfo{matchingDesired(cfg)}
	drift, reasons := detectDrift(indexes, cfg)
	if drift {
		t.Fatalf("detectDrift() should return false, reasons: %v", reasons)
	}
}

func TestDetectDriftOpClassAndParams(t *testing.T) {
	cfg := testConfig()
	idx := matchingDesired(cfg)
	idx.OpClass = "vector_l2_ops"
	idx.M = 32
	idx.EFConstruction = 128
	drift, reasons := detectDrift([]IndexInfo{idx}, cfg)
	for _, fragment := range []string{"opclass", "m=32", "ef_construction=128"} {
		if !drift || !containsReason(reasons, fragment) {
			t.Fatalf("detectDrift() = %t, %v, want %q", drift, reasons, fragment)
		}
	}
}

func TestDetectDriftScopesStaleIndexesToManagedFamily(t *testing.T) {
	cfg := testConfig()
	desired := matchingDesired(cfg)
	unrelated := matchingDesired(cfg)
	unrelated.Name = "other_population_idx"
	unrelated.IsDesired = false
	unrelated.ManagedName = "other_population_idx"
	if drift, reasons := detectDrift([]IndexInfo{desired, unrelated}, cfg); drift {
		t.Fatalf("unrelated managed family caused drift: %v", reasons)
	}
	unrelated.ManagedName = cfg.Index.Name
	if drift, reasons := detectDrift([]IndexInfo{desired, unrelated}, cfg); !drift || !containsReason(reasons, "awaiting retirement") {
		t.Fatalf("same managed family did not cause retirement drift: %t, %v", drift, reasons)
	}
}

func TestParseHNSWParams(t *testing.T) {
	tests := []struct {
		options string
		wantM   int
		wantEF  int
	}{
		{options: "m=16,ef_construction=64", wantM: 16, wantEF: 64},
		{options: "ef_construction=128,m=32", wantM: 32, wantEF: 128},
		{options: "m=32", wantM: 32, wantEF: 64},
		{options: "", wantM: 16, wantEF: 64},
	}
	for _, tt := range tests {
		m, ef := parseHNSWParams(tt.options)
		if m != tt.wantM || ef != tt.wantEF {
			t.Fatalf("parseHNSWParams(%q) = (%d, %d), want (%d, %d)", tt.options, m, ef, tt.wantM, tt.wantEF)
		}
	}
}

func TestStructuredOwnership(t *testing.T) {
	cfg := testConfig()
	comment := OwnershipComment(cfg)
	owned, legacy, retirement, managedName, specHash := ownership(comment, cfg.Reconcile.OwnershipTag)
	if !owned || legacy || retirement || managedName != cfg.Index.Name || specHash != cfg.SpecHash() {
		t.Fatalf("ownership(%q) = %t, %t, %t, %q, %q", comment, owned, legacy, retirement, managedName, specHash)
	}

	owned, legacy, retirement, managedName, specHash = ownership("/* pgvector-index-manager */", cfg.Reconcile.OwnershipTag)
	if !owned || !legacy || retirement || managedName != "" || specHash != "" {
		t.Fatalf("legacy ownership = %t, %t, %t, %q, %q", owned, legacy, retirement, managedName, specHash)
	}
	if owned, _, _, _, _ := ownership(comment, "another-owner"); owned {
		t.Fatal("ownership accepted a different owner tag")
	}

	owned, legacy, retirement, managedName, specHash = ownership(RetirementComment(cfg), cfg.Reconcile.OwnershipTag)
	if owned || legacy || !retirement || managedName != cfg.Index.Name || specHash != cfg.SpecHash() {
		t.Fatalf("retirement ownership = %t, %t, %t, %q, %q", owned, legacy, retirement, managedName, specHash)
	}
}

func matchingDesired(cfg *config.Config) IndexInfo {
	return IndexInfo{
		Name: cfg.Index.Name, IsDesired: true,
		Valid: true, Ready: true, Live: true, IsHealthy: true,
		Owned: true, ManagedName: cfg.Index.Name, SpecHash: cfg.SpecHash(),
		OpClass: cfg.OpClass(), M: cfg.Index.M, EFConstruction: cfg.Index.EFConstruction,
		Predicate: "status = 'active'::text",
	}
}

func testConfig() *config.Config {
	return &config.Config{
		Table: config.TableConfig{Schema: "public", Name: "documents", VectorCol: "embedding"},
		Index: config.IndexConfig{
			Name: "documents_embedding_idx", Type: "vector", Metric: "cosine",
			Dimensions: 128, M: 16, EFConstruction: 64,
		},
		Population: config.PopulationConfig{Filters: []config.FilterConfig{{Column: "status", Values: []string{"active"}}}},
		Reconcile:  config.ReconcileConfig{OwnershipTag: "pgvector-index-manager"},
	}
}

func containsReason(reasons []string, fragment string) bool {
	for _, reason := range reasons {
		if strings.Contains(reason, fragment) {
			return true
		}
	}
	return false
}
