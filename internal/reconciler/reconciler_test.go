package reconciler

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/lame13/pgvector-index-manager/internal/catalog"
	"github.com/lame13/pgvector-index-manager/internal/config"
)

func TestAdvisoryLockKey(t *testing.T) {
	key1 := advisoryLockKey("public", "documents")
	key2 := advisoryLockKey("public", "documents")
	key3 := advisoryLockKey("other", "documents")
	if key1 != key2 {
		t.Fatalf("advisoryLockKey() should be deterministic: %d != %d", key1, key2)
	}
	if key1 == key3 {
		t.Fatalf("advisoryLockKey() should differ for different schemas: %d == %d", key1, key3)
	}
}

func TestBuildPlanBlocksUnownedDesiredIndex(t *testing.T) {
	cfg := testConfig()
	status := &catalog.Status{
		Table: "public.documents", Drift: true,
		Indexes: []catalog.IndexInfo{{
			Name: cfg.Index.Name, IsDesired: true, IsHealthy: true,
			OpClass: "vector_l2_ops", M: 32, EFConstruction: 128,
		}},
	}
	plan := buildPlan(status, cfg)
	if !plan.Blocked || len(plan.Actions) != 1 || plan.Actions[0].Kind != ActionBlocked {
		t.Fatalf("buildPlan() = %#v, want one blocking action", plan)
	}
}

func TestBuildPlanRepairsOwnedSpecDrift(t *testing.T) {
	cfg := testConfig()
	status := &catalog.Status{
		Table: "public.documents", Drift: true,
		DriftReasons: []string{"opclass mismatch"},
		Indexes: []catalog.IndexInfo{{
			Name: cfg.Index.Name, IsDesired: true, IsHealthy: true, Owned: true,
			ManagedName: cfg.Index.Name, SpecHash: "old-spec",
			OpClass: "vector_l2_ops", M: 32, EFConstruction: 128,
		}},
	}
	plan := buildPlan(status, cfg)
	if plan.Blocked || len(plan.Actions) != 2 || plan.Actions[0].Kind != ActionBuild || plan.Actions[1].Kind != ActionPublish {
		t.Fatalf("buildPlan() = %#v, want build and publish", plan)
	}
}

func TestBuildPlanPreservesOtherManagedFamilies(t *testing.T) {
	cfg := testConfig()
	desired := matchingIndex(cfg, cfg.Index.Name)
	desired.IsDesired = true
	status := &catalog.Status{
		Table: "public.documents", Drift: true,
		Indexes: []catalog.IndexInfo{
			desired,
			{
				Name: "documents_other_population_idx", IsHealthy: true, Owned: true,
				ManagedName: "documents_other_population_idx", SpecHash: "other",
				OpClass: cfg.OpClass(), M: 16, EFConstruction: 64,
			},
		},
		DriftReasons: []string{"synthetic drift"},
	}
	plan := buildPlan(status, cfg)
	if len(plan.Actions) != 0 {
		t.Fatalf("buildPlan() planned changes to an unrelated managed family: %#v", plan.Actions)
	}
}

func TestBuildPlanReusesVerifiedReplacement(t *testing.T) {
	cfg := testConfig()
	replacement := matchingIndex(cfg, "documents_embedding_idx_build_123")
	status := &catalog.Status{
		Table: "public.documents", Drift: true,
		DriftReasons: []string{"desired missing"},
		Indexes:      []catalog.IndexInfo{replacement},
	}
	plan := buildPlan(status, cfg)
	if len(plan.Actions) != 1 || plan.Actions[0].Kind != ActionPublish {
		t.Fatalf("buildPlan() = %#v, want reuse/publish only", plan)
	}
}

func TestBuildPlanBlocksAnotherManagedFamilyEvenWhenDropUnownedIsEnabled(t *testing.T) {
	cfg := testConfig()
	cfg.Reconcile.DropUnowned = true
	foreign := matchingIndex(cfg, cfg.Index.Name)
	foreign.IsDesired = true
	foreign.ManagedName = "another_family_idx"
	status := &catalog.Status{Table: "public.documents", Drift: true, Indexes: []catalog.IndexInfo{foreign}}

	plan := buildPlan(status, cfg)
	if !plan.Blocked || len(plan.Actions) != 1 || plan.Actions[0].Kind != ActionBlocked {
		t.Fatalf("buildPlan() = %#v, want managed-family block", plan)
	}
	if _, err := blockingConflict(status, cfg); !errors.Is(err, ErrManagedFamilyConflict) {
		t.Fatalf("blockingConflict() error = %v, want ErrManagedFamilyConflict", err)
	}
}

func TestBuildPlanBlocksNamespaceConflict(t *testing.T) {
	cfg := testConfig()
	status := &catalog.Status{
		Table: "public.documents", Drift: true, NamespaceConflict: "a table",
		DriftReasons: []string{"desired name is occupied"},
	}
	plan := buildPlan(status, cfg)
	if !plan.Blocked || len(plan.Actions) != 1 || plan.Actions[0].Kind != ActionBlocked {
		t.Fatalf("buildPlan() = %#v, want namespace block", plan)
	}
	if _, err := blockingConflict(status, cfg); !errors.Is(err, ErrNamespaceConflict) {
		t.Fatalf("blockingConflict() error = %v, want ErrNamespaceConflict", err)
	}
}

func TestTemporaryIndexNameFitsPostgreSQLLimit(t *testing.T) {
	name, err := temporaryIndexName(strings.Repeat("é", 31), "retired")
	if err != nil {
		t.Fatal(err)
	}
	if len([]byte(name)) > 63 {
		t.Fatalf("temporaryIndexName() returned %d bytes: %q", len([]byte(name)), name)
	}
	if !strings.Contains(name, "_retired_") {
		t.Fatalf("temporaryIndexName() = %q, missing phase", name)
	}
}

func TestActionResultDuration(t *testing.T) {
	result := ActionResult{Description: "Create index", Duration: time.Second}
	if result.Duration.Seconds() != 1.0 {
		t.Fatalf("ActionResult.Duration = %v, want 1s", result.Duration)
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

func matchingIndex(cfg *config.Config, name string) catalog.IndexInfo {
	return catalog.IndexInfo{
		Name: name, IsHealthy: true, Valid: true, Ready: true, Live: true,
		Owned: true, Managed: true, ManagedName: cfg.Index.Name, SpecHash: cfg.SpecHash(),
		StructureHash: "verified-structure", ActualStructureHash: "verified-structure",
		AccessMethod: "hnsw", OpClass: cfg.OpClass(), M: cfg.Index.M, EFConstruction: cfg.Index.EFConstruction,
		TotalColumns: 1, KeyColumns: 1, DirectColumn: cfg.Table.VectorCol, IndexType: cfg.VectorCast(),
		ReferencedColumns: []string{cfg.Table.VectorCol, "status"}, Predicate: "status = 'active'::text",
	}
}
