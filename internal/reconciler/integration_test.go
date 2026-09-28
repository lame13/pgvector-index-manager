package reconciler

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/lame13/pgvector-index-manager/internal/catalog"
	"github.com/lame13/pgvector-index-manager/internal/config"
)

func TestApplyIntegrationRepairsDriftAndPreservesOtherPopulation(t *testing.T) {
	pool := integrationPool(t)
	ctx := context.Background()
	schema := integrationSchema(t, pool)
	cfg := integrationConfig(schema, "documents_embedding_idx", "a")

	result, err := Apply(ctx, pool, cfg, false)
	if err != nil {
		t.Fatalf("initial Apply() error = %v", err)
	}
	if len(result.Actions) != 2 {
		t.Fatalf("initial Apply() actions = %#v, want build and publish", result.Actions)
	}

	otherCfg := integrationConfig(schema, "documents_other_population_idx", "b")
	otherSQL := fmt.Sprintf(
		"CREATE INDEX %s ON %s.documents USING hnsw ((embedding::vector(3)) vector_cosine_ops) WHERE kind = 'b'",
		config.QuoteIdentifier(otherCfg.Index.Name), config.QuoteIdentifier(schema),
	)
	if _, err := pool.Exec(ctx, otherSQL); err != nil {
		t.Fatalf("creating other population index: %v", err)
	}
	commentSQL := fmt.Sprintf("COMMENT ON INDEX %s.%s IS %s",
		config.QuoteIdentifier(schema), config.QuoteIdentifier(otherCfg.Index.Name),
		config.QuoteLiteral(catalog.OwnershipComment(otherCfg, inspectedIndex(t, pool, otherCfg, otherCfg.Index.Name))))
	if _, err := pool.Exec(ctx, commentSQL); err != nil {
		t.Fatalf("commenting other population index: %v", err)
	}

	if _, err := pool.Exec(ctx, fmt.Sprintf("ALTER INDEX %s.%s SET (m = 32)",
		config.QuoteIdentifier(schema), config.QuoteIdentifier(cfg.Index.Name))); err != nil {
		t.Fatalf("introducing managed drift: %v", err)
	}
	result, err = Apply(ctx, pool, cfg, false)
	if err != nil {
		t.Fatalf("repair Apply() error = %v", err)
	}
	if len(result.Actions) != 3 {
		t.Fatalf("repair Apply() actions = %#v, want build, publish, retire", result.Actions)
	}
	status, err := catalog.Inspect(ctx, pool, cfg)
	if err != nil {
		t.Fatalf("Inspect() error = %v", err)
	}
	desired := status.Desired()
	if desired == nil || !catalog.IndexMatchesSpec(*desired, cfg) {
		t.Fatalf("published desired index did not converge: %#v", desired)
	}
	var otherExists bool
	if err := pool.QueryRow(ctx, "SELECT to_regclass($1) IS NOT NULL", schema+"."+otherCfg.Index.Name).Scan(&otherExists); err != nil {
		t.Fatal(err)
	}
	if !otherExists {
		t.Fatal("Apply() retired an index from another managed population")
	}
}

func TestApplyIntegrationBlocksUnownedSameNameIndex(t *testing.T) {
	pool := integrationPool(t)
	ctx := context.Background()
	schema := integrationSchema(t, pool)
	cfg := integrationConfig(schema, "documents_embedding_idx", "a")
	createSQL := fmt.Sprintf(
		"CREATE INDEX %s ON %s.documents USING hnsw (embedding vector_l2_ops) WITH (m = 32, ef_construction = 128)",
		config.QuoteIdentifier(cfg.Index.Name), config.QuoteIdentifier(schema),
	)
	if _, err := pool.Exec(ctx, createSQL); err != nil {
		t.Fatalf("creating unowned index: %v", err)
	}

	result, err := Apply(ctx, pool, cfg, false)
	if !errors.Is(err, ErrUnownedDesired) {
		t.Fatalf("Apply() error = %v, want ErrUnownedDesired", err)
	}
	if result == nil || !result.Failed || len(result.Actions) != 1 || result.Actions[0].Kind != ActionBlocked {
		t.Fatalf("Apply() result = %#v, want blocked failure", result)
	}
	var opclass string
	if err := pool.QueryRow(ctx, `
		SELECT opc.opcname
		FROM pg_index AS idx
		JOIN pg_class AS index_cls ON index_cls.oid = idx.indexrelid
		JOIN pg_namespace AS index_ns ON index_ns.oid = index_cls.relnamespace
		JOIN pg_opclass AS opc ON opc.oid = idx.indclass[0]
		WHERE index_ns.nspname = $1 AND index_cls.relname = $2`, schema, cfg.Index.Name).Scan(&opclass); err != nil {
		t.Fatal(err)
	}
	if opclass != "vector_l2_ops" {
		t.Fatalf("unowned index changed to opclass %s", opclass)
	}

	cfg.Reconcile.DropUnowned = true
	result, err = Apply(ctx, pool, cfg, false)
	if err != nil {
		t.Fatalf("authorized Apply() error = %v", err)
	}
	if len(result.Actions) != 3 {
		t.Fatalf("authorized Apply() actions = %#v, want build, publish, retire", result.Actions)
	}
	status, err := catalog.Inspect(ctx, pool, cfg)
	if err != nil {
		t.Fatal(err)
	}
	desired := status.Desired()
	if desired == nil || !catalog.IndexMatchesSpec(*desired, cfg) {
		t.Fatalf("authorized replacement did not converge: %#v", desired)
	}

	const interruptedRetirement = "documents_embedding_idx_retired_interrupted"
	staleSQL := fmt.Sprintf(
		"CREATE INDEX %s ON %s.documents USING hnsw (embedding vector_l2_ops)",
		config.QuoteIdentifier(interruptedRetirement), config.QuoteIdentifier(schema),
	)
	if _, err := pool.Exec(ctx, staleSQL); err != nil {
		t.Fatal(err)
	}
	retirementCommentSQL := fmt.Sprintf("COMMENT ON INDEX %s.%s IS %s",
		config.QuoteIdentifier(schema), config.QuoteIdentifier(interruptedRetirement),
		config.QuoteLiteral(catalog.RetirementComment(
			cfg, inspectedIndex(t, pool, cfg, interruptedRetirement), time.Now().Add(-time.Second), true,
		)))
	if _, err := pool.Exec(ctx, retirementCommentSQL); err != nil {
		t.Fatal(err)
	}
	result, err = Apply(ctx, pool, cfg, false)
	if err != nil {
		t.Fatalf("retirement recovery Apply() error = %v", err)
	}
	if len(result.Actions) != 1 || result.Actions[0].Kind != ActionRetire {
		t.Fatalf("retirement recovery actions = %#v, want one retirement", result.Actions)
	}
}

func TestApplyIntegrationBlocksCrossFamilyOwnership(t *testing.T) {
	pool := integrationPool(t)
	ctx := context.Background()
	schema := integrationSchema(t, pool)
	cfg := integrationConfig(schema, "documents_embedding_idx", "a")
	cfg.Reconcile.DropUnowned = true
	createSQL := fmt.Sprintf(
		"CREATE INDEX %s ON %s.documents USING hnsw ((embedding::vector(3)) vector_cosine_ops) WHERE kind = 'a'",
		config.QuoteIdentifier(cfg.Index.Name), config.QuoteIdentifier(schema),
	)
	if _, err := pool.Exec(ctx, createSQL); err != nil {
		t.Fatal(err)
	}
	foreignCfg := integrationConfig(schema, "another_family_idx", "a")
	commentSQL := fmt.Sprintf("COMMENT ON INDEX %s.%s IS %s",
		config.QuoteIdentifier(schema), config.QuoteIdentifier(cfg.Index.Name),
		config.QuoteLiteral(catalog.OwnershipComment(foreignCfg, inspectedIndex(t, pool, cfg, cfg.Index.Name))))
	if _, err := pool.Exec(ctx, commentSQL); err != nil {
		t.Fatal(err)
	}

	result, err := Apply(ctx, pool, cfg, false)
	if !errors.Is(err, ErrManagedFamilyConflict) {
		t.Fatalf("Apply() error = %v, want ErrManagedFamilyConflict", err)
	}
	if result == nil || !result.Failed || len(result.Actions) != 1 || result.Actions[0].Kind != ActionBlocked {
		t.Fatalf("Apply() result = %#v, want one blocking action", result)
	}
	if !relationExists(t, pool, schema+"."+cfg.Index.Name) {
		t.Fatal("Apply() removed an index owned by another managed family")
	}
}

func TestApplyIntegrationHonorsDurableRetirementDeadline(t *testing.T) {
	pool := integrationPool(t)
	ctx := context.Background()
	schema := integrationSchema(t, pool)
	cfg := integrationConfig(schema, "documents_embedding_idx", "a")
	if _, err := Apply(ctx, pool, cfg, false); err != nil {
		t.Fatalf("initial Apply() error = %v", err)
	}

	const staleName = "documents_embedding_idx_retired_restart"
	createSQL := fmt.Sprintf(
		"CREATE INDEX %s ON %s.documents USING hnsw ((embedding::vector(3)) vector_cosine_ops) WHERE kind = 'a'",
		config.QuoteIdentifier(staleName), config.QuoteIdentifier(schema),
	)
	if _, err := pool.Exec(ctx, createSQL); err != nil {
		t.Fatal(err)
	}
	future := time.Now().Add(5 * time.Minute).UTC()
	stale := inspectedIndex(t, pool, cfg, staleName)
	commentSQL := fmt.Sprintf("COMMENT ON INDEX %s.%s IS %s",
		config.QuoteIdentifier(schema), config.QuoteIdentifier(staleName),
		config.QuoteLiteral(catalog.RetirementComment(cfg, stale, future, false)))
	if _, err := pool.Exec(ctx, commentSQL); err != nil {
		t.Fatal(err)
	}

	waitCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	result, err := Apply(waitCtx, pool, cfg, false)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Apply() error = %v, want deadline while grace remains", err)
	}
	if result == nil || !result.Failed {
		t.Fatalf("Apply() result = %#v, want interrupted grace wait", result)
	}
	if !relationExists(t, pool, schema+"."+staleName) {
		t.Fatal("restart recovery bypassed the durable grace deadline")
	}

	stale = inspectedIndex(t, pool, cfg, staleName)
	commentSQL = fmt.Sprintf("COMMENT ON INDEX %s.%s IS %s",
		config.QuoteIdentifier(schema), config.QuoteIdentifier(staleName),
		config.QuoteLiteral(catalog.RetirementComment(cfg, stale, time.Now().Add(-time.Second), false)))
	if _, err := pool.Exec(ctx, commentSQL); err != nil {
		t.Fatal(err)
	}
	result, err = Apply(ctx, pool, cfg, false)
	if err != nil {
		t.Fatalf("eligible retirement Apply() error = %v", err)
	}
	if len(result.Actions) != 1 || result.Actions[0].Kind != ActionRetire {
		t.Fatalf("eligible retirement actions = %#v, want one retirement", result.Actions)
	}
}

func TestApplyIntegrationDetectsStructuralReplacementUnderCopiedComment(t *testing.T) {
	pool := integrationPool(t)
	ctx := context.Background()
	schema := integrationSchema(t, pool)
	cfg := integrationConfig(schema, "documents_embedding_idx", "a")
	if _, err := Apply(ctx, pool, cfg, false); err != nil {
		t.Fatalf("initial Apply() error = %v", err)
	}
	original := inspectedIndex(t, pool, cfg, cfg.Index.Name)
	if _, err := pool.Exec(ctx, fmt.Sprintf("DROP INDEX %s.%s",
		config.QuoteIdentifier(schema), config.QuoteIdentifier(cfg.Index.Name))); err != nil {
		t.Fatal(err)
	}
	createSQL := fmt.Sprintf(
		"CREATE INDEX %s ON %s.documents USING hnsw ((embedding::vector(3)) vector_cosine_ops) WHERE kind = 'b'",
		config.QuoteIdentifier(cfg.Index.Name), config.QuoteIdentifier(schema),
	)
	if _, err := pool.Exec(ctx, createSQL); err != nil {
		t.Fatal(err)
	}
	commentSQL := fmt.Sprintf("COMMENT ON INDEX %s.%s IS %s",
		config.QuoteIdentifier(schema), config.QuoteIdentifier(cfg.Index.Name), config.QuoteLiteral(original.Comment))
	if _, err := pool.Exec(ctx, commentSQL); err != nil {
		t.Fatal(err)
	}

	status, err := catalog.Inspect(ctx, pool, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if !status.Drift || catalog.IndexMatchesSpec(*status.Desired(), cfg) {
		t.Fatalf("copied ownership claim hid structural drift: %#v", status.Desired())
	}
	if !containsDriftReason(status.DriftReasons, "catalog structure differs") {
		t.Fatalf("drift reasons = %v, want structural mismatch", status.DriftReasons)
	}

	result, err := Apply(ctx, pool, cfg, false)
	if err != nil {
		t.Fatalf("repair Apply() error = %v", err)
	}
	if len(result.Actions) != 3 {
		t.Fatalf("repair actions = %#v, want build, publish, retire", result.Actions)
	}
}

func TestApplyIntegrationBlocksNamespaceConflictBeforeBuild(t *testing.T) {
	pool := integrationPool(t)
	ctx := context.Background()
	schema := integrationSchema(t, pool)
	cfg := integrationConfig(schema, "documents_embedding_idx", "a")
	if _, err := pool.Exec(ctx, fmt.Sprintf("CREATE TABLE %s.%s (id integer)",
		config.QuoteIdentifier(schema), config.QuoteIdentifier(cfg.Index.Name))); err != nil {
		t.Fatal(err)
	}

	status, err := catalog.Inspect(ctx, pool, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if status.NamespaceConflict != "a table" {
		t.Fatalf("NamespaceConflict = %q, want a table", status.NamespaceConflict)
	}
	result, err := Apply(ctx, pool, cfg, false)
	if !errors.Is(err, ErrNamespaceConflict) {
		t.Fatalf("Apply() error = %v, want ErrNamespaceConflict", err)
	}
	if result == nil || len(result.Actions) != 1 || result.Actions[0].Kind != ActionBlocked {
		t.Fatalf("Apply() result = %#v, want preflight block", result)
	}
	status, inspectErr := catalog.Inspect(ctx, pool, cfg)
	if inspectErr != nil {
		t.Fatal(inspectErr)
	}
	if len(status.Indexes) != 0 {
		t.Fatalf("namespace preflight allowed build artifacts: %#v", status.Indexes)
	}
}

func TestApplyIntegrationReusesInterruptedReplacement(t *testing.T) {
	pool := integrationPool(t)
	ctx := context.Background()
	schema := integrationSchema(t, pool)
	cfg := integrationConfig(schema, "documents_embedding_idx", "a")
	if _, err := Apply(ctx, pool, cfg, false); err != nil {
		t.Fatalf("initial Apply() error = %v", err)
	}
	const interruptedName = "documents_embedding_idx_build_interrupted"
	renameSQL := fmt.Sprintf("ALTER INDEX %s.%s RENAME TO %s",
		config.QuoteIdentifier(schema), config.QuoteIdentifier(cfg.Index.Name), config.QuoteIdentifier(interruptedName))
	if _, err := pool.Exec(ctx, renameSQL); err != nil {
		t.Fatal(err)
	}

	result, err := Apply(ctx, pool, cfg, false)
	if err != nil {
		t.Fatalf("recovery Apply() error = %v", err)
	}
	if len(result.Actions) != 1 || result.Actions[0].Kind != ActionPublish {
		t.Fatalf("recovery Apply() actions = %#v, want publish-only reuse", result.Actions)
	}
	status, err := catalog.Inspect(ctx, pool, cfg)
	if err != nil {
		t.Fatal(err)
	}
	desired := status.Desired()
	if desired == nil || !catalog.IndexMatchesSpec(*desired, cfg) {
		t.Fatalf("reused replacement did not converge: %#v", desired)
	}
}

func integrationPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv("PGVECTOR_INDEX_MANAGER_TEST_DSN")
	if dsn == "" {
		t.Skip("PGVECTOR_INDEX_MANAGER_TEST_DSN is not set")
	}
	pool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return pool
}

func integrationSchema(t *testing.T, pool *pgxpool.Pool) string {
	t.Helper()
	ctx := context.Background()
	schema := fmt.Sprintf("index_manager_it_%d", time.Now().UnixNano())
	if _, err := pool.Exec(ctx, "CREATE EXTENSION IF NOT EXISTS vector"); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, fmt.Sprintf("CREATE SCHEMA %s", config.QuoteIdentifier(schema))); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), fmt.Sprintf("DROP SCHEMA %s CASCADE", config.QuoteIdentifier(schema)))
	})
	createTableSQL := fmt.Sprintf(`
		CREATE TABLE %s.documents (
			id bigserial PRIMARY KEY,
			embedding vector(3) NOT NULL,
			kind text NOT NULL
		)`, config.QuoteIdentifier(schema))
	if _, err := pool.Exec(ctx, createTableSQL); err != nil {
		t.Fatal(err)
	}
	insertSQL := fmt.Sprintf(`INSERT INTO %s.documents (embedding, kind) VALUES
		('[1,0,0]', 'a'), ('[0,1,0]', 'a'), ('[0,0,1]', 'b')`, config.QuoteIdentifier(schema))
	if _, err := pool.Exec(ctx, insertSQL); err != nil {
		t.Fatal(err)
	}
	return schema
}

func integrationConfig(schema, indexName, kind string) *config.Config {
	return &config.Config{
		Connection: config.ConnectionConfig{StatementTimeout: "30s", LockTimeout: "5s"},
		Table:      config.TableConfig{Schema: schema, Name: "documents", VectorCol: "embedding"},
		Index: config.IndexConfig{
			Name: indexName, Type: "vector", Metric: "cosine",
			Dimensions: 3, M: 16, EFConstruction: 64,
		},
		Population: config.PopulationConfig{Filters: []config.FilterConfig{{Column: "kind", Values: []string{kind}}}},
		Reconcile: config.ReconcileConfig{
			OwnershipTag: "pgvector-index-manager-test", BuildTimeout: "30s", GracePeriod: "0s",
		},
	}
}

func inspectedIndex(t *testing.T, pool *pgxpool.Pool, cfg *config.Config, name string) catalog.IndexInfo {
	t.Helper()
	status, err := catalog.Inspect(context.Background(), pool, cfg)
	if err != nil {
		t.Fatal(err)
	}
	for _, idx := range status.Indexes {
		if idx.Name == name {
			return idx
		}
	}
	t.Fatalf("index %s not found in %#v", name, status.Indexes)
	return catalog.IndexInfo{}
}

func relationExists(t *testing.T, pool *pgxpool.Pool, qualifiedName string) bool {
	t.Helper()
	var exists bool
	if err := pool.QueryRow(context.Background(), "SELECT to_regclass($1) IS NOT NULL", qualifiedName).Scan(&exists); err != nil {
		t.Fatal(err)
	}
	return exists
}

func containsDriftReason(reasons []string, fragment string) bool {
	for _, reason := range reasons {
		if strings.Contains(reason, fragment) {
			return true
		}
	}
	return false
}

func TestApplyAllIntegrationReconcilesIndependentTargets(t *testing.T) {
	pool := integrationPool(t)
	ctx := context.Background()
	schema := integrationSchema(t, pool)

	active := integrationConfig(schema, "documents_active_idx", "a")
	active.Name = "active"
	other := integrationConfig(schema, "documents_other_idx", "b")
	other.Name = "other"
	// Managed index names isolate families even when they share an ownership tag.
	targets := []Target{
		{Name: active.Name, Config: active, Pool: pool},
		{Name: other.Name, Config: other, Pool: pool},
	}

	multi := ApplyAll(ctx, targets, false)
	if multi.Failed() {
		t.Fatalf("ApplyAll() errors = %v", multi.Errors())
	}
	if len(multi.Targets) != 2 || !multi.Changed() {
		t.Fatalf("ApplyAll() = %#v, want two changed targets", multi.Targets)
	}
	for _, cfg := range []*config.Config{active, other} {
		status, err := catalog.Inspect(ctx, pool, cfg)
		if err != nil {
			t.Fatalf("Inspect(%s) error = %v", cfg.Index.Name, err)
		}
		desired := status.Desired()
		if desired == nil || !catalog.IndexMatchesSpec(*desired, cfg) {
			t.Fatalf("target %s did not converge: %#v", cfg.Name, desired)
		}
	}
	// Repairing one family must also preserve the other family sharing its tag.
	if _, err := pool.Exec(ctx, fmt.Sprintf("ALTER INDEX %s.%s SET (m = 32)",
		config.QuoteIdentifier(schema), config.QuoteIdentifier(active.Index.Name))); err != nil {
		t.Fatal(err)
	}
	multi = ApplyAll(ctx, targets, false)
	if multi.Failed() || len(multi.Targets[0].Result.Actions) != 3 || len(multi.Targets[1].Result.Actions) != 0 {
		t.Fatalf("repair ApplyAll() = %#v, want only the first family repaired", multi.Targets)
	}
	// A further pass converges with nothing to do and retires neither family.
	multi = ApplyAll(ctx, targets, false)
	if multi.Failed() || multi.Changed() {
		t.Fatalf("converged ApplyAll() = %#v, want a no-op", multi.Targets)
	}
	for _, name := range []string{active.Index.Name, other.Index.Name} {
		if !relationExists(t, pool, schema+"."+name) {
			t.Fatalf("multi-target pass retired %s", name)
		}
	}
}

func TestApplyAllIntegrationIsolatesTargetFailures(t *testing.T) {
	pool := integrationPool(t)
	ctx := context.Background()
	schema := integrationSchema(t, pool)

	healthy := integrationConfig(schema, "documents_active_idx", "a")
	healthy.Name = "healthy"
	broken := integrationConfig(schema, "documents_other_idx", "b")
	broken.Name = "broken"
	// An unrelated table on the broken target's index name fails namespace
	// preflight, which must not cost the healthy target its work.
	if _, err := pool.Exec(ctx, fmt.Sprintf("CREATE TABLE %s.%s (id integer)",
		config.QuoteIdentifier(schema), config.QuoteIdentifier(broken.Index.Name))); err != nil {
		t.Fatal(err)
	}

	multi := ApplyAll(ctx, []Target{
		{Name: broken.Name, Config: broken, Pool: pool},
		{Name: healthy.Name, Config: healthy, Pool: pool},
	}, false)
	if len(multi.Targets) != 2 {
		t.Fatalf("ApplyAll() targets = %#v, want two entries", multi.Targets)
	}
	if !errors.Is(multi.Targets[0].Err, ErrNamespaceConflict) {
		t.Fatalf("broken target error = %v, want ErrNamespaceConflict", multi.Targets[0].Err)
	}
	if !multi.Failed() {
		t.Fatal("Failed() = false, want true when one target is blocked")
	}
	healthyResult := multi.Targets[1].Result
	if multi.Targets[1].Err != nil || healthyResult == nil || len(healthyResult.Actions) != 2 {
		t.Fatalf("healthy target after a blocked peer = %#v/%v, want build and publish", healthyResult, multi.Targets[1].Err)
	}

	status, err := catalog.Inspect(ctx, pool, healthy)
	if err != nil {
		t.Fatal(err)
	}
	desired := status.Desired()
	if desired == nil || !catalog.IndexMatchesSpec(*desired, healthy) {
		t.Fatalf("healthy target did not converge alongside a blocked peer: %#v", desired)
	}
}
