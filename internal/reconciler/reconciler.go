package reconciler

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"hash/fnv"
	"log"
	"strings"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/lame13/pgvector-index-manager/internal/catalog"
	"github.com/lame13/pgvector-index-manager/internal/config"
	"github.com/lame13/pgvector-index-manager/internal/pg"
)

var (
	// ErrUnownedDesired is returned when applying drift would require replacing
	// a same-name index that the manager cannot prove it owns.
	ErrUnownedDesired = errors.New("desired index is unowned")

	// ErrManagedFamilyConflict is returned when the desired name is occupied by
	// an index carrying structured metadata for another owner or managed family.
	ErrManagedFamilyConflict = errors.New("desired index belongs to another managed family")

	// ErrNamespaceConflict is returned when a non-target relation already owns
	// the desired schema/name.
	ErrNamespaceConflict = errors.New("desired index name has a namespace conflict")
)

type ActionKind string

const (
	ActionBuild   ActionKind = "build"
	ActionPublish ActionKind = "publish"
	ActionRetire  ActionKind = "retire"
	ActionBlocked ActionKind = "blocked"
)

// PlanResult describes what changes would be made.
type PlanResult struct {
	Table        string
	Actions      []Action
	Blocked      bool
	DriftReasons []string
}

// ApplyResult describes what changes were made.
type ApplyResult struct {
	Table     string
	DryRun    bool
	Actions   []ActionResult
	Failed    bool
	StartTime time.Time
	EndTime   time.Time
}

// Action describes a single planned change.
type Action struct {
	Kind        ActionKind
	Description string
	Reason      string
}

// ActionResult describes a single executed change.
type ActionResult struct {
	Kind        ActionKind
	IndexName   string
	Description string
	Error       string
	Duration    time.Duration
}

// Plan inspects the current state and produces a plan of index changes.
func Plan(ctx context.Context, pool *pgxpool.Pool, cfg *config.Config) (*PlanResult, error) {
	status, err := catalog.Inspect(ctx, pool, cfg)
	if err != nil {
		return nil, err
	}
	return buildPlan(status, cfg), nil
}

func buildPlan(status *catalog.Status, cfg *config.Config) *PlanResult {
	result := &PlanResult{
		Table: status.Table, DriftReasons: append([]string(nil), status.DriftReasons...),
	}
	if !status.Drift {
		return result
	}
	if action, _ := blockingConflict(status, cfg); action != nil {
		result.Blocked = true
		result.Actions = append(result.Actions, *action)
		return result
	}

	desired := status.Desired()
	if desired == nil || !catalog.IndexMatchesSpec(*desired, cfg) {
		if replacement := status.MatchingReplacement(cfg); replacement != nil {
			result.Actions = append(result.Actions, Action{
				Kind: ActionPublish, Description: fmt.Sprintf("Publish verified replacement %s as %s", replacement.Name, cfg.Index.Name),
				Reason: "a healthy run-scoped replacement already matches the configured spec",
			})
		} else {
			result.Actions = append(result.Actions, Action{
				Kind: ActionBuild, Description: fmt.Sprintf("Build and verify a replacement for %s", cfg.Index.Name),
				Reason: strings.Join(status.DriftReasons, "; "),
			})
			result.Actions = append(result.Actions, Action{
				Kind: ActionPublish, Description: fmt.Sprintf("Atomically publish the replacement as %s", cfg.Index.Name),
				Reason: "replacement must be verified before the old index is retired",
			})
		}
	}

	for _, idx := range status.Indexes {
		if idx.IsDesired || !sameManagedFamily(idx, cfg) {
			continue
		}
		if replacement := status.MatchingReplacement(cfg); replacement != nil && idx.Name == replacement.Name &&
			(desired == nil || !catalog.IndexMatchesSpec(*desired, cfg)) {
			continue
		}
		result.Actions = append(result.Actions, Action{
			Kind: ActionRetire, Description: fmt.Sprintf("Retire stale managed index %s", idx.Name),
			Reason: retirementPlanReason(idx, cfg),
		})
	}
	return result
}

// Apply inspects the current state and executes the reconciliation.
func Apply(ctx context.Context, pool *pgxpool.Pool, cfg *config.Config, dryRun bool) (result *ApplyResult, returnedErr error) {
	start := time.Now()
	status, err := catalog.Inspect(ctx, pool, cfg)
	if err != nil {
		return nil, err
	}
	result = &ApplyResult{Table: status.Table, DryRun: dryRun, StartTime: start}
	defer func() { result.EndTime = time.Now() }()

	if !status.Drift {
		return result, nil
	}
	if dryRun {
		plan := buildPlan(status, cfg)
		result.Failed = plan.Blocked
		for _, action := range plan.Actions {
			item := ActionResult{Kind: action.Kind, Description: action.Description + " (dry run)"}
			if action.Kind == ActionBlocked {
				item.Error = action.Reason
			}
			result.Actions = append(result.Actions, item)
		}
		if plan.Blocked {
			_, conflictErr := blockingConflict(status, cfg)
			return result, conflictErr
		}
		return result, nil
	}

	lockKey := advisoryLockKey(cfg.Table.Schema, cfg.Table.Name)
	lockConn, err := pg.AcquireAdvisoryLock(ctx, pool, lockKey)
	if err != nil {
		result.Failed = true
		return result, err
	}
	defer func() {
		if err := pg.ReleaseAdvisoryLock(lockConn, lockKey); err != nil && returnedErr == nil {
			result.Failed = true
			returnedErr = err
		}
	}()

	status, err = catalog.Inspect(ctx, pool, cfg)
	if err != nil {
		result.Failed = true
		return result, err
	}
	if !status.Drift {
		return result, nil
	}
	if action, conflictErr := blockingConflict(status, cfg); action != nil {
		appendFailure(result, action.Kind, cfg.Index.Name, action.Description, conflictErr)
		return result, conflictErr
	}

	desired := status.Desired()
	if desired == nil || !catalog.IndexMatchesSpec(*desired, cfg) {
		replacement := status.MatchingReplacement(cfg)
		if replacement == nil {
			built, err := buildReplacement(ctx, lockConn, pool, cfg, result)
			if err != nil {
				return result, err
			}
			replacement = built
		}

		if err := publishReplacement(ctx, lockConn, cfg, *replacement, desired, result); err != nil {
			return result, err
		}

		status, err = catalog.Inspect(ctx, pool, cfg)
		if err != nil {
			result.Failed = true
			return result, fmt.Errorf("verifying published index: %w", err)
		}
		published := status.Desired()
		if published == nil || !catalog.IndexMatchesSpec(*published, cfg) {
			err := fmt.Errorf("published index %s did not verify against the configured spec", cfg.Index.Name)
			appendFailure(result, ActionPublish, cfg.Index.Name, fmt.Sprintf("Verify published index %s", cfg.Index.Name), err)
			return result, err
		}

	}

	// Re-inspect only after the desired name has been verified. Retire only
	// indexes whose structured metadata scopes them to this exact managed name;
	// other populations and legacy/unowned indexes are preserved.
	status, err = catalog.Inspect(ctx, pool, cfg)
	if err != nil {
		result.Failed = true
		return result, err
	}
	for _, idx := range status.Indexes {
		if idx.IsDesired || !sameManagedFamily(idx, cfg) {
			continue
		}
		scheduled, err := ensureRetirementScheduled(ctx, lockConn, idx, cfg)
		if err != nil {
			result.Failed = true
			return result, err
		}
		if err := waitUntil(ctx, lockConn, scheduled.RetireAfter); err != nil {
			result.Failed = true
			return result, err
		}
		if err := retireIndex(ctx, lockConn, scheduled, cfg, result); err != nil {
			return result, err
		}
	}
	return result, nil
}

// ApplyContinuous reconciles one target immediately and then at each configured
// interval. Cancellation is a clean shutdown rather than a runtime failure.
// Continuous runs over several targets go through ApplyAllContinuous.
func ApplyContinuous(ctx context.Context, pool *pgxpool.Pool, cfg *config.Config, onResult func(*ApplyResult) error) error {
	interval, err := ContinuousInterval(cfg)
	if err != nil {
		return err
	}
	targets := []Target{{Name: cfg.Name, Config: cfg, Pool: pool}}
	return ApplyAllContinuous(ctx, targets, interval, func(_ Target, result *ApplyResult, _ error) error {
		if onResult == nil {
			return nil
		}
		return onResult(result)
	})
}

func buildReplacement(ctx context.Context, conn *pgxpool.Conn, pool *pgxpool.Pool, cfg *config.Config, result *ApplyResult) (*catalog.IndexInfo, error) {
	started := time.Now()
	name, err := temporaryIndexName(cfg.Index.Name, "build")
	if err != nil {
		return nil, err
	}
	buildTimeout, err := time.ParseDuration(cfg.Reconcile.BuildTimeout)
	if err != nil {
		return nil, fmt.Errorf("parsing build timeout: %w", err)
	}
	buildCtx, cancel := context.WithTimeout(ctx, buildTimeout)
	defer cancel()
	if err := pg.ConfigureSession(buildCtx, conn, cfg.Reconcile.BuildTimeout, cfg.Connection.LockTimeout); err != nil {
		return nil, err
	}

	createSQL := fmt.Sprintf(
		`CREATE INDEX CONCURRENTLY %s ON %s USING hnsw (%s %s) WITH (m = %d, ef_construction = %d)`,
		config.QuoteIdentifier(name), cfg.TableSQL(), cfg.IndexExpressionSQL(), cfg.OpClass(), cfg.Index.M, cfg.Index.EFConstruction,
	)
	if predicate := cfg.PopulationPredicateSQL(); predicate != "" {
		createSQL += " WHERE " + predicate
	}
	log.Printf("Building replacement index %s...", name)
	if _, err := conn.Exec(buildCtx, createSQL); err != nil {
		appendFailure(result, ActionBuild, name, fmt.Sprintf("Build replacement index %s", name), err)
		if cleanupErr := cleanupAttemptedIndex(conn, cfg, name); cleanupErr != nil {
			appendFailure(result, ActionRetire, name, fmt.Sprintf("Clean up failed replacement index %s", name), cleanupErr)
		}
		return nil, fmt.Errorf("creating replacement index: %w", err)
	}

	if err := pg.ConfigureSession(ctx, conn, cfg.Connection.StatementTimeout, cfg.Connection.LockTimeout); err != nil {
		appendFailure(result, ActionBuild, name, fmt.Sprintf("Configure verification for index %s", name), err)
		if cleanupErr := cleanupAttemptedIndex(conn, cfg, name); cleanupErr != nil {
			appendFailure(result, ActionRetire, name, fmt.Sprintf("Clean up unverified replacement index %s", name), cleanupErr)
		}
		return nil, err
	}
	status, err := catalog.Inspect(ctx, pool, cfg)
	if err != nil {
		appendFailure(result, ActionBuild, name, fmt.Sprintf("Inspect replacement index %s", name), err)
		if cleanupErr := cleanupAttemptedIndex(conn, cfg, name); cleanupErr != nil {
			appendFailure(result, ActionRetire, name, fmt.Sprintf("Clean up unverified replacement index %s", name), cleanupErr)
		}
		return nil, err
	}
	candidate := findIndex(status.Indexes, name)
	if candidate == nil || !candidate.IsHealthy || !catalog.IndexStructureMatchesSpec(*candidate, cfg) {
		err := fmt.Errorf("replacement index did not pass catalog verification")
		appendFailure(result, ActionBuild, name, fmt.Sprintf("Verify replacement index %s", name), err)
		if cleanupErr := cleanupAttemptedIndex(conn, cfg, name); cleanupErr != nil {
			appendFailure(result, ActionRetire, name, fmt.Sprintf("Clean up failed replacement index %s", name), cleanupErr)
		}
		return nil, err
	}

	commentSQL := fmt.Sprintf("COMMENT ON INDEX %s IS %s",
		qualifiedIndex(cfg.Table.Schema, name), config.QuoteLiteral(catalog.OwnershipComment(cfg, *candidate)))
	if _, err := conn.Exec(ctx, commentSQL); err != nil {
		appendFailure(result, ActionBuild, name, fmt.Sprintf("Record ownership for index %s", name), err)
		if cleanupErr := cleanupAttemptedIndex(conn, cfg, name); cleanupErr != nil {
			appendFailure(result, ActionRetire, name, fmt.Sprintf("Clean up untagged replacement index %s", name), cleanupErr)
		}
		return nil, fmt.Errorf("recording replacement ownership: %w", err)
	}

	status, err = catalog.Inspect(ctx, pool, cfg)
	if err != nil {
		appendFailure(result, ActionBuild, name, fmt.Sprintf("Verify ownership for index %s", name), err)
		return nil, err
	}
	candidate = findIndex(status.Indexes, name)
	if candidate == nil || !catalog.IndexMatchesSpec(*candidate, cfg) {
		err := fmt.Errorf("replacement ownership/spec verification failed")
		appendFailure(result, ActionBuild, name, fmt.Sprintf("Verify replacement index %s", name), err)
		return nil, err
	}
	result.Actions = append(result.Actions, ActionResult{
		Kind: ActionBuild, IndexName: name,
		Description: fmt.Sprintf("Build and verify replacement index %s", name), Duration: time.Since(started),
	})
	return candidate, nil
}

func publishReplacement(ctx context.Context, conn *pgxpool.Conn, cfg *config.Config, replacement catalog.IndexInfo, current *catalog.IndexInfo, result *ApplyResult) error {
	started := time.Now()
	if err := pg.ConfigureSession(ctx, conn, cfg.Connection.StatementTimeout, cfg.Connection.LockTimeout); err != nil {
		return err
	}
	gracePeriod, err := configuredGracePeriod(cfg)
	if err != nil {
		return err
	}
	tx, err := conn.Begin(ctx)
	if err != nil {
		return fmt.Errorf("beginning atomic index swap: %w", err)
	}
	defer tx.Rollback(ctx)

	if current != nil {
		retirementName, err := temporaryIndexName(cfg.Index.Name, "retired")
		if err != nil {
			return err
		}
		renameOldSQL := fmt.Sprintf("ALTER INDEX %s RENAME TO %s",
			qualifiedIndex(current.Schema, current.Name), config.QuoteIdentifier(retirementName))
		if _, err := tx.Exec(ctx, renameOldSQL); err != nil {
			appendFailure(result, ActionPublish, cfg.Index.Name, fmt.Sprintf("Move current index %s aside", current.Name), err)
			return fmt.Errorf("renaming current index: %w", err)
		}
		retired := *current
		retired.Name = retirementName
		retired.IsDesired = false
		var retireAfter time.Time
		if err := tx.QueryRow(ctx,
			"SELECT clock_timestamp() + ($1 * interval '1 second')", gracePeriod.Seconds(),
		).Scan(&retireAfter); err != nil {
			return fmt.Errorf("calculating retirement deadline: %w", err)
		}
		commentSQL := fmt.Sprintf("COMMENT ON INDEX %s IS %s",
			qualifiedIndex(current.Schema, retirementName),
			config.QuoteLiteral(catalog.RetirementComment(cfg, retired, retireAfter, !current.Owned)))
		if _, err := tx.Exec(ctx, commentSQL); err != nil {
			appendFailure(result, ActionPublish, current.Name, fmt.Sprintf("Record durable retirement deadline for %s", current.Name), err)
			return fmt.Errorf("recording durable retirement deadline: %w", err)
		}
	}

	renameReplacementSQL := fmt.Sprintf("ALTER INDEX %s RENAME TO %s",
		qualifiedIndex(replacement.Schema, replacement.Name), config.QuoteIdentifier(cfg.Index.Name))
	if _, err := tx.Exec(ctx, renameReplacementSQL); err != nil {
		appendFailure(result, ActionPublish, cfg.Index.Name, fmt.Sprintf("Publish replacement as %s", cfg.Index.Name), err)
		return fmt.Errorf("publishing replacement index: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		appendFailure(result, ActionPublish, cfg.Index.Name, fmt.Sprintf("Commit publication of %s", cfg.Index.Name), err)
		return fmt.Errorf("committing atomic index swap: %w", err)
	}
	result.Actions = append(result.Actions, ActionResult{
		Kind: ActionPublish, IndexName: cfg.Index.Name,
		Description: fmt.Sprintf("Atomically publish replacement as %s", cfg.Index.Name), Duration: time.Since(started),
	})
	return nil
}

func retireIndex(ctx context.Context, conn *pgxpool.Conn, idx catalog.IndexInfo, cfg *config.Config, result *ApplyResult) error {
	started := time.Now()
	if err := pg.ConfigureSession(ctx, conn, cfg.Connection.StatementTimeout, cfg.Connection.LockTimeout); err != nil {
		return err
	}
	dropSQL := fmt.Sprintf("DROP INDEX CONCURRENTLY IF EXISTS %s", qualifiedIndex(idx.Schema, idx.Name))
	log.Printf("Retiring index %s...", idx.Name)
	if _, err := conn.Exec(ctx, dropSQL); err != nil {
		appendFailure(result, ActionRetire, idx.Name, fmt.Sprintf("Retire index %s", idx.Name), err)
		return fmt.Errorf("retiring index %s: %w", idx.Name, err)
	}
	result.Actions = append(result.Actions, ActionResult{
		Kind: ActionRetire, IndexName: idx.Name,
		Description: fmt.Sprintf("Retire index %s concurrently", idx.Name), Duration: time.Since(started),
	})
	return nil
}

func ensureRetirementScheduled(ctx context.Context, conn *pgxpool.Conn, idx catalog.IndexInfo, cfg *config.Config) (catalog.IndexInfo, error) {
	if err := pg.ConfigureSession(ctx, conn, cfg.Connection.StatementTimeout, cfg.Connection.LockTimeout); err != nil {
		return idx, err
	}
	if idx.RetirementPending && !idx.RetireAfter.IsZero() {
		return idx, nil
	}
	gracePeriod, err := configuredGracePeriod(cfg)
	if err != nil {
		return idx, err
	}
	if err := conn.QueryRow(ctx,
		"SELECT clock_timestamp() + ($1 * interval '1 second')", gracePeriod.Seconds(),
	).Scan(&idx.RetireAfter); err != nil {
		return idx, fmt.Errorf("calculating retirement deadline for index %s: %w", idx.Name, err)
	}
	comment := catalog.RetirementComment(cfg, idx, idx.RetireAfter, idx.RetirementAuthorized)
	commentSQL := fmt.Sprintf("COMMENT ON INDEX %s IS %s",
		qualifiedIndex(idx.Schema, idx.Name), config.QuoteLiteral(comment))
	if _, err := conn.Exec(ctx, commentSQL); err != nil {
		return idx, fmt.Errorf("recording retirement deadline for index %s: %w", idx.Name, err)
	}
	idx.RetirementPending = true
	return idx, nil
}

func configuredGracePeriod(cfg *config.Config) (time.Duration, error) {
	if cfg.Reconcile.GracePeriod == "" {
		return 0, nil
	}
	duration, err := time.ParseDuration(cfg.Reconcile.GracePeriod)
	if err != nil {
		return 0, fmt.Errorf("parsing grace period: %w", err)
	}
	if duration < 0 {
		return 0, fmt.Errorf("grace period must be at least zero")
	}
	return duration, nil
}

func waitUntil(ctx context.Context, conn *pgxpool.Conn, deadline time.Time) error {
	var databaseNow time.Time
	if err := conn.QueryRow(ctx, "SELECT clock_timestamp()").Scan(&databaseNow); err != nil {
		return fmt.Errorf("checking retirement deadline: %w", err)
	}
	duration := deadline.Sub(databaseNow)
	if duration <= 0 {
		return nil
	}
	log.Printf("Waiting %s for durable retirement grace period...", duration.Round(time.Second))
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func retirementPlanReason(idx catalog.IndexInfo, cfg *config.Config) string {
	base := "the desired index is verified and this index belongs to the same managed family"
	if !idx.RetireAfter.IsZero() {
		return fmt.Sprintf("%s; its durable grace deadline is %s", base, idx.RetireAfter.UTC().Format(time.RFC3339Nano))
	}
	if gracePeriod, err := configuredGracePeriod(cfg); err == nil && gracePeriod > 0 {
		return fmt.Sprintf("%s; apply will persist and honor a %s grace period before dropping it", base, gracePeriod)
	}
	return base
}

func sameManagedFamily(idx catalog.IndexInfo, cfg *config.Config) bool {
	managed := idx.Owned && !idx.LegacyOwnership
	authorizedUnowned := idx.RetirementAuthorized && cfg.Reconcile.DropUnowned
	return (managed || authorizedUnowned) && idx.ManagedName == cfg.Index.Name
}

func blockingConflict(status *catalog.Status, cfg *config.Config) (*Action, error) {
	if status.NamespaceConflict != "" {
		err := fmt.Errorf("%w: %s.%s is occupied by %s", ErrNamespaceConflict, cfg.Table.Schema, cfg.Index.Name, status.NamespaceConflict)
		return &Action{
			Kind: ActionBlocked, Description: fmt.Sprintf("Refuse to publish %s because its namespace is occupied", cfg.Index.Name),
			Reason: err.Error(),
		}, err
	}
	desired := status.Desired()
	if desired == nil {
		return nil, nil
	}
	if desired.Managed && (!desired.Owned || desired.ManagedName != cfg.Index.Name || desired.RetirementPending) {
		err := fmt.Errorf("%w: %s is owned by %q for family %q", ErrManagedFamilyConflict, desired.Name, desired.Owner, desired.ManagedName)
		return &Action{
			Kind: ActionBlocked, Description: fmt.Sprintf("Refuse to replace managed index %s", desired.Name),
			Reason: "choose a unique index name or reconcile it with the configuration that owns that managed family",
		}, err
	}
	if !desired.Owned && !cfg.Reconcile.DropUnowned {
		err := fmt.Errorf("%w: %s; review plan and set reconcile.drop_unowned=true to authorize replacement", ErrUnownedDesired, desired.Name)
		return &Action{
			Kind: ActionBlocked, Description: fmt.Sprintf("Refuse to replace unowned index %s", desired.Name),
			Reason: "set reconcile.drop_unowned=true only after reviewing the plan and accepting destructive replacement",
		}, err
	}
	return nil, nil
}

func findIndex(indexes []catalog.IndexInfo, name string) *catalog.IndexInfo {
	for i := range indexes {
		if indexes[i].Name == name {
			return &indexes[i]
		}
	}
	return nil
}

func qualifiedIndex(schema, name string) string {
	return config.QuoteIdentifier(schema) + "." + config.QuoteIdentifier(name)
}

func appendFailure(result *ApplyResult, kind ActionKind, name, description string, err error) {
	result.Failed = true
	result.Actions = append(result.Actions, ActionResult{
		Kind: kind, IndexName: name, Description: description, Error: err.Error(),
	})
}

func cleanupAttemptedIndex(conn *pgxpool.Conn, cfg *config.Config, name string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := pg.ConfigureSession(ctx, conn, "30s", cfg.Connection.LockTimeout); err != nil {
		return err
	}
	if _, err := conn.Exec(ctx, fmt.Sprintf("DROP INDEX CONCURRENTLY IF EXISTS %s", qualifiedIndex(cfg.Table.Schema, name))); err != nil {
		return err
	}
	return nil
}

var fallbackNameCounter atomic.Uint64

func temporaryIndexName(base, phase string) (string, error) {
	random := make([]byte, 6)
	if _, err := rand.Read(random); err != nil {
		counter := fallbackNameCounter.Add(1)
		random = []byte{
			byte(counter >> 40), byte(counter >> 32), byte(counter >> 24),
			byte(counter >> 16), byte(counter >> 8), byte(counter),
		}
	}
	suffix := "_" + phase + "_" + hex.EncodeToString(random)
	maxBaseBytes := 63 - len(suffix)
	if maxBaseBytes < 1 {
		return "", fmt.Errorf("temporary index suffix exceeds PostgreSQL identifier limit")
	}
	for len([]byte(base)) > maxBaseBytes {
		_, size := utf8.DecodeLastRuneInString(base)
		if size == 0 {
			break
		}
		base = base[:len(base)-size]
	}
	return base + suffix, nil
}

// advisoryLockKey generates a deterministic table-scoped lock key.
func advisoryLockKey(schema, table string) int64 {
	hash := fnv.New64a()
	_, _ = hash.Write([]byte(schema))
	_, _ = hash.Write([]byte{0})
	_, _ = hash.Write([]byte(table))
	return int64(hash.Sum64())
}
