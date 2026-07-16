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

// ErrUnownedDesired is returned when applying drift would require replacing a
// same-name index that the manager cannot prove it owns.
var ErrUnownedDesired = errors.New("desired index is unowned")

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

	desired := status.Desired()
	if desired == nil || !catalog.IndexMatchesSpec(*desired, cfg) {
		if desired != nil && !desired.Owned && !cfg.Reconcile.DropUnowned {
			result.Blocked = true
			result.Actions = append(result.Actions, Action{
				Kind:        ActionBlocked,
				Description: fmt.Sprintf("Refuse to replace unowned index %s", desired.Name),
				Reason:      "set reconcile.drop_unowned=true only after reviewing the plan and accepting destructive replacement",
			})
			return result
		}
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
			Reason: "the desired index is verified and this index belongs to the same managed family",
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
			return result, ErrUnownedDesired
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

	desired := status.Desired()
	if desired == nil || !catalog.IndexMatchesSpec(*desired, cfg) {
		if desired != nil && !desired.Owned && !cfg.Reconcile.DropUnowned {
			err := fmt.Errorf("%w: %s; review plan and set reconcile.drop_unowned=true to authorize replacement", ErrUnownedDesired, desired.Name)
			appendFailure(result, ActionBlocked, desired.Name, fmt.Sprintf("Refuse to replace unowned index %s", desired.Name), err)
			return result, err
		}

		replacement := status.MatchingReplacement(cfg)
		if replacement == nil {
			built, err := buildReplacement(ctx, lockConn, pool, cfg, result)
			if err != nil {
				return result, err
			}
			replacement = built
		}

		retired, err := publishReplacement(ctx, lockConn, cfg, *replacement, desired, result)
		if err != nil {
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

		if retired != nil {
			if err := waitGracePeriod(ctx, cfg); err != nil {
				result.Failed = true
				return result, err
			}
			if err := retireIndex(ctx, lockConn, *retired, cfg, result); err != nil {
				return result, err
			}
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
		if err := retireIndex(ctx, lockConn, idx, cfg, result); err != nil {
			return result, err
		}
	}
	return result, nil
}

// ApplyContinuous reconciles immediately and then at each configured interval.
// Cancellation is a clean shutdown rather than a runtime failure.
func ApplyContinuous(ctx context.Context, pool *pgxpool.Pool, cfg *config.Config, onResult func(*ApplyResult) error) error {
	interval, err := time.ParseDuration(cfg.Reconcile.Interval)
	if err != nil {
		return fmt.Errorf("parsing interval: %w", err)
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	log.Printf("Starting continuous reconciliation (interval: %s)", interval)

	for {
		result, applyErr := Apply(ctx, pool, cfg, false)
		if result != nil && onResult != nil && (len(result.Actions) > 0 || result.Failed) {
			if callbackErr := onResult(result); callbackErr != nil {
				log.Printf("Reconciliation result handling error: %v", callbackErr)
			}
		}
		if applyErr != nil && !errors.Is(applyErr, context.Canceled) {
			log.Printf("Reconciliation error: %v", applyErr)
		}

		select {
		case <-ctx.Done():
			log.Printf("Stopping continuous reconciliation")
			return nil
		case <-ticker.C:
		}
	}
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
	if candidate == nil || !candidate.IsHealthy || candidate.OpClass != cfg.OpClass() ||
		candidate.M != cfg.Index.M || candidate.EFConstruction != cfg.Index.EFConstruction ||
		(candidate.Predicate == "") != (len(cfg.Population.Filters) == 0) {
		err := fmt.Errorf("replacement index did not pass catalog verification")
		appendFailure(result, ActionBuild, name, fmt.Sprintf("Verify replacement index %s", name), err)
		if cleanupErr := cleanupAttemptedIndex(conn, cfg, name); cleanupErr != nil {
			appendFailure(result, ActionRetire, name, fmt.Sprintf("Clean up failed replacement index %s", name), cleanupErr)
		}
		return nil, err
	}

	commentSQL := fmt.Sprintf("COMMENT ON INDEX %s IS %s",
		qualifiedIndex(cfg.Table.Schema, name), config.QuoteLiteral(catalog.OwnershipComment(cfg)))
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

func publishReplacement(ctx context.Context, conn *pgxpool.Conn, cfg *config.Config, replacement catalog.IndexInfo, current *catalog.IndexInfo, result *ApplyResult) (*catalog.IndexInfo, error) {
	started := time.Now()
	if err := pg.ConfigureSession(ctx, conn, cfg.Connection.StatementTimeout, cfg.Connection.LockTimeout); err != nil {
		return nil, err
	}
	tx, err := conn.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("beginning atomic index swap: %w", err)
	}
	defer tx.Rollback(ctx)

	var retired *catalog.IndexInfo
	if current != nil {
		retirementName, err := temporaryIndexName(cfg.Index.Name, "retired")
		if err != nil {
			return nil, err
		}
		renameOldSQL := fmt.Sprintf("ALTER INDEX %s RENAME TO %s",
			qualifiedIndex(current.Schema, current.Name), config.QuoteIdentifier(retirementName))
		if _, err := tx.Exec(ctx, renameOldSQL); err != nil {
			appendFailure(result, ActionPublish, cfg.Index.Name, fmt.Sprintf("Move current index %s aside", current.Name), err)
			return nil, fmt.Errorf("renaming current index: %w", err)
		}
		if !current.Owned {
			commentSQL := fmt.Sprintf("COMMENT ON INDEX %s IS %s",
				qualifiedIndex(current.Schema, retirementName), config.QuoteLiteral(catalog.RetirementComment(cfg)))
			if _, err := tx.Exec(ctx, commentSQL); err != nil {
				appendFailure(result, ActionPublish, current.Name, fmt.Sprintf("Record authorized retirement for %s", current.Name), err)
				return nil, fmt.Errorf("recording authorized retirement: %w", err)
			}
		}
		copy := *current
		copy.Name = retirementName
		copy.IsDesired = false
		if !current.Owned {
			copy.RetirementAuthorized = true
			copy.ManagedName = cfg.Index.Name
			copy.SpecHash = cfg.SpecHash()
		}
		retired = &copy
	}

	renameReplacementSQL := fmt.Sprintf("ALTER INDEX %s RENAME TO %s",
		qualifiedIndex(replacement.Schema, replacement.Name), config.QuoteIdentifier(cfg.Index.Name))
	if _, err := tx.Exec(ctx, renameReplacementSQL); err != nil {
		appendFailure(result, ActionPublish, cfg.Index.Name, fmt.Sprintf("Publish replacement as %s", cfg.Index.Name), err)
		return nil, fmt.Errorf("publishing replacement index: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		appendFailure(result, ActionPublish, cfg.Index.Name, fmt.Sprintf("Commit publication of %s", cfg.Index.Name), err)
		return nil, fmt.Errorf("committing atomic index swap: %w", err)
	}
	result.Actions = append(result.Actions, ActionResult{
		Kind: ActionPublish, IndexName: cfg.Index.Name,
		Description: fmt.Sprintf("Atomically publish replacement as %s", cfg.Index.Name), Duration: time.Since(started),
	})
	return retired, nil
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

func waitGracePeriod(ctx context.Context, cfg *config.Config) error {
	if cfg.Reconcile.GracePeriod == "" {
		return nil
	}
	duration, err := time.ParseDuration(cfg.Reconcile.GracePeriod)
	if err != nil || duration <= 0 {
		return err
	}
	log.Printf("Applying retirement grace period of %s...", duration)
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func sameManagedFamily(idx catalog.IndexInfo, cfg *config.Config) bool {
	managed := idx.Owned && !idx.LegacyOwnership
	authorizedUnowned := idx.RetirementAuthorized && cfg.Reconcile.DropUnowned
	return (managed || authorizedUnowned) && idx.ManagedName == cfg.Index.Name
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
