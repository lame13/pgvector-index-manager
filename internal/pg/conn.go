package pg

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Connect opens a connection pool and verifies connectivity.
func Connect(ctx context.Context, dsn string) (*pgxpool.Pool, error) {
	poolConfig, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parsing connection config: %w", err)
	}
	poolConfig.MaxConns = 4
	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return nil, fmt.Errorf("creating connection pool: %w", err)
	}

	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("connecting to PostgreSQL: %w", err)
	}

	return pool, nil
}

// ValidatePGVector checks that the pgvector extension is installed and meets
// the minimum version requirement.
func ValidatePGVector(ctx context.Context, pool *pgxpool.Pool) (string, error) {
	var extVersion string
	err := pool.QueryRow(ctx,
		"SELECT extversion FROM pg_extension WHERE extname = 'vector'",
	).Scan(&extVersion)
	if err != nil {
		return "", fmt.Errorf("pgvector extension not found (install pgvector first): %w", err)
	}
	if err := checkVersion(extVersion); err != nil {
		return "", err
	}
	return extVersion, nil
}

func checkVersion(version string) error {
	parts := strings.SplitN(version, ".", 3)
	if len(parts) < 2 {
		return fmt.Errorf("cannot parse pgvector extension version %q", version)
	}
	major, err := strconv.Atoi(parts[0])
	if err != nil {
		return fmt.Errorf("cannot parse pgvector extension version %q: %w", version, err)
	}
	minor, err := strconv.Atoi(parts[1])
	if err != nil {
		return fmt.Errorf("cannot parse pgvector extension version %q: %w", version, err)
	}
	if major == 0 && minor < 8 {
		return fmt.Errorf("pgvector %s detected; version 0.8+ required", version)
	}
	return nil
}

// AcquireAdvisoryLock acquires a session-level advisory lock for the given key.
// This serializes index operations across multiple manager instances.
func AcquireAdvisoryLock(ctx context.Context, pool *pgxpool.Pool, lockKey int64) (*pgxpool.Conn, error) {
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("acquiring dedicated lock connection: %w", err)
	}
	var acquired bool
	err = conn.QueryRow(ctx, "SELECT pg_try_advisory_lock($1)", lockKey).Scan(&acquired)
	if err != nil {
		conn.Release()
		return nil, fmt.Errorf("acquiring advisory lock: %w", err)
	}
	if !acquired {
		conn.Release()
		return nil, fmt.Errorf("could not acquire advisory lock (another instance may be running)")
	}
	return conn, nil
}

// ReleaseAdvisoryLock releases a session-level advisory lock on the exact
// connection that acquired it. If cleanup cannot be proven, the connection is
// closed instead of returning a lock-bearing session to the pool.
func ReleaseAdvisoryLock(conn *pgxpool.Conn, lockKey int64) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var released bool
	err := conn.QueryRow(ctx, "SELECT pg_advisory_unlock($1)", lockKey).Scan(&released)
	if err != nil || !released {
		raw := conn.Hijack()
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
		_ = raw.Close(closeCtx)
		closeCancel()
		if err != nil {
			return fmt.Errorf("releasing advisory lock: %w", err)
		}
		return fmt.Errorf("releasing advisory lock: lock was not held by the dedicated connection")
	}
	conn.Release()
	return nil
}

// ConfigureSession applies bounded statement and lock timeouts to a dedicated
// connection used for non-transactional concurrent index DDL.
func ConfigureSession(ctx context.Context, conn *pgxpool.Conn, statementTimeout, lockTimeout string) error {
	if _, err := conn.Exec(ctx, "SELECT set_config('statement_timeout', $1, false)", statementTimeout); err != nil {
		return fmt.Errorf("setting session statement timeout: %w", err)
	}
	if _, err := conn.Exec(ctx, "SELECT set_config('lock_timeout', $1, false)", lockTimeout); err != nil {
		return fmt.Errorf("setting session lock timeout: %w", err)
	}
	return nil
}
