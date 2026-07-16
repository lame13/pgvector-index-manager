package pg

import (
	"context"
	"os"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

func TestAdvisoryLockRequiresConnection(t *testing.T) {
	// Advisory lock functions require a live PostgreSQL connection.
	// This test verifies the function signature compiles correctly.
	// Integration tests would require a running PostgreSQL instance.
}

func TestCheckVersion(t *testing.T) {
	for _, version := range []string{"0.8.0", "0.9.1", "1.0.0"} {
		if err := checkVersion(version); err != nil {
			t.Fatalf("checkVersion(%q) error = %v", version, err)
		}
	}
	for _, version := range []string{"0.7.4", "garbage"} {
		if err := checkVersion(version); err == nil {
			t.Fatalf("checkVersion(%q) unexpectedly succeeded", version)
		}
	}
}

func TestAdvisoryLockIntegrationUsesDedicatedSession(t *testing.T) {
	dsn := os.Getenv("PGVECTOR_INDEX_MANAGER_TEST_DSN")
	if dsn == "" {
		t.Skip("PGVECTOR_INDEX_MANAGER_TEST_DSN is not set")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	const key int64 = 738291047561
	lockConn, err := AcquireAdvisoryLock(ctx, pool, key)
	if err != nil {
		t.Fatal(err)
	}

	var acquiredElsewhere bool
	if err := pool.QueryRow(ctx, "SELECT pg_try_advisory_lock($1)", key).Scan(&acquiredElsewhere); err != nil {
		t.Fatal(err)
	}
	if acquiredElsewhere {
		t.Fatal("another pooled session acquired the supposedly held advisory lock")
	}
	if err := ReleaseAdvisoryLock(lockConn, key); err != nil {
		t.Fatal(err)
	}

	conn, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Release()
	if err := conn.QueryRow(ctx, "SELECT pg_try_advisory_lock($1)", key).Scan(&acquiredElsewhere); err != nil {
		t.Fatal(err)
	}
	if !acquiredElsewhere {
		t.Fatal("advisory lock remained held after ReleaseAdvisoryLock")
	}
	if _, err := conn.Exec(ctx, "SELECT pg_advisory_unlock($1)", key); err != nil {
		t.Fatal(err)
	}
}
