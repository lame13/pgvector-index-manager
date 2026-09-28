package cmd

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/lame13/pgvector-index-manager/internal/config"
	"github.com/lame13/pgvector-index-manager/internal/pg"
	"github.com/lame13/pgvector-index-manager/internal/reconciler"
)

// connectTargets opens one pool per distinct DSN and pairs every resolved
// configuration with its pool, so targets sharing a database share a pool. The
// returned function closes every pool that was opened.
func connectTargets(ctx context.Context, configs []*config.Config) ([]reconciler.Target, func(), error) {
	pools := make(map[string]*pgxpool.Pool, len(configs))
	closePools := func() {
		for _, pool := range pools {
			pool.Close()
		}
	}

	targets := make([]reconciler.Target, 0, len(configs))
	for _, cfg := range configs {
		pool, ok := pools[cfg.Connection.DSN]
		if !ok {
			connected, err := pg.Connect(ctx, cfg.Connection.DSN)
			if err != nil {
				closePools()
				return nil, nil, targetError("connecting to PostgreSQL", cfg.Name, err)
			}
			pool = connected
			pools[cfg.Connection.DSN] = pool
		}
		targets = append(targets, reconciler.Target{Name: cfg.Name, Config: cfg, Pool: pool})
	}
	return targets, closePools, nil
}

func targetError(operation, name string, err error) error {
	if name != "" {
		return fmt.Errorf("%s target %s: %w", operation, name, err)
	}
	return fmt.Errorf("%s: %w", operation, err)
}

// printTargetBanner labels the block that follows when a run covers a named
// target, so multi-target output stays readable.
func printTargetBanner(name string) {
	if name == "" {
		return
	}
	fmt.Printf("Target: %s\n", name)
}
