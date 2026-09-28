package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/spf13/cobra"

	"github.com/lame13/pgvector-index-manager/internal/config"
)

func TestConnectTargetsDefersConnectivityAndSharesPools(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	configs := []*config.Config{
		{Name: "first", Connection: config.ConnectionConfig{DSN: "postgres://localhost/testdb"}},
		{Name: "second", Connection: config.ConnectionConfig{DSN: "postgres://localhost/testdb"}},
	}
	targets, closePools, err := connectTargets(ctx, configs)
	if err != nil {
		t.Fatalf("pool setup attempted to connect before running targets: %v", err)
	}
	defer closePools()
	if len(targets) != 2 || targets[0].Pool != targets[1].Pool {
		t.Fatal("targets with the same DSN should share a pool")
	}
}

func TestCommandsIntegrationIsolateTargetFailures(t *testing.T) {
	dsn := os.Getenv("PGVECTOR_INDEX_MANAGER_TEST_DSN")
	if dsn == "" {
		t.Skip("PGVECTOR_INDEX_MANAGER_TEST_DSN is not set")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	schema := fmt.Sprintf("index_manager_cmd_%d", time.Now().UnixNano())
	if _, err := pool.Exec(ctx, "CREATE EXTENSION IF NOT EXISTS vector"); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, "CREATE SCHEMA "+config.QuoteIdentifier(schema)); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(ctx, "DROP SCHEMA "+config.QuoteIdentifier(schema)+" CASCADE")
	})
	if _, err := pool.Exec(ctx, "CREATE TABLE "+config.QuoteIdentifier(schema)+".documents (embedding vector(3))"); err != nil {
		t.Fatal(err)
	}

	for _, command := range []*cobra.Command{statusCmd, planCmd, applyCmd} {
		for _, failure := range []string{"missing-table", "connection", "report"} {
			if failure == "report" && command != applyCmd {
				continue
			}
			t.Run(command.Name()+"/"+failure, func(t *testing.T) {
				dir := t.TempDir()
				overrides := "    table:\n      name: missing\n"
				switch failure {
				case "connection":
					// A nonexistent Unix socket fails immediately without contacting a service.
					overrides = "    connection:\n      dsn: " + yamlString("host="+dir+"/missing dbname=indexmanager connect_timeout=1") + "\n"
				case "report":
					blocked := filepath.Join(dir, "blocked")
					if err := os.WriteFile(blocked, []byte("not a directory"), 0600); err != nil {
						t.Fatal(err)
					}
					overrides = "    report:\n      output_dir: " + yamlString(blocked) + "\n"
				}
				contents := fmt.Sprintf(`connection:
  dsn: %s
  statement_timeout: 2s
table:
  schema: %s
  name: documents
  vector_column: embedding
index:
  dimensions: 3
report:
  output_dir: %s
targets:
  - name: broken
    index:
      name: broken_idx
%s  - name: healthy
    index:
      name: healthy_idx
  - name: also-broken
    table:
      name: also_missing
    index:
      name: also_broken_idx
`, yamlString(dsn), schema, yamlString(dir), overrides)
				path := filepath.Join(dir, "config.yaml")
				if err := os.WriteFile(path, []byte(contents), 0600); err != nil {
					t.Fatal(err)
				}
				previousConfig, previousDryRun := CfgFile, dryRun
				CfgFile, dryRun = path, false
				t.Cleanup(func() { CfgFile, dryRun = previousConfig, previousDryRun })
				command.SetContext(ctx)
				// A file avoids blocking on a full stdout pipe while printing results.
				output, err := os.CreateTemp(dir, "stdout")
				if err != nil {
					t.Fatal(err)
				}
				defer output.Close()
				previousStdout := os.Stdout
				os.Stdout = output
				defer func() { os.Stdout = previousStdout }()
				runErr := command.RunE(command, nil)
				if runErr == nil || !strings.Contains(runErr.Error(), "broken") || !strings.Contains(runErr.Error(), "also-broken") {
					t.Errorf("RunE() error = %v, want every failed target named", runErr)
				}
				if _, err := output.Seek(0, io.SeekStart); err != nil {
					t.Fatal(err)
				}
				data, err := io.ReadAll(output)
				if err != nil {
					t.Fatal(err)
				}
				if !strings.Contains(string(data), "Target: healthy") {
					t.Errorf("healthy target was skipped: %s", data)
				}
				if command == applyCmd {
					if _, err := os.Stat(filepath.Join(dir, "report-healthy.json")); err != nil {
						t.Errorf("healthy target report missing: %v", err)
					}
				}
			})
		}
	}
}

func yamlString(value string) string {
	quoted, _ := json.Marshal(value)
	return string(quoted)
}
