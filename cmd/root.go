package cmd

import (
	"context"
	"errors"

	"github.com/spf13/cobra"
)

var CfgFile string

// Version is set by main.go via go:embed.
var Version = "dev"

// ErrNothingToApply identifies a plan run where no indexes need changes. The
// process boundary maps it to exit code 0.
var ErrNothingToApply = errors.New("no index changes needed")

var rootCmd = &cobra.Command{
	Use:   "pgvector-index-manager",
	Short: "Safe reconciler for population-specific pgvector HNSW indexes",
	Long: `pgvector-index-manager inspects, plans, and safely reconciles pgvector
HNSW indexes for population-specific workloads.

It inspects existing indexes, detects drift from desired configuration,
builds replacements concurrently, tracks ownership, verifies replacements
before retiring old indexes, and supports one-shot and continuous
reconciliation modes.`,
	SilenceErrors: true,
	SilenceUsage:  true,
}

func init() {
	rootCmd.PersistentFlags().StringVarP(&CfgFile, "config", "c", "", "path to YAML config file")
}

// Execute runs the root command with cancellation propagated to database work.
func Execute(ctx context.Context) error {
	return rootCmd.ExecuteContext(ctx)
}

// ExitCode maps command outcomes to the documented process exit codes.
func ExitCode(err error) int {
	if err == nil {
		return 0
	}
	if errors.Is(err, ErrNothingToApply) {
		return 0
	}
	return 1
}
