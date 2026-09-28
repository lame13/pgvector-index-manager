package cmd

import (
	"errors"
	"fmt"

	"github.com/spf13/cobra"

	"github.com/lame13/pgvector-index-manager/internal/catalog"
	"github.com/lame13/pgvector-index-manager/internal/config"
)

var statusCmd = &cobra.Command{
	Use:   "status",
	Short: "Inspect current pgvector HNSW indexes",
	Long: `Inspect the current state of pgvector HNSW indexes for the configured
table. Shows index health, ownership status, and drift from desired
configuration.

A configuration file may declare several targets; status is printed for each
of them.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		if CfgFile == "" {
			return fmt.Errorf("--config (-c) flag is required")
		}

		configs, err := config.LoadTargets(CfgFile)
		if err != nil {
			return fmt.Errorf("loading config: %w", err)
		}

		ctx := cmd.Context()
		targets, closePools, err := connectTargets(ctx, configs)
		if err != nil {
			return err
		}
		defer closePools()

		var errs []error
		for _, target := range targets {
			printTargetBanner(target.Name)
			status, err := catalog.Inspect(ctx, target.Pool, target.Config)
			if err != nil {
				errs = append(errs, targetError("inspecting indexes", target.Name, err))
				continue
			}
			printStatus(status, target.Config)
		}

		return errors.Join(errs...)
	},
}

func init() {
	rootCmd.AddCommand(statusCmd)
}

func printStatus(status *catalog.Status, cfg *config.Config) {
	fmt.Println()
	fmt.Println("═══════════════════════════════════════════")
	fmt.Println("  Index Status")
	fmt.Println("═══════════════════════════════════════════")
	fmt.Printf("  Table: %s\n", status.Table)
	fmt.Printf("  Vector column: %s\n", status.VectorColumn)
	fmt.Printf("  pgvector version: %s\n", status.PGVectorVersion)
	fmt.Printf("  Desired index: %s\n", status.DesiredIndex)
	fmt.Println()

	// Show desired configuration
	fmt.Println("  Desired Configuration:")
	fmt.Printf("    Type: %s\n", cfg.Index.Type)
	fmt.Printf("    Metric: %s\n", cfg.Index.Metric)
	fmt.Printf("    Dimensions: %d\n", cfg.Index.Dimensions)
	fmt.Printf("    OpClass: %s\n", cfg.OpClass())
	if cfg.Index.M > 0 {
		fmt.Printf("    m: %d\n", cfg.Index.M)
	}
	if cfg.Index.EFConstruction > 0 {
		fmt.Printf("    ef_construction: %d\n", cfg.Index.EFConstruction)
	}
	if len(cfg.Population.Filters) == 0 {
		fmt.Println("    Population filters: none (whole table)")
	} else {
		fmt.Printf("    Population filters: %d\n", len(cfg.Population.Filters))
	}
	fmt.Println()

	if len(status.Indexes) == 0 {
		fmt.Println("  Current Indexes: None found")
	} else {
		fmt.Println("  Current Indexes:")
		fmt.Println()
		fmt.Printf("    %-25s  %-10s  %-8s  %-8s  %-8s  %-8s  %-10s\n",
			"Name", "Access", "Valid", "Ready", "Live", "Owned", "Size")
		fmt.Println("    ─────────────────────────  ──────────  ────────  ────────  ────────  ────────  ──────────")
		for _, idx := range status.Indexes {
			owned := "no"
			if idx.RetirementPending {
				owned = "retire"
			} else if idx.Owned {
				owned = "yes"
			} else if idx.Managed {
				owned = "foreign"
			}
			fmt.Printf("    %-25s  %-10s  %-8t  %-8t  %-8t  %-8s  %-10s\n",
				idx.Name, idx.AccessMethod, idx.Valid, idx.Ready, idx.Live, owned, idx.Size)
		}
	}

	fmt.Println()
	if status.Drift {
		fmt.Println("  ⚠ Drift detected:")
		for _, reason := range status.DriftReasons {
			fmt.Printf("    • %s\n", reason)
		}
	} else if len(status.Indexes) > 0 {
		fmt.Println("  ✓ No drift detected: indexes match desired configuration.")
	} else {
		fmt.Println("  ⚠ No indexes found. Run 'apply' to create the desired index.")
	}
	fmt.Println("═══════════════════════════════════════════")
	fmt.Println()
}
