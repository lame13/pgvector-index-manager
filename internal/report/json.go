package report

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/lame13/pgvector-index-manager/internal/config"
	"github.com/lame13/pgvector-index-manager/internal/reconciler"
)

// JSONReport is the top-level structure written to the JSON report file.
type JSONReport struct {
	Tool      string       `json:"tool"`
	Version   string       `json:"version"`
	Timestamp string       `json:"timestamp"`
	Table     string       `json:"table,omitempty"`
	DryRun    bool         `json:"dry_run"`
	Failed    bool         `json:"failed"`
	Config    JSONConfig   `json:"config"`
	Actions   []JSONAction `json:"actions"`
	Summary   JSONSummary  `json:"summary"`
}

// JSONConfig describes the reconciliation configuration (privacy-safe).
type JSONConfig struct {
	Target            string `json:"target"`
	Connection        string `json:"connection,omitempty"`
	IndexName         string `json:"index_name,omitempty"`
	IndexType         string `json:"index_type"`
	Metric            string `json:"metric"`
	Dimensions        int    `json:"dimensions"`
	M                 int    `json:"m"`
	EFConstruction    int    `json:"ef_construction"`
	PopulationFilters int    `json:"population_filter_count"`
	OwnershipTag      string `json:"ownership_tag,omitempty"`
	DropUnowned       bool   `json:"drop_unowned"`
}

// JSONAction describes a single reconciliation action.
type JSONAction struct {
	Kind        reconciler.ActionKind `json:"kind"`
	Description string                `json:"description"`
	Error       string                `json:"error,omitempty"`
}

// JSONSummary provides aggregate counts.
type JSONSummary struct {
	TotalActions int `json:"total_actions"`
	Successful   int `json:"successful"`
	Failed       int `json:"failed"`
}

// WriteJSON produces the privacy-safe JSON report file.
func WriteJSON(version string, result *reconciler.ApplyResult, cfg *config.Config) error {
	if err := os.MkdirAll(cfg.Report.OutputDir, 0750); err != nil {
		return fmt.Errorf("creating output directory: %w", err)
	}

	report := JSONReport{
		Tool:      "pgvector-index-manager",
		Version:   version,
		Timestamp: result.StartTime.UTC().Format(time.RFC3339),
		DryRun:    result.DryRun,
		Failed:    result.Failed,
		Actions:   make([]JSONAction, 0, len(result.Actions)),
		Config: JSONConfig{
			Target:            cfg.Report.TargetLabel,
			IndexType:         cfg.Index.Type,
			Metric:            cfg.Index.Metric,
			Dimensions:        cfg.Index.Dimensions,
			M:                 cfg.Index.M,
			EFConstruction:    cfg.Index.EFConstruction,
			PopulationFilters: len(cfg.Population.Filters),
			DropUnowned:       cfg.Reconcile.DropUnowned,
		},
	}

	if cfg.Report.RedactConnection {
		report.Config.Connection = "[redacted]"
	} else {
		report.Config.Connection = cfg.Connection.DSN
	}
	if cfg.Report.IncludeTargetDetails {
		report.Table = result.Table
		report.Config.IndexName = cfg.Index.Name
		report.Config.OwnershipTag = cfg.Reconcile.OwnershipTag
	}

	for _, action := range result.Actions {
		description := action.Description
		errorMessage := action.Error
		if !cfg.Report.IncludeTargetDetails {
			description = genericActionDescription(action.Kind)
			if errorMessage != "" {
				errorMessage = "[redacted; enable report.include_target_details for diagnostics]"
			}
		}
		report.Actions = append(report.Actions, JSONAction{
			Kind: action.Kind, Description: description, Error: errorMessage,
		})
		if action.Error != "" {
			report.Summary.Failed++
		} else {
			report.Summary.Successful++
		}
	}
	report.Summary.TotalActions = len(result.Actions)

	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return fmt.Errorf("marshalling report: %w", err)
	}

	path := filepath.Join(cfg.Report.OutputDir, "report.json")
	if err := writePrivateFile(path, data); err != nil {
		return fmt.Errorf("writing JSON report: %w", err)
	}

	return nil
}

func genericActionDescription(kind reconciler.ActionKind) string {
	switch kind {
	case reconciler.ActionBuild:
		return "Build and verify replacement index"
	case reconciler.ActionPublish:
		return "Atomically publish verified replacement"
	case reconciler.ActionRetire:
		return "Retire superseded managed index concurrently"
	case reconciler.ActionBlocked:
		return "Refuse unsafe same-name replacement"
	default:
		return "Reconciliation action"
	}
}

// writePrivateFile writes data to a file with restrictive permissions.
func writePrivateFile(path string, data []byte) error {
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return err
	}
	if err := file.Chmod(0600); err != nil {
		file.Close()
		return err
	}
	if _, err := file.Write(data); err != nil {
		file.Close()
		return err
	}
	return file.Close()
}
