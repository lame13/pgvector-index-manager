package catalog

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/lame13/pgvector-index-manager/internal/config"
	"github.com/lame13/pgvector-index-manager/internal/pg"
)

const ownershipPrefix = "pgvector-index-manager:"

// Status holds the current state of indexes for a table.
type Status struct {
	Table           string
	VectorColumn    string
	DesiredIndex    string
	PGVectorVersion string
	Indexes         []IndexInfo
	Drift           bool
	DriftReasons    []string
}

// IndexInfo describes a single HNSW index on the target table.
type IndexInfo struct {
	Name                 string
	Schema               string
	AccessMethod         string
	OpClass              string
	Valid                bool
	Ready                bool
	Live                 bool
	Owned                bool
	LegacyOwnership      bool
	RetirementAuthorized bool
	ManagedName          string
	SpecHash             string
	IsDesired            bool
	IsHealthy            bool
	M                    int
	EFConstruction       int
	IndexExpression      string
	Predicate            string
	Definition           string
	Comment              string
	Size                 string
}

type ownershipMetadata struct {
	Version     int    `json:"version"`
	State       string `json:"state,omitempty"`
	Owner       string `json:"owner"`
	ManagedName string `json:"managed_name"`
	SpecHash    string `json:"spec_hash"`
}

// OwnershipComment returns the durable marker written to a managed index.
func OwnershipComment(cfg *config.Config) string {
	payload, _ := json.Marshal(ownershipMetadata{
		Version: 1, State: "managed", Owner: cfg.Reconcile.OwnershipTag,
		ManagedName: cfg.Index.Name, SpecHash: cfg.SpecHash(),
	})
	return ownershipPrefix + string(payload)
}

// RetirementComment durably records that an originally unowned, same-name
// index was explicitly authorized for retirement. This lets a later run finish
// cleanup if the process stops after the atomic publication step.
func RetirementComment(cfg *config.Config) string {
	payload, _ := json.Marshal(ownershipMetadata{
		Version: 1, State: "retire_unowned", Owner: cfg.Reconcile.OwnershipTag,
		ManagedName: cfg.Index.Name, SpecHash: cfg.SpecHash(),
	})
	return ownershipPrefix + string(payload)
}

// Inspect examines the current state of pgvector HNSW indexes for the
// configured table and returns a status report.
func Inspect(ctx context.Context, pool *pgxpool.Pool, cfg *config.Config) (*Status, error) {
	timeout, err := time.ParseDuration(cfg.Connection.StatementTimeout)
	if err != nil || timeout <= 0 {
		return nil, fmt.Errorf("invalid connection statement timeout %q", cfg.Connection.StatementTimeout)
	}
	if timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}
	extVersion, err := pg.ValidatePGVector(ctx, pool)
	if err != nil {
		return nil, err
	}

	var tableExists bool
	if err := pool.QueryRow(ctx,
		"SELECT to_regclass($1) IS NOT NULL",
		cfg.TableSQL(),
	).Scan(&tableExists); err != nil {
		return nil, fmt.Errorf("checking table %s: %w", cfg.FullyQualifiedName(), err)
	}
	if !tableExists {
		return nil, fmt.Errorf("table %s not found", cfg.FullyQualifiedName())
	}

	columns := []struct {
		label string
		name  string
	}{
		{label: "vector", name: cfg.Table.VectorCol},
	}
	for _, filter := range cfg.Population.Filters {
		columns = append(columns, struct {
			label string
			name  string
		}{label: "population filter", name: filter.Column})
	}
	for _, column := range columns {
		exists, err := columnExists(ctx, pool, cfg, column.name)
		if err != nil {
			return nil, err
		}
		if !exists {
			return nil, fmt.Errorf("%s column %s not found on %s", column.label, column.name, cfg.FullyQualifiedName())
		}
	}

	var sourceTypeName, sourceType string
	if err := pool.QueryRow(ctx, `
		SELECT source_type.typname, format_type(attribute.atttypid, attribute.atttypmod)
		FROM pg_attribute AS attribute
		JOIN pg_class AS table_cls ON table_cls.oid = attribute.attrelid
		JOIN pg_namespace AS table_ns ON table_ns.oid = table_cls.relnamespace
		JOIN pg_type AS source_type ON source_type.oid = attribute.atttypid
		WHERE table_ns.nspname = $1 AND table_cls.relname = $2
		  AND attribute.attname = $3 AND attribute.attnum > 0 AND NOT attribute.attisdropped`,
		cfg.Table.Schema, cfg.Table.Name, cfg.Table.VectorCol,
	).Scan(&sourceTypeName, &sourceType); err != nil {
		return nil, fmt.Errorf("determining vector column type: %w", err)
	}
	if sourceTypeName != "vector" && sourceTypeName != "halfvec" {
		return nil, fmt.Errorf("vector column %s has type %s, expected vector or halfvec", cfg.Table.VectorCol, sourceType)
	}

	rows, err := pool.Query(ctx, `
		SELECT
			index_ns.nspname,
			index_cls.relname,
			am.amname,
			opc.opcname,
			idx.indisvalid,
			idx.indisready,
			idx.indislive,
			COALESCE(pg_get_expr(idx.indexprs, idx.indrelid, true), ''),
			COALESCE(pg_get_expr(idx.indpred, idx.indrelid, true), ''),
			COALESCE(array_to_string(index_cls.reloptions, ','), ''),
			pg_get_indexdef(idx.indexrelid),
			COALESCE(description.description, ''),
			pg_size_pretty(pg_relation_size(idx.indexrelid))
		FROM pg_class AS index_cls
		JOIN pg_namespace AS index_ns ON index_ns.oid = index_cls.relnamespace
		JOIN pg_index AS idx ON idx.indexrelid = index_cls.oid
		JOIN pg_class AS table_cls ON table_cls.oid = idx.indrelid
		JOIN pg_namespace AS table_ns ON table_ns.oid = table_cls.relnamespace
		JOIN pg_am AS am ON am.oid = index_cls.relam
		JOIN pg_opclass AS opc ON opc.oid = idx.indclass[0]
		LEFT JOIN pg_description AS description
		  ON description.objoid = idx.indexrelid
		 AND description.classoid = 'pg_class'::regclass
		 AND description.objsubid = 0
		WHERE table_ns.nspname = $1
		  AND table_cls.relname = $2
		  AND am.amname = 'hnsw'
		ORDER BY index_cls.relname`,
		cfg.Table.Schema, cfg.Table.Name,
	)
	if err != nil {
		return nil, fmt.Errorf("querying HNSW indexes: %w", err)
	}
	defer rows.Close()

	var indexes []IndexInfo
	for rows.Next() {
		var idx IndexInfo
		var relOptions string
		if err := rows.Scan(
			&idx.Schema, &idx.Name, &idx.AccessMethod, &idx.OpClass,
			&idx.Valid, &idx.Ready, &idx.Live, &idx.IndexExpression,
			&idx.Predicate, &relOptions, &idx.Definition, &idx.Comment, &idx.Size,
		); err != nil {
			return nil, fmt.Errorf("scanning index info: %w", err)
		}
		idx.Owned, idx.LegacyOwnership, idx.RetirementAuthorized, idx.ManagedName, idx.SpecHash = ownership(idx.Comment, cfg.Reconcile.OwnershipTag)
		idx.IsDesired = idx.Name == cfg.Index.Name
		idx.IsHealthy = idx.Valid && idx.Ready && idx.Live
		idx.M, idx.EFConstruction = parseHNSWParams(relOptions)
		indexes = append(indexes, idx)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating indexes: %w", err)
	}

	status := &Status{
		Table: cfg.FullyQualifiedName(), VectorColumn: cfg.Table.VectorCol,
		DesiredIndex: cfg.Index.Name, PGVectorVersion: extVersion, Indexes: indexes,
	}
	status.Drift, status.DriftReasons = detectDrift(indexes, cfg)
	return status, nil
}

func columnExists(ctx context.Context, pool *pgxpool.Pool, cfg *config.Config, name string) (bool, error) {
	var exists bool
	if err := pool.QueryRow(ctx, `
		SELECT EXISTS(
			SELECT 1
			FROM pg_attribute AS attribute
			JOIN pg_class AS table_cls ON table_cls.oid = attribute.attrelid
			JOIN pg_namespace AS table_ns ON table_ns.oid = table_cls.relnamespace
			WHERE table_ns.nspname = $1 AND table_cls.relname = $2
			  AND attribute.attname = $3 AND attribute.attnum > 0 AND NOT attribute.attisdropped
		)`, cfg.Table.Schema, cfg.Table.Name, name,
	).Scan(&exists); err != nil {
		return false, fmt.Errorf("checking column %s: %w", name, err)
	}
	return exists, nil
}

// Desired returns the configured-name index, if one exists.
func (s *Status) Desired() *IndexInfo {
	for i := range s.Indexes {
		if s.Indexes[i].IsDesired {
			return &s.Indexes[i]
		}
	}
	return nil
}

// MatchingReplacement returns a healthy owned candidate created for the same
// desired name and exact spec. This enables safe retry after an interrupted
// swap without rebuilding another expensive index.
func (s *Status) MatchingReplacement(cfg *config.Config) *IndexInfo {
	for i := range s.Indexes {
		idx := &s.Indexes[i]
		if !idx.IsDesired && indexMatchesSpec(*idx, cfg) {
			return idx
		}
	}
	return nil
}

// IndexMatchesSpec reports whether an index is healthy and carries ownership
// metadata for the exact configured vector/population contract.
func IndexMatchesSpec(idx IndexInfo, cfg *config.Config) bool {
	return indexMatchesSpec(idx, cfg)
}

func indexMatchesSpec(idx IndexInfo, cfg *config.Config) bool {
	if !idx.IsHealthy || !idx.Owned || idx.LegacyOwnership {
		return false
	}
	if idx.ManagedName != cfg.Index.Name || idx.SpecHash != cfg.SpecHash() {
		return false
	}
	if idx.OpClass != cfg.OpClass() || idx.M != cfg.Index.M || idx.EFConstruction != cfg.Index.EFConstruction {
		return false
	}
	return (idx.Predicate == "") == (len(cfg.Population.Filters) == 0)
}

// parseHNSWParams extracts m and ef_construction from pg_class.reloptions.
// PostgreSQL omits options set to extension defaults, so those defaults are
// the starting values.
func parseHNSWParams(relOptions string) (m, efConstruction int) {
	m, efConstruction = 16, 64
	mRe := regexp.MustCompile(`(?:^|,)m\s*=\s*(\d+)`)
	efRe := regexp.MustCompile(`(?:^|,)ef_construction\s*=\s*(\d+)`)
	if match := mRe.FindStringSubmatch(relOptions); len(match) > 1 {
		m, _ = strconv.Atoi(match[1])
	}
	if match := efRe.FindStringSubmatch(relOptions); len(match) > 1 {
		efConstruction, _ = strconv.Atoi(match[1])
	}
	return m, efConstruction
}

func ownership(comment, tag string) (owned, legacy, retirementAuthorized bool, managedName, specHash string) {
	if tag == "" {
		return false, false, false, "", ""
	}
	if strings.HasPrefix(comment, ownershipPrefix) {
		var metadata ownershipMetadata
		if err := json.Unmarshal([]byte(strings.TrimPrefix(comment, ownershipPrefix)), &metadata); err == nil &&
			metadata.Version == 1 && metadata.Owner == tag && metadata.ManagedName != "" && metadata.SpecHash != "" {
			switch metadata.State {
			case "", "managed":
				return true, false, false, metadata.ManagedName, metadata.SpecHash
			case "retire_unowned":
				return false, false, true, metadata.ManagedName, metadata.SpecHash
			}
		}
	}
	if comment == "/* "+tag+" */" {
		return true, true, false, "", ""
	}
	return false, false, false, "", ""
}

func detectDrift(indexes []IndexInfo, cfg *config.Config) (bool, []string) {
	var reasons []string
	var desired *IndexInfo
	for i := range indexes {
		idx := &indexes[i]
		if idx.IsDesired {
			desired = idx
		}
		if ((idx.Owned && !idx.LegacyOwnership) || (idx.RetirementAuthorized && cfg.Reconcile.DropUnowned)) &&
			idx.ManagedName == cfg.Index.Name && !idx.IsDesired {
			reasons = append(reasons, fmt.Sprintf("stale managed index %s is awaiting retirement", idx.Name))
		}
	}
	if desired == nil {
		reasons = append(reasons, fmt.Sprintf("desired index %s not found", cfg.Index.Name))
		return true, reasons
	}
	if !desired.Valid {
		reasons = append(reasons, fmt.Sprintf("desired index %s is invalid", desired.Name))
	}
	if !desired.Ready {
		reasons = append(reasons, fmt.Sprintf("desired index %s is not ready", desired.Name))
	}
	if !desired.Live {
		reasons = append(reasons, fmt.Sprintf("desired index %s is not live", desired.Name))
	}
	if !desired.Owned {
		reasons = append(reasons, fmt.Sprintf("desired index %s is unowned", desired.Name))
	} else if desired.LegacyOwnership {
		reasons = append(reasons, fmt.Sprintf("desired index %s has legacy ownership metadata", desired.Name))
	}
	if desired.ManagedName != "" && desired.ManagedName != cfg.Index.Name {
		reasons = append(reasons, fmt.Sprintf("desired index %s belongs to managed family %s", desired.Name, desired.ManagedName))
	}
	if desired.SpecHash != "" && desired.SpecHash != cfg.SpecHash() {
		reasons = append(reasons, fmt.Sprintf("desired index %s does not match the configured population/vector spec", desired.Name))
	}
	if desired.OpClass != cfg.OpClass() {
		reasons = append(reasons, fmt.Sprintf("desired index %s uses opclass %s, expected %s", desired.Name, desired.OpClass, cfg.OpClass()))
	}
	if desired.M != cfg.Index.M {
		reasons = append(reasons, fmt.Sprintf("desired index %s has m=%d, expected %d", desired.Name, desired.M, cfg.Index.M))
	}
	if desired.EFConstruction != cfg.Index.EFConstruction {
		reasons = append(reasons, fmt.Sprintf("desired index %s has ef_construction=%d, expected %d", desired.Name, desired.EFConstruction, cfg.Index.EFConstruction))
	}
	if (desired.Predicate == "") != (len(cfg.Population.Filters) == 0) {
		reasons = append(reasons, fmt.Sprintf("desired index %s partial-index predicate presence does not match the configured population", desired.Name))
	}
	if desired.Owned && !desired.LegacyOwnership && desired.SpecHash == cfg.SpecHash() &&
		!indexMatchesSpec(*desired, cfg) && len(reasons) == 0 {
		reasons = append(reasons, fmt.Sprintf("desired index %s does not match the configured spec", desired.Name))
	}
	return len(reasons) > 0, reasons
}
