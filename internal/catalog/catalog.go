package catalog

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/lame13/pgvector-index-manager/internal/config"
	"github.com/lame13/pgvector-index-manager/internal/pg"
)

const ownershipPrefix = "pgvector-index-manager:"

// Status holds the current state of indexes for a table.
type Status struct {
	Table             string
	VectorColumn      string
	DesiredIndex      string
	PGVectorVersion   string
	NamespaceConflict string
	Indexes           []IndexInfo
	Drift             bool
	DriftReasons      []string
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
	Managed              bool
	LegacyOwnership      bool
	RetirementAuthorized bool
	RetirementPending    bool
	Owner                string
	ManagedName          string
	SpecHash             string
	StructureHash        string
	ActualStructureHash  string
	RetireAfter          time.Time
	IsDesired            bool
	IsHealthy            bool
	M                    int
	EFConstruction       int
	TotalColumns         int
	KeyColumns           int
	DirectColumn         string
	IndexType            string
	IndexExpression      string
	ReferencedColumns    []string
	Predicate            string
	Definition           string
	Comment              string
	Size                 string
}

type ownershipMetadata struct {
	Version       int    `json:"version"`
	State         string `json:"state,omitempty"`
	Owner         string `json:"owner"`
	ManagedName   string `json:"managed_name"`
	SpecHash      string `json:"spec_hash"`
	StructureHash string `json:"structure_hash,omitempty"`
	RetireAfter   string `json:"retire_after,omitempty"`
}

// OwnershipComment returns the durable marker written to a managed index.
func OwnershipComment(cfg *config.Config, idx IndexInfo) string {
	payload, _ := json.Marshal(ownershipMetadata{
		Version: 2, State: "managed", Owner: cfg.Reconcile.OwnershipTag,
		ManagedName: cfg.Index.Name, SpecHash: cfg.SpecHash(), StructureHash: idx.ActualStructureHash,
	})
	return ownershipPrefix + string(payload)
}

// RetirementComment durably records both retirement authority and the grace
// deadline. This lets a later run honor the remaining grace period and finish
// cleanup if the process stops after publication.
func RetirementComment(cfg *config.Config, idx IndexInfo, retireAfter time.Time, originallyUnowned bool) string {
	state := "retiring"
	if originallyUnowned {
		state = "retire_unowned"
	}
	payload, _ := json.Marshal(ownershipMetadata{
		Version: 2, State: state, Owner: cfg.Reconcile.OwnershipTag,
		ManagedName: cfg.Index.Name, SpecHash: cfg.SpecHash(), StructureHash: idx.ActualStructureHash,
		RetireAfter: retireAfter.UTC().Format(time.RFC3339Nano),
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
			idx.indnatts,
			idx.indnkeyatts,
			COALESCE(key_attribute.attname, ''),
			format_type(index_attribute.atttypid, index_attribute.atttypmod),
			COALESCE(pg_get_expr(idx.indexprs, idx.indrelid, true), ''),
			COALESCE(pg_get_expr(idx.indpred, idx.indrelid, true), ''),
			ARRAY(
				SELECT referenced_attribute.attname
				FROM pg_depend AS dependency
				JOIN pg_attribute AS referenced_attribute
				  ON referenced_attribute.attrelid = dependency.refobjid
				 AND referenced_attribute.attnum = dependency.refobjsubid
				WHERE dependency.classid = 'pg_class'::regclass
				  AND dependency.objid = idx.indexrelid
				  AND dependency.refclassid = 'pg_class'::regclass
				  AND dependency.refobjid = idx.indrelid
				  AND dependency.refobjsubid > 0
				ORDER BY referenced_attribute.attname
			),
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
		JOIN pg_attribute AS index_attribute
		  ON index_attribute.attrelid = idx.indexrelid
		 AND index_attribute.attnum = 1
		LEFT JOIN pg_attribute AS key_attribute
		  ON key_attribute.attrelid = idx.indrelid
		 AND key_attribute.attnum = idx.indkey[0]
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
			&idx.Valid, &idx.Ready, &idx.Live, &idx.TotalColumns, &idx.KeyColumns,
			&idx.DirectColumn, &idx.IndexType, &idx.IndexExpression,
			&idx.Predicate, &idx.ReferencedColumns, &relOptions, &idx.Definition, &idx.Comment, &idx.Size,
		); err != nil {
			return nil, fmt.Errorf("scanning index info: %w", err)
		}
		idx.M, idx.EFConstruction = parseHNSWParams(relOptions)
		idx.ActualStructureHash = structureHash(idx)
		applyOwnership(&idx, ownership(idx.Comment, cfg.Reconcile.OwnershipTag))
		idx.IsDesired = idx.Name == cfg.Index.Name
		idx.IsHealthy = idx.Valid && idx.Ready && idx.Live
		indexes = append(indexes, idx)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating indexes: %w", err)
	}

	status := &Status{
		Table: cfg.FullyQualifiedName(), VectorColumn: cfg.Table.VectorCol,
		DesiredIndex: cfg.Index.Name, PGVectorVersion: extVersion, Indexes: indexes,
	}
	var relationKind string
	err = pool.QueryRow(ctx, `
		SELECT relation.relkind::text
		FROM pg_class AS relation
		JOIN pg_namespace AS namespace ON namespace.oid = relation.relnamespace
		WHERE namespace.nspname = $1 AND relation.relname = $2`,
		cfg.Table.Schema, cfg.Index.Name,
	).Scan(&relationKind)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("checking desired index namespace: %w", err)
	}
	if err == nil && status.Desired() == nil {
		status.NamespaceConflict = relationKindDescription(relationKind)
	}
	status.Drift, status.DriftReasons = detectDrift(indexes, cfg)
	if status.NamespaceConflict != "" {
		status.Drift = true
		status.DriftReasons = append([]string{
			fmt.Sprintf("desired index name %s is occupied by %s in schema %s", cfg.Index.Name, status.NamespaceConflict, cfg.Table.Schema),
		}, status.DriftReasons...)
	}
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
	if !idx.IsHealthy || !idx.Owned || idx.LegacyOwnership || idx.RetirementPending {
		return false
	}
	if idx.ManagedName != cfg.Index.Name || idx.SpecHash != cfg.SpecHash() {
		return false
	}
	if idx.StructureHash == "" || idx.StructureHash != idx.ActualStructureHash {
		return false
	}
	return IndexStructureMatchesSpec(idx, cfg)
}

// IndexStructureMatchesSpec verifies catalog structure independently of the
// ownership/spec claim stored in the index comment.
func IndexStructureMatchesSpec(idx IndexInfo, cfg *config.Config) bool {
	if idx.AccessMethod != "hnsw" || idx.OpClass != cfg.OpClass() ||
		idx.M != cfg.Index.M || idx.EFConstruction != cfg.Index.EFConstruction ||
		idx.TotalColumns != 1 || idx.KeyColumns != 1 || idx.IndexType != cfg.VectorCast() {
		return false
	}
	if !indexExpressionMatches(idx, cfg) || !sameStrings(idx.ReferencedColumns, expectedReferencedColumns(cfg)) {
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

type ownershipInfo struct {
	owned, managed, legacy, retirementAuthorized, retirementPending bool
	owner, managedName, specHash, structureHash                     string
	retireAfter                                                     time.Time
}

func ownership(comment, tag string) ownershipInfo {
	if tag == "" {
		return ownershipInfo{}
	}
	if strings.HasPrefix(comment, ownershipPrefix) {
		var metadata ownershipMetadata
		if err := json.Unmarshal([]byte(strings.TrimPrefix(comment, ownershipPrefix)), &metadata); err == nil &&
			(metadata.Version == 1 || metadata.Version == 2) && metadata.Owner != "" && metadata.ManagedName != "" && metadata.SpecHash != "" {
			info := ownershipInfo{
				managed: true, owner: metadata.Owner, managedName: metadata.ManagedName,
				specHash: metadata.SpecHash, structureHash: metadata.StructureHash,
			}
			if metadata.RetireAfter != "" {
				info.retireAfter, _ = time.Parse(time.RFC3339Nano, metadata.RetireAfter)
			}
			switch metadata.State {
			case "", "managed":
				info.owned = metadata.Owner == tag
				return info
			case "retiring":
				info.owned = metadata.Owner == tag
				info.retirementPending = true
				return info
			case "retire_unowned":
				info.retirementAuthorized = metadata.Owner == tag
				info.retirementPending = true
				return info
			}
		}
	}
	if comment == "/* "+tag+" */" {
		return ownershipInfo{owned: true, legacy: true, owner: tag}
	}
	return ownershipInfo{}
}

func applyOwnership(idx *IndexInfo, info ownershipInfo) {
	idx.Owned = info.owned
	idx.Managed = info.managed
	idx.LegacyOwnership = info.legacy
	idx.RetirementAuthorized = info.retirementAuthorized
	idx.RetirementPending = info.retirementPending
	idx.Owner = info.owner
	idx.ManagedName = info.managedName
	idx.SpecHash = info.specHash
	idx.StructureHash = info.structureHash
	idx.RetireAfter = info.retireAfter
}

func structureHash(idx IndexInfo) string {
	type structure struct {
		AccessMethod      string   `json:"access_method"`
		OpClass           string   `json:"opclass"`
		M                 int      `json:"m"`
		EFConstruction    int      `json:"ef_construction"`
		TotalColumns      int      `json:"total_columns"`
		KeyColumns        int      `json:"key_columns"`
		DirectColumn      string   `json:"direct_column"`
		IndexType         string   `json:"index_type"`
		IndexExpression   string   `json:"index_expression"`
		Predicate         string   `json:"predicate"`
		ReferencedColumns []string `json:"referenced_columns"`
	}
	columns := append([]string(nil), idx.ReferencedColumns...)
	sort.Strings(columns)
	payload, _ := json.Marshal(structure{
		AccessMethod: idx.AccessMethod, OpClass: idx.OpClass, M: idx.M, EFConstruction: idx.EFConstruction,
		TotalColumns: idx.TotalColumns, KeyColumns: idx.KeyColumns, DirectColumn: idx.DirectColumn,
		IndexType: idx.IndexType, IndexExpression: idx.IndexExpression, Predicate: idx.Predicate,
		ReferencedColumns: columns,
	})
	hash := sha256.Sum256(payload)
	return fmt.Sprintf("%x", hash[:])
}

func indexExpressionMatches(idx IndexInfo, cfg *config.Config) bool {
	if idx.DirectColumn != "" {
		return idx.DirectColumn == cfg.Table.VectorCol && idx.IndexExpression == ""
	}
	quoted := config.QuoteIdentifier(cfg.Table.VectorCol)
	identifiers := []string{cfg.Table.VectorCol, quoted}
	for _, identifier := range identifiers {
		for _, expected := range []string{
			fmt.Sprintf("(%s)::%s", identifier, cfg.VectorCast()),
			fmt.Sprintf("%s::%s", identifier, cfg.VectorCast()),
			fmt.Sprintf("(%s::%s)", identifier, cfg.VectorCast()),
		} {
			if idx.IndexExpression == expected {
				return true
			}
		}
	}
	return false
}

func expectedReferencedColumns(cfg *config.Config) []string {
	seen := map[string]struct{}{cfg.Table.VectorCol: {}}
	for _, filter := range cfg.Population.Filters {
		seen[filter.Column] = struct{}{}
	}
	columns := make([]string, 0, len(seen))
	for column := range seen {
		columns = append(columns, column)
	}
	sort.Strings(columns)
	return columns
}

func sameStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

func relationKindDescription(kind string) string {
	switch kind {
	case "r", "p":
		return "a table"
	case "v":
		return "a view"
	case "m":
		return "a materialized view"
	case "S":
		return "a sequence"
	case "i", "I":
		return "another index"
	case "f":
		return "a foreign table"
	default:
		return "another relation"
	}
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
	if desired.RetirementPending {
		reasons = append(reasons, fmt.Sprintf("desired index %s is marked for retirement", desired.Name))
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
	if desired.TotalColumns != 1 || desired.KeyColumns != 1 {
		reasons = append(reasons, fmt.Sprintf("desired index %s does not have exactly one HNSW key and no included columns", desired.Name))
	}
	if desired.IndexType != cfg.VectorCast() {
		reasons = append(reasons, fmt.Sprintf("desired index %s stores %s values, expected %s", desired.Name, desired.IndexType, cfg.VectorCast()))
	}
	if !indexExpressionMatches(*desired, cfg) {
		reasons = append(reasons, fmt.Sprintf("desired index %s does not index the configured vector column/cast", desired.Name))
	}
	if !sameStrings(desired.ReferencedColumns, expectedReferencedColumns(cfg)) {
		reasons = append(reasons, fmt.Sprintf("desired index %s references columns outside the configured vector/population contract", desired.Name))
	}
	if (desired.Predicate == "") != (len(cfg.Population.Filters) == 0) {
		reasons = append(reasons, fmt.Sprintf("desired index %s partial-index predicate presence does not match the configured population", desired.Name))
	}
	if desired.Owned && !desired.LegacyOwnership {
		if desired.StructureHash == "" {
			reasons = append(reasons, fmt.Sprintf("desired index %s lacks structural verification metadata", desired.Name))
		} else if desired.StructureHash != desired.ActualStructureHash {
			reasons = append(reasons, fmt.Sprintf("desired index %s catalog structure differs from its verified structure", desired.Name))
		}
	}
	return len(reasons) > 0, reasons
}
