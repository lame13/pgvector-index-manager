package config

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"
)

type Config struct {
	// Name identifies the target inside a multi-target configuration file. It
	// is empty for legacy single-target files. See targets.go.
	Name       string           `yaml:"-"`
	Connection ConnectionConfig `yaml:"connection"`
	Table      TableConfig      `yaml:"table"`
	Index      IndexConfig      `yaml:"index"`
	Population PopulationConfig `yaml:"population"`
	Reconcile  ReconcileConfig  `yaml:"reconcile"`
	Report     ReportConfig     `yaml:"report"`
}

type ConnectionConfig struct {
	DSN              string `yaml:"dsn"`
	StatementTimeout string `yaml:"statement_timeout"`
	LockTimeout      string `yaml:"lock_timeout"`
}

type TableConfig struct {
	Schema    string `yaml:"schema"`
	Name      string `yaml:"name"`
	VectorCol string `yaml:"vector_column"`
}

type IndexConfig struct {
	Name           string `yaml:"name"`
	Type           string `yaml:"type"`            // vector or halfvec
	Metric         string `yaml:"metric"`          // cosine, l2, ip
	Dimensions     int    `yaml:"dimensions"`      // vector dimensions
	M              int    `yaml:"m"`               // HNSW m parameter
	EFConstruction int    `yaml:"ef_construction"` // HNSW ef_construction parameter
}

// PopulationConfig defines the rows covered by the partial HNSW index. An
// empty filter list means the whole table.
type PopulationConfig struct {
	Filters []FilterConfig `yaml:"filters"`
}

// FilterConfig is a safe equality/IN predicate. Values are represented as
// strings and PostgreSQL coerces each literal to the column's type.
type FilterConfig struct {
	Column string   `yaml:"column"`
	Values []string `yaml:"values"`
}

type ReconcileConfig struct {
	// OwnershipTag identifies this manager. Retirement also requires matching
	// structured managed-family metadata, not just this tag.
	OwnershipTag string `yaml:"ownership_tag"`

	// DropUnowned controls whether the tool will drop indexes it did not create.
	// Default: false (safe).
	DropUnowned bool `yaml:"drop_unowned"`

	// BuildTimeout is the maximum time for a single index build. Default: 1h.
	BuildTimeout string `yaml:"build_timeout"`

	// GracePeriod is the time to wait after building a replacement before
	// retiring the old index. Default: 0s.
	GracePeriod string `yaml:"grace_period"`

	// Continuous enables continuous reconciliation mode.
	Continuous bool `yaml:"continuous"`

	// Interval is the polling interval in continuous mode. Default: 5m.
	Interval string `yaml:"interval"`
}

type ReportConfig struct {
	OutputDir            string `yaml:"output_dir"`
	TargetLabel          string `yaml:"target_label"`
	RedactConnection     bool   `yaml:"redact_connection"`
	IncludeTargetDetails bool   `yaml:"include_target_details"`
}

func defaultConfig() *Config {
	return &Config{
		Connection: ConnectionConfig{
			StatementTimeout: "30s",
			LockTimeout:      "5s",
		},
		Table: TableConfig{
			Schema: "public",
		},
		Index: IndexConfig{
			Type:           "vector",
			Metric:         "cosine",
			Dimensions:     0,
			M:              16,
			EFConstruction: 64,
		},
		Reconcile: ReconcileConfig{
			OwnershipTag: "pgvector-index-manager",
			BuildTimeout: "1h",
			Interval:     "5m",
		},
		Report: ReportConfig{
			OutputDir:        "./reports",
			TargetLabel:      "target",
			RedactConnection: true,
		},
	}
}

func (c *Config) validate() error {
	if c.Connection.DSN == "" {
		return fmt.Errorf("connection.dsn is required")
	}
	if err := validateDuration("connection.statement_timeout", c.Connection.StatementTimeout, false); err != nil {
		return err
	}
	if err := validateDuration("connection.lock_timeout", c.Connection.LockTimeout, true); err != nil {
		return err
	}
	identifiers := []struct{ name, value string }{
		{name: "table.schema", value: c.Table.Schema},
		{name: "table.name", value: c.Table.Name},
		{name: "table.vector_column", value: c.Table.VectorCol},
		{name: "index.name", value: c.Index.Name},
	}
	for _, identifier := range identifiers {
		if err := validateIdentifier(identifier.name, identifier.value); err != nil {
			return err
		}
	}

	validTypes := map[string]bool{"vector": true, "halfvec": true}
	if !validTypes[c.Index.Type] {
		return fmt.Errorf("index.type must be one of: vector, halfvec (got %q)", c.Index.Type)
	}

	validMetrics := map[string]bool{"cosine": true, "l2": true, "ip": true}
	if !validMetrics[c.Index.Metric] {
		return fmt.Errorf("index.metric must be one of: cosine, l2, ip (got %q)", c.Index.Metric)
	}
	dimensionLimit := 2000
	if c.Index.Type == "halfvec" {
		dimensionLimit = 4000
	}
	if c.Index.Dimensions < 1 || c.Index.Dimensions > dimensionLimit {
		return fmt.Errorf("index.dimensions must be between 1 and %d for %s (got %d)", dimensionLimit, c.Index.Type, c.Index.Dimensions)
	}

	if c.Index.M < 1 || c.Index.M > 100 {
		return fmt.Errorf("index.m must be between 1 and 100 (got %d)", c.Index.M)
	}
	if c.Index.EFConstruction < 1 || c.Index.EFConstruction > 1000 {
		return fmt.Errorf("index.ef_construction must be between 1 and 1000 (got %d)", c.Index.EFConstruction)
	}

	if !ownershipTagPattern.MatchString(c.Reconcile.OwnershipTag) {
		return fmt.Errorf("reconcile.ownership_tag must be 1-128 characters using letters, numbers, '.', '_', ':', '/', or '-'")
	}
	if err := validateDuration("reconcile.build_timeout", c.Reconcile.BuildTimeout, false); err != nil {
		return err
	}
	if c.Reconcile.GracePeriod != "" {
		if err := validateDuration("reconcile.grace_period", c.Reconcile.GracePeriod, true); err != nil {
			return err
		}
	}
	if c.Reconcile.Continuous {
		if err := validateDuration("reconcile.interval", c.Reconcile.Interval, false); err != nil {
			return err
		}
	}

	seenColumns := make(map[string]struct{}, len(c.Population.Filters))
	for i, filter := range c.Population.Filters {
		if err := validateIdentifier(fmt.Sprintf("population.filters[%d].column", i), filter.Column); err != nil {
			return err
		}
		if _, exists := seenColumns[filter.Column]; exists {
			return fmt.Errorf("population filter column %q is configured more than once", filter.Column)
		}
		seenColumns[filter.Column] = struct{}{}
		if len(filter.Values) == 0 {
			return fmt.Errorf("population.filters[%d].values must not be empty", i)
		}
		seenValues := make(map[string]struct{}, len(filter.Values))
		for _, value := range filter.Values {
			if strings.IndexByte(value, 0) >= 0 {
				return fmt.Errorf("population.filters[%d].values must not contain NUL bytes", i)
			}
			if _, exists := seenValues[value]; exists {
				return fmt.Errorf("population filter %q contains duplicate value %q", filter.Column, value)
			}
			seenValues[value] = struct{}{}
		}
	}

	if c.Report.OutputDir == "" {
		return fmt.Errorf("report.output_dir is required")
	}
	if strings.TrimSpace(c.Report.TargetLabel) == "" {
		return fmt.Errorf("report.target_label is required")
	}

	return nil
}

var ownershipTagPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:/-]{0,127}$`)

func validateIdentifier(name, value string) error {
	if value == "" {
		return fmt.Errorf("%s is required", name)
	}
	if !utf8.ValidString(value) || strings.IndexByte(value, 0) >= 0 {
		return fmt.Errorf("%s must be valid UTF-8 without NUL bytes", name)
	}
	if len([]byte(value)) > 63 {
		return fmt.Errorf("%s must be at most 63 bytes to avoid PostgreSQL identifier truncation", name)
	}
	return nil
}

func validateDuration(name, value string, allowZero bool) error {
	duration, err := time.ParseDuration(value)
	if err != nil {
		return fmt.Errorf("%s must be a valid duration: %w", name, err)
	}
	if duration < 0 || (!allowZero && duration == 0) {
		comparison := "greater than"
		if allowZero {
			comparison = "at least"
		}
		return fmt.Errorf("%s must be %s zero", name, comparison)
	}
	return nil
}

// VectorCast returns the PostgreSQL type name used in ::casts.
func (c *Config) VectorCast() string {
	return fmt.Sprintf("%s(%d)", c.Index.Type, c.Index.Dimensions)
}

// TableSQL returns the schema-qualified table name, safely quoted for SQL.
func (c *Config) TableSQL() string {
	return pgx.Identifier{c.Table.Schema, c.Table.Name}.Sanitize()
}

// FullyQualifiedName returns schema.name.
func (c *Config) FullyQualifiedName() string {
	return fmt.Sprintf("%s.%s", c.Table.Schema, c.Table.Name)
}

// OpClass returns the pgvector operator class for the configured type and metric.
func (c *Config) OpClass() string {
	return fmt.Sprintf("%s_%s_ops", c.Index.Type, c.Index.Metric)
}

// IndexExpressionSQL returns the safely quoted and dimensioned expression used
// by the HNSW index. The explicit cast makes vector identity part of the DDL.
func (c *Config) IndexExpressionSQL() string {
	return fmt.Sprintf("(%s::%s)", QuoteIdentifier(c.Table.VectorCol), c.VectorCast())
}

// PopulationPredicateSQL returns a safe partial-index predicate. An empty
// string means the index covers the whole table.
func (c *Config) PopulationPredicateSQL() string {
	if len(c.Population.Filters) == 0 {
		return ""
	}
	filters := append([]FilterConfig(nil), c.Population.Filters...)
	sort.Slice(filters, func(i, j int) bool { return filters[i].Column < filters[j].Column })
	clauses := make([]string, 0, len(filters))
	for _, filter := range filters {
		values := append([]string(nil), filter.Values...)
		sort.Strings(values)
		literals := make([]string, len(values))
		for i, value := range values {
			literals[i] = QuoteLiteral(value)
		}
		column := QuoteIdentifier(filter.Column)
		if len(literals) == 1 {
			clauses = append(clauses, fmt.Sprintf("%s = %s", column, literals[0]))
		} else {
			clauses = append(clauses, fmt.Sprintf("%s IN (%s)", column, strings.Join(literals, ", ")))
		}
	}
	return strings.Join(clauses, " AND ")
}

// SpecHash returns a stable identity for every catalog-relevant part of the
// managed index. Reordering filters or their values does not change the hash.
func (c *Config) SpecHash() string {
	type canonicalFilter struct {
		Column string   `json:"column"`
		Values []string `json:"values"`
	}
	type canonicalSpec struct {
		Schema         string            `json:"schema"`
		Table          string            `json:"table"`
		Index          string            `json:"index"`
		VectorColumn   string            `json:"vector_column"`
		Type           string            `json:"type"`
		Metric         string            `json:"metric"`
		Dimensions     int               `json:"dimensions"`
		M              int               `json:"m"`
		EFConstruction int               `json:"ef_construction"`
		Filters        []canonicalFilter `json:"filters"`
	}
	filters := make([]canonicalFilter, 0, len(c.Population.Filters))
	for _, filter := range c.Population.Filters {
		values := append([]string(nil), filter.Values...)
		sort.Strings(values)
		filters = append(filters, canonicalFilter{Column: filter.Column, Values: values})
	}
	sort.Slice(filters, func(i, j int) bool { return filters[i].Column < filters[j].Column })
	payload, _ := json.Marshal(canonicalSpec{
		Schema: c.Table.Schema, Table: c.Table.Name, Index: c.Index.Name,
		VectorColumn: c.Table.VectorCol, Type: c.Index.Type, Metric: c.Index.Metric,
		Dimensions: c.Index.Dimensions, M: c.Index.M, EFConstruction: c.Index.EFConstruction,
		Filters: filters,
	})
	hash := sha256.Sum256(payload)
	return hex.EncodeToString(hash[:])
}

// QuoteLiteral quotes a string for trusted DDL that cannot use bind
// parameters. E” syntax keeps backslash behavior independent of server
// standard_conforming_strings settings.
func QuoteLiteral(value string) string {
	value = strings.ReplaceAll(value, `\`, `\\`)
	value = strings.ReplaceAll(value, `'`, `\'`)
	return "E'" + value + "'"
}

// QuoteIdentifier safely quotes one PostgreSQL identifier.
func QuoteIdentifier(identifier string) string {
	return pgx.Identifier{identifier}.Sanitize()
}
