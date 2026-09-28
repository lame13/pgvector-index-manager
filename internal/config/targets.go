package config

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"regexp"
	"strings"

	"gopkg.in/yaml.v3"
)

// Document is the parsed shape of a configuration file. Every section is
// optional at this level: sections at the top of the file are shared defaults,
// and each entry under targets overrides only the fields it sets.
//
// The override types track field presence explicitly, so an explicit false or
// an empty filter list is distinguishable from an omitted setting.
type Document struct {
	Connection *ConnectionOverride `yaml:"connection"`
	Table      *TableOverride      `yaml:"table"`
	Index      *IndexOverride      `yaml:"index"`
	Population *PopulationOverride `yaml:"population"`
	Reconcile  *ReconcileOverride  `yaml:"reconcile"`
	Report     *ReportOverride     `yaml:"report"`
	Targets    []TargetConfig      `yaml:"targets"`
}

// TargetConfig is one managed index family in a multi-target file. Name labels
// the target in command output and in report filenames.
type TargetConfig struct {
	Name       string              `yaml:"name"`
	Connection *ConnectionOverride `yaml:"connection"`
	Table      *TableOverride      `yaml:"table"`
	Index      *IndexOverride      `yaml:"index"`
	Population *PopulationOverride `yaml:"population"`
	Reconcile  *ReconcileOverride  `yaml:"reconcile"`
	Report     *ReportOverride     `yaml:"report"`
}

// ConnectionOverride replaces individual connection settings.
type ConnectionOverride struct {
	DSN              *string `yaml:"dsn"`
	StatementTimeout *string `yaml:"statement_timeout"`
	LockTimeout      *string `yaml:"lock_timeout"`
}

// TableOverride replaces individual table settings.
type TableOverride struct {
	Schema    *string `yaml:"schema"`
	Name      *string `yaml:"name"`
	VectorCol *string `yaml:"vector_column"`
}

// IndexOverride replaces individual index settings.
type IndexOverride struct {
	Name           *string `yaml:"name"`
	Type           *string `yaml:"type"`
	Metric         *string `yaml:"metric"`
	Dimensions     *int    `yaml:"dimensions"`
	M              *int    `yaml:"m"`
	EFConstruction *int    `yaml:"ef_construction"`
}

// PopulationOverride replaces the filter list. Setting an empty list is a
// deliberate way to widen a target to the whole table.
type PopulationOverride struct {
	Filters *[]FilterConfig `yaml:"filters"`
}

// ReconcileOverride replaces individual reconciliation settings.
//
// Continuous mode and its interval are accepted here only so existing files
// keep parsing and so targets inherit the shared values; LoadTargets rejects a
// per-target declaration of them because one process runs a single poll loop
// for every target.
type ReconcileOverride struct {
	OwnershipTag *string `yaml:"ownership_tag"`
	DropUnowned  *bool   `yaml:"drop_unowned"`
	BuildTimeout *string `yaml:"build_timeout"`
	GracePeriod  *string `yaml:"grace_period"`
	Continuous   *bool   `yaml:"continuous"`
	Interval     *string `yaml:"interval"`
}

// ReportOverride replaces individual report settings.
type ReportOverride struct {
	OutputDir            *string `yaml:"output_dir"`
	TargetLabel          *string `yaml:"target_label"`
	RedactConnection     *bool   `yaml:"redact_connection"`
	IncludeTargetDetails *bool   `yaml:"include_target_details"`
}

func (o *ConnectionOverride) apply(cfg *ConnectionConfig) {
	if o == nil {
		return
	}
	if o.DSN != nil {
		cfg.DSN = *o.DSN
	}
	if o.StatementTimeout != nil {
		cfg.StatementTimeout = *o.StatementTimeout
	}
	if o.LockTimeout != nil {
		cfg.LockTimeout = *o.LockTimeout
	}
}

func (o *TableOverride) apply(cfg *TableConfig) {
	if o == nil {
		return
	}
	if o.Schema != nil {
		cfg.Schema = *o.Schema
	}
	if o.Name != nil {
		cfg.Name = *o.Name
	}
	if o.VectorCol != nil {
		cfg.VectorCol = *o.VectorCol
	}
}

func (o *IndexOverride) apply(cfg *IndexConfig) {
	if o == nil {
		return
	}
	if o.Name != nil {
		cfg.Name = *o.Name
	}
	if o.Type != nil {
		cfg.Type = *o.Type
	}
	if o.Metric != nil {
		cfg.Metric = *o.Metric
	}
	if o.Dimensions != nil {
		cfg.Dimensions = *o.Dimensions
	}
	if o.M != nil {
		cfg.M = *o.M
	}
	if o.EFConstruction != nil {
		cfg.EFConstruction = *o.EFConstruction
	}
}

func (o *PopulationOverride) apply(cfg *PopulationConfig) {
	if o == nil || o.Filters == nil {
		return
	}
	filters := make([]FilterConfig, 0, len(*o.Filters))
	for _, filter := range *o.Filters {
		filters = append(filters, FilterConfig{
			Column: filter.Column,
			Values: append([]string(nil), filter.Values...),
		})
	}
	cfg.Filters = filters
}

func (o *ReconcileOverride) apply(cfg *ReconcileConfig) {
	if o == nil {
		return
	}
	if o.OwnershipTag != nil {
		cfg.OwnershipTag = *o.OwnershipTag
	}
	if o.DropUnowned != nil {
		cfg.DropUnowned = *o.DropUnowned
	}
	if o.BuildTimeout != nil {
		cfg.BuildTimeout = *o.BuildTimeout
	}
	if o.GracePeriod != nil {
		cfg.GracePeriod = *o.GracePeriod
	}
	if o.Continuous != nil {
		cfg.Continuous = *o.Continuous
	}
	if o.Interval != nil {
		cfg.Interval = *o.Interval
	}
}

func (o *ReportOverride) apply(cfg *ReportConfig) {
	if o == nil {
		return
	}
	if o.OutputDir != nil {
		cfg.OutputDir = *o.OutputDir
	}
	if o.TargetLabel != nil {
		cfg.TargetLabel = *o.TargetLabel
	}
	if o.RedactConnection != nil {
		cfg.RedactConnection = *o.RedactConnection
	}
	if o.IncludeTargetDetails != nil {
		cfg.IncludeTargetDetails = *o.IncludeTargetDetails
	}
}

// overrides keeps the section overrides in one value so root defaults and
// per-target overrides can share a single merge path.
type overrides struct {
	connection *ConnectionOverride
	table      *TableOverride
	index      *IndexOverride
	population *PopulationOverride
	reconcile  *ReconcileOverride
	report     *ReportOverride
}

func (o overrides) apply(cfg *Config) {
	o.connection.apply(&cfg.Connection)
	o.table.apply(&cfg.Table)
	o.index.apply(&cfg.Index)
	o.population.apply(&cfg.Population)
	o.reconcile.apply(&cfg.Reconcile)
	o.report.apply(&cfg.Report)
}

func (t TargetConfig) overrides() overrides {
	return overrides{
		connection: t.Connection,
		table:      t.Table,
		index:      t.Index,
		population: t.Population,
		reconcile:  t.Reconcile,
		report:     t.Report,
	}
}

func (d *Document) rootOverrides() overrides {
	return overrides{
		connection: d.Connection,
		table:      d.Table,
		index:      d.Index,
		population: d.Population,
		reconcile:  d.Reconcile,
		report:     d.Report,
	}
}

// Load reads a configuration file that describes exactly one target. Files
// that list several targets must go through LoadTargets.
func Load(path string) (*Config, error) {
	targets, err := LoadTargets(path)
	if err != nil {
		return nil, err
	}
	if len(targets) != 1 {
		return nil, fmt.Errorf("configuration defines %d targets; use LoadTargets to reconcile them together", len(targets))
	}
	return targets[0], nil
}

// LoadTargets reads and validates a configuration file, returning one resolved
// and fully validated Config per target. A file without a targets list yields a
// single target, so legacy files keep working unchanged.
//
// Top-level sections are shared defaults. Each target overrides only the
// fields it sets, which lets a file share one DSN, table, or reconcile policy
// while giving every managed index family its own name, population, and
// ownership tag.
func LoadTargets(path string) ([]*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading config file: %w", err)
	}
	doc, err := decodeDocument(data)
	if err != nil {
		return nil, fmt.Errorf("parsing config: %w", err)
	}

	base := defaultConfig()
	doc.rootOverrides().apply(base)

	if doc.Targets == nil {
		if err := base.validate(); err != nil {
			return nil, err
		}
		return []*Config{base}, nil
	}
	if len(doc.Targets) == 0 {
		return nil, fmt.Errorf("targets must not be empty")
	}

	labelSetByRoot := doc.Report != nil && doc.Report.TargetLabel != nil
	targets := make([]*Config, 0, len(doc.Targets))
	seenNames := make(map[string]int, len(doc.Targets))
	type indexKey struct{ dsn, schema, name string }
	seenIndexes := make(map[indexKey]string, len(doc.Targets))
	for i, target := range doc.Targets {
		name := strings.TrimSpace(target.Name)
		if name == "" {
			return nil, fmt.Errorf("targets[%d].name is required", i)
		}
		if err := validateTargetName(name); err != nil {
			return nil, fmt.Errorf("targets[%d].name: %w", i, err)
		}
		// Report filenames must also be distinct on case-insensitive filesystems.
		nameKey := strings.ToLower(name)
		if previous, duplicate := seenNames[nameKey]; duplicate {
			return nil, fmt.Errorf("targets[%d].name %q duplicates targets[%d].name", i, name, previous)
		}
		seenNames[nameKey] = i

		if target.Reconcile != nil && (target.Reconcile.Continuous != nil || target.Reconcile.Interval != nil) {
			return nil, fmt.Errorf("targets[%d] (%s): reconcile.continuous and reconcile.interval are run-level settings; declare them in the shared reconcile section", i, name)
		}

		resolved := base.clone()
		resolved.Name = name
		target.overrides().apply(resolved)
		if !labelSetByRoot && (target.Report == nil || target.Report.TargetLabel == nil) {
			resolved.Report.TargetLabel = name
		}
		if err := resolved.validate(); err != nil {
			return nil, fmt.Errorf("targets[%d] (%s): %w", i, name, err)
		}

		key := indexKey{resolved.Connection.DSN, resolved.Table.Schema, resolved.Index.Name}
		if previous, duplicate := seenIndexes[key]; duplicate {
			return nil, fmt.Errorf("targets[%d] (%s) and target %s both manage %s.%s on the same DSN", i, name, previous, key.schema, key.name)
		}
		seenIndexes[key] = name

		targets = append(targets, resolved)
	}
	return targets, nil
}

func decodeDocument(data []byte) (*Document, error) {
	doc := &Document{}
	decoder := yaml.NewDecoder(bytes.NewReader(data))
	decoder.KnownFields(true)
	if err := decoder.Decode(doc); err != nil {
		return nil, err
	}
	if err := ensureSingleDocument(decoder); err != nil {
		return nil, err
	}
	return doc, nil
}

func ensureSingleDocument(decoder *yaml.Decoder) error {
	var extra any
	err := decoder.Decode(&extra)
	if err == io.EOF {
		return nil
	}
	if err != nil {
		return err
	}
	return fmt.Errorf("multiple YAML documents are not supported")
}

var targetNamePattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$`)

func validateTargetName(name string) error {
	if !targetNamePattern.MatchString(name) {
		return fmt.Errorf("%q must be 1-64 characters using letters, numbers, '.', '_', or '-'", name)
	}
	return nil
}

// clone deep-copies a resolved configuration so per-target overrides never
// mutate the shared base.
func (c *Config) clone() *Config {
	clone := *c
	clone.Population.Filters = make([]FilterConfig, 0, len(c.Population.Filters))
	for _, filter := range c.Population.Filters {
		clone.Population.Filters = append(clone.Population.Filters, FilterConfig{
			Column: filter.Column,
			Values: append([]string(nil), filter.Values...),
		})
	}
	return &clone
}
