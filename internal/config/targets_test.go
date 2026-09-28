package config

import (
	"path/filepath"
	"strings"
	"testing"
)

const multiTargetConfig = `
connection:
  dsn: postgres://localhost/testdb
  statement_timeout: 45s
table:
  schema: public
  name: documents
  vector_column: embedding
index:
  type: vector
  metric: cosine
  dimensions: 128
  m: 24
population:
  filters:
    - column: tenant
      values: ["acme"]
reconcile:
  ownership_tag: shared-tool
  drop_unowned: true
  build_timeout: 45m
report:
  output_dir: ./reports
  include_target_details: true
targets:
  - name: active
    index:
      name: documents_active_idx
    population:
      filters:
        - column: status
          values: ["active"]
  - name: whole-table
    index:
      name: documents_all_idx
    population:
      filters: []
    reconcile:
      ownership_tag: whole-table-tool
      drop_unowned: false
    report:
      output_dir: ./reports/whole-table
`

func TestLoadTargetsInheritsSharedSectionsAndOverridesTargetFields(t *testing.T) {
	targets, err := LoadTargets(writeTestConfig(t, multiTargetConfig))
	if err != nil {
		t.Fatalf("LoadTargets() error = %v", err)
	}
	if len(targets) != 2 {
		t.Fatalf("LoadTargets() returned %d targets, want 2", len(targets))
	}
	active, whole := targets[0], targets[1]

	if active.Name != "active" || whole.Name != "whole-table" {
		t.Fatalf("target names = %q/%q, want active/whole-table", active.Name, whole.Name)
	}

	// Shared sections are inherited field by field.
	if active.Connection.DSN != "postgres://localhost/testdb" || active.Connection.StatementTimeout != "45s" {
		t.Fatalf("connection inheritance = %#v", active.Connection)
	}
	if active.Table.Schema != "public" || active.Table.Name != "documents" || active.Table.VectorCol != "embedding" {
		t.Fatalf("table inheritance = %#v", active.Table)
	}
	if active.Index.Type != "vector" || active.Index.Dimensions != 128 || active.Index.M != 24 {
		t.Fatalf("index inheritance = %#v", active.Index)
	}
	if active.Reconcile.BuildTimeout != "45m" || !active.Report.IncludeTargetDetails {
		t.Fatalf("reconcile/report inheritance = %#v/%#v", active.Reconcile, active.Report)
	}

	// Target sections replace exactly the fields they set.
	if active.Index.Name != "documents_active_idx" || whole.Index.Name != "documents_all_idx" {
		t.Fatalf("index names = %q/%q", active.Index.Name, whole.Index.Name)
	}
	if got, want := active.PopulationPredicateSQL(), `"status" = E'active'`; got != want {
		t.Fatalf("active predicate = %q, want %q (shared tenant filter must be replaced)", got, want)
	}
	if got := whole.PopulationPredicateSQL(); got != "" {
		t.Fatalf("whole-table predicate = %q, want empty", got)
	}
	if !active.Reconcile.DropUnowned || whole.Reconcile.DropUnowned {
		t.Fatalf("drop_unowned overrides = %t/%t, want true/false", active.Reconcile.DropUnowned, whole.Reconcile.DropUnowned)
	}
	if whole.Reconcile.OwnershipTag != "whole-table-tool" || whole.Report.OutputDir != "./reports/whole-table" {
		t.Fatalf("whole-table overrides = %#v/%#v", whole.Reconcile, whole.Report)
	}

	// Report labels fall back to the target name so per-target reports differ.
	if active.Report.TargetLabel != "active" || whole.Report.TargetLabel != "whole-table" {
		t.Fatalf("target labels = %q/%q, want the target names", active.Report.TargetLabel, whole.Report.TargetLabel)
	}

	// One file, two independent managed families.
	if active.SpecHash() == whole.SpecHash() {
		t.Fatal("targets on different populations share a spec hash")
	}
	if active.Population.Filters[0].Column != "status" {
		t.Fatalf("shared population slice was mutated: %#v", active.Population.Filters)
	}
}

func TestLoadTargetsHonorsExplicitSharedTargetLabel(t *testing.T) {
	path := writeTestConfig(t, `
connection:
  dsn: postgres://localhost/testdb
table:
  name: documents
  vector_column: embedding
index:
  dimensions: 128
report:
  output_dir: ./reports
  target_label: shared-label
targets:
  - name: active
    index:
      name: documents_active_idx
  - name: premium
    index:
      name: documents_premium_idx
`)
	targets, err := LoadTargets(path)
	if err != nil {
		t.Fatalf("LoadTargets() error = %v", err)
	}
	for _, target := range targets {
		if target.Report.TargetLabel != "shared-label" {
			t.Fatalf("target %s label = %q, want the shared label", target.Name, target.Report.TargetLabel)
		}
	}
}

func TestLoadTargetsRejectsInvalidTargetLists(t *testing.T) {
	const base = `
connection:
  dsn: postgres://localhost/testdb
table:
  name: documents
  vector_column: embedding
index:
  dimensions: 128
report:
  output_dir: ./reports
targets:
`
	tests := []struct {
		name     string
		contents string
		wantErr  string
	}{
		{
			name: "missing name",
			contents: base + `  - index:
      name: documents_active_idx
`,
			wantErr: "targets[0].name is required",
		},
		{
			name: "unsafe name",
			contents: base + `  - name: ../escape
    index:
      name: documents_active_idx
`,
			wantErr: "targets[0].name",
		},
		{
			name: "duplicate name",
			contents: base + `  - name: active
    index:
      name: documents_active_idx
  - name: active
    index:
      name: documents_premium_idx
`,
			wantErr: "duplicates targets[0].name",
		},
		{
			name: "case-insensitive report filename collision",
			contents: base + `  - name: active
    index:
      name: documents_active_idx
  - name: ACTIVE
    index:
      name: documents_premium_idx
`,
			wantErr: "duplicates targets[0].name",
		},
		{
			name: "duplicate managed index",
			contents: base + `  - name: active
    index:
      name: documents_idx
  - name: premium
    index:
      name: documents_idx
`,
			wantErr: "both manage public.documents_idx",
		},
		{
			name: "run-level setting inside target",
			contents: base + `  - name: active
    index:
      name: documents_active_idx
    reconcile:
      continuous: true
`,
			wantErr: "run-level settings",
		},
		{
			name: "unknown target field",
			contents: base + `  - name: active
    index:
      name: documents_active_idx
    population:
      predicates: []
`,
			wantErr: "field predicates not found",
		},
		{
			name: "invalid resolved value names the target",
			contents: base + `  - name: active
    index:
      name: documents_active_idx
  - name: premium
    index:
      name: documents_premium_idx
      metric: invalid
`,
			wantErr: "targets[1] (premium): index.metric must be one of",
		},
		{
			name: "missing dsn after merge",
			contents: `table:
  name: documents
  vector_column: embedding
index:
  name: documents_active_idx
  dimensions: 128
report:
  output_dir: ./reports
targets:
  - name: active
`,
			wantErr: "connection.dsn is required",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := LoadTargets(writeTestConfig(t, tt.contents))
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("LoadTargets() error = %v, want containing %q", err, tt.wantErr)
			}
		})
	}
}

func TestLoadRejectsMultiTargetFiles(t *testing.T) {
	_, err := Load(writeTestConfig(t, multiTargetConfig))
	if err == nil || !strings.Contains(err.Error(), "use LoadTargets") {
		t.Fatalf("Load() error = %v, want a LoadTargets hint", err)
	}
}

func TestLoadTargetsReturnsSingleTargetForLegacyFiles(t *testing.T) {
	targets, err := LoadTargets(writeTestConfig(t, minimalConfig))
	if err != nil {
		t.Fatalf("LoadTargets() error = %v", err)
	}
	if len(targets) != 1 {
		t.Fatalf("LoadTargets() returned %d targets, want 1", len(targets))
	}
	if targets[0].Name != "" || targets[0].Report.TargetLabel != "target" {
		t.Fatalf("legacy target = %#v, want unnamed target with the historical label", targets[0])
	}
}

func TestSampleMultiTargetConfigLoads(t *testing.T) {
	path := filepath.Join("..", "..", "testdata", "sample-config-multi.yaml")
	targets, err := LoadTargets(path)
	if err != nil {
		t.Fatalf("LoadTargets(sample-config-multi.yaml) error = %v", err)
	}
	if len(targets) < 2 {
		t.Fatalf("sample multi-target config defines %d targets, want at least 2", len(targets))
	}
	for _, target := range targets {
		if target.Name == "" || target.Index.Name == "" {
			t.Fatalf("sample target %#v is incomplete", target)
		}
	}
}

func TestLoadTargetsRejectsExplicitEmptyList(t *testing.T) {
	_, err := LoadTargets(writeTestConfig(t, minimalConfig+"targets: []\n"))
	if err == nil || !strings.Contains(err.Error(), "targets must not be empty") {
		t.Fatalf("LoadTargets() error = %v, want an empty-targets error", err)
	}
}

func TestLoadTargetsDistinguishesIndexNamespaces(t *testing.T) {
	for _, test := range []struct {
		name    string
		targets string
	}{
		{"dotted identifiers", `targets:
  - name: first
    table:
      schema: public.one
    index:
      name: idx
  - name: second
    table:
      schema: public
    index:
      name: one.idx
`},
		{"separate databases", `targets:
  - name: first
  - name: second
    connection:
      dsn: postgres://localhost/otherdb
`},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := LoadTargets(writeTestConfig(t, minimalConfig+test.targets)); err != nil {
				t.Fatalf("distinct index namespaces rejected: %v", err)
			}
		})
	}
}
