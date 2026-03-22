# Master Schema: {{project_name}}

**Generated**: {{timestamp}}
**Sources Analyzed**: {{source_count}}
**Total Tables**: {{total_tables}}
**Total Columns**: {{total_columns}}
**Estimated Total Rows**: {{total_rows}}

---

## Sources Analyzed

| # | Source Name | Type | Database | Schemas | Tables | Views | Rows | Size | Quality Score |
|---|-----------|------|----------|---------|--------|-------|------|------|---------------|
| {{n}} | {{source_name}} | {{source_type}} | {{database}} | {{schema_count}} | {{table_count}} | {{view_count}} | {{row_count}} | {{size}} | {{quality_score}}/100 |

**Total Sources**: {{source_count}}

---

## Cross-Source Relationships

### Identified Cross-Source Links

These are relationships detected between tables in different data sources, based on column name matching, type compatibility, and foreign key analysis.

| # | Source A | Table A | Column A | Source B | Table B | Column B | Confidence | Basis |
|---|---------|---------|----------|---------|---------|----------|------------|-------|
| {{n}} | {{source_a}} | {{table_a}} | {{column_a}} | {{source_b}} | {{table_b}} | {{column_b}} | {{confidence}} | {{basis}} |

**Confidence Levels**:
- **HIGH**: Exact column name and type match with at least one side having a primary key.
- **MEDIUM**: Column name match with compatible (but not identical) types.
- **LOW**: Pattern-based inference (e.g., naming convention match only).

### Cross-Source Relationship Map

```
{{relationship_ascii_diagram}}
```

### Shared Entity Types

Entities that appear to exist across multiple sources:

| Entity | Sources | Tables | Notes |
|--------|---------|--------|-------|
| {{entity_name}} | {{sources_list}} | {{tables_list}} | {{notes}} |

---

## Overall Data Quality

### Quality Scores

| Source | Score | Grade | Top Issue |
|--------|-------|-------|-----------|
| {{source_name}} | {{score}}/100 | {{grade}} | {{top_issue}} |
| **Overall** | **{{overall_score}}/100** | **{{overall_grade}}** | -- |

**Grading Scale**: A (90-100), B (80-89), C (70-79), D (60-69), F (0-59)

### Quality Issue Distribution

| Issue Category | Count | Affected Tables | Severity |
|---------------|-------|----------------|----------|
| Missing Primary Keys | {{count}} | {{tables}} | HIGH |
| High Null Rate Columns | {{count}} | {{tables}} | MEDIUM |
| Orphaned Foreign Keys | {{count}} | {{tables}} | HIGH |
| Empty Tables | {{count}} | {{tables}} | LOW |
| Missing Indexes on FK Columns | {{count}} | {{tables}} | MEDIUM |
| Type Inconsistencies Across Sources | {{count}} | {{tables}} | HIGH |
| Wide Tables (>50 columns) | {{count}} | {{tables}} | LOW |
| No Documentation/Descriptions | {{count}} | {{tables}} | LOW |

### Tables Flagged for Human Review

| # | Source | Table | Reason | Flagged By |
|---|--------|-------|--------|------------|
| {{n}} | {{source_name}} | {{schema}}.{{table}} | {{reason}} | {{flagged_by}} |

---

## Architecture Observations

### Schema Design Patterns

{{schema_design_observations}}

Patterns to evaluate and comment on:
- **Normalization level**: Are tables appropriately normalized? Over-normalized? Under-normalized?
- **Naming conventions**: Are table and column names consistent? What conventions are used (snake_case, camelCase, PascalCase)?
- **Audit columns**: Do tables consistently include `created_at`, `updated_at`, `created_by`, etc.?
- **Soft deletes**: Is there a consistent pattern for soft deletes (`deleted_at`, `is_active`, etc.)?
- **Multi-tenancy**: Are there tenant isolation patterns (e.g., `tenant_id` columns)?
- **Versioning**: Are there versioning patterns (e.g., `version`, `valid_from`, `valid_to`)?
- **Enum handling**: Are enums stored as database enums, check constraints, or reference tables?

### Data Flow Architecture

{{data_flow_observations}}

Observations about:
- **Source-of-truth tables**: Which tables are the authoritative source for key entities?
- **Derived/aggregate tables**: Which tables appear to be materialized views or pre-computed aggregates?
- **ETL patterns**: Evidence of ETL processes (staging tables, batch load timestamps, etc.).
- **Replication indicators**: Tables that appear to be replicas of tables in other sources.

### Scalability Observations

{{scalability_observations}}

Observations about:
- **Large tables**: Tables with disproportionately high row counts or sizes.
- **Index coverage**: Are frequently-joined columns properly indexed?
- **Partitioning**: Evidence of table partitioning.
- **Growth patterns**: Tables likely to grow rapidly based on their structure.

---

## Recommendations

### Priority Summary

| Priority | Count | Categories |
|----------|-------|------------|
| P0 (Critical) | {{p0_count}} | {{p0_categories}} |
| P1 (High) | {{p1_count}} | {{p1_categories}} |
| P2 (Medium) | {{p2_count}} | {{p2_categories}} |
| P3 (Low) | {{p3_count}} | {{p3_categories}} |

### Top Recommendations

| # | Recommendation | Priority | Category | Affected |
|---|---------------|----------|----------|----------|
| {{n}} | {{recommendation}} | {{priority}} | {{category}} | {{affected_tables}} |

Detailed recommendations are available in `output/reports/recommendations.md`.

---

## Profiling Metadata

| Metric | Value |
|--------|-------|
| **Analysis Started** | {{analysis_start}} |
| **Analysis Completed** | {{analysis_end}} |
| **Total Duration** | {{total_duration}} |
| **Tables Profiled by Deep Agent** | {{deep_agent_profiled}} |
| **Tables Re-Profiled by Claude** | {{claude_reprofiled}} |
| **Tables Failed** | {{tables_failed}} |
| **Tables Flagged for Review** | {{tables_flagged}} |
| **Total Re-Profile Requests** | {{total_reprofile_requests}} |
| **Deep Agent Model** | {{deep_agent_model}} |
| **Orchestrator Model** | {{orchestrator_model}} |

---

## File Index

All generated files for this analysis run:

```
output/
  master_schema.md                          <- This file
  sources/
{{source_file_tree}}
  analysis/
    relationships.md
    quality_audit.md
    lineage.md
  reports/
    executive_summary.md
    data_dictionary.md
    recommendations.md
  context/
    plan.md
    progress.md
    discovery/
    agent_comms/
    feedback/
```
