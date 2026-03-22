# Source Summary: {{source_name}}

**Generated**: {{timestamp}}

---

## Connection Information

| Property | Value |
|----------|-------|
| **Source Name** | {{source_name}} |
| **Source Type** | {{source_type}} |
| **Host** | {{host}} |
| **Port** | {{port}} |
| **Database** | {{database}} |
| **User** | {{user}} |
| **Password** | `***` |
| **Connection Status** | {{connection_status}} |

---

## High-Level Statistics

| Metric | Value |
|--------|-------|
| **Total Schemas** | {{total_schemas}} |
| **Total Tables** | {{total_tables}} |
| **Total Views** | {{total_views}} |
| **Total Columns** | {{total_columns}} |
| **Estimated Total Rows** | {{total_rows}} |
| **Estimated Total Size** | {{total_size}} |
| **Estimated Data Size** | {{total_data_size}} |
| **Estimated Index Size** | {{total_index_size}} |

---

## Schema Overview

### Schemas

| Schema | Tables | Views | Total Objects |
|--------|--------|-------|---------------|
| {{schema_name}} | {{table_count}} | {{view_count}} | {{object_count}} |

### All Tables

| # | Schema | Table | Type | Columns | Row Count | Size | Has PK | Has FK |
|---|--------|-------|------|---------|-----------|------|--------|--------|
| {{n}} | {{schema}} | {{table}} | {{type}} | {{col_count}} | {{row_count}} | {{size}} | {{has_pk}} | {{has_fk}} |

---

## Key Relationships

### Foreign Key Summary

| # | Source Table | Source Column | Target Table | Target Column | Constraint |
|---|-------------|---------------|-------------|---------------|------------|
| {{n}} | {{source_schema}}.{{source_table}} | {{source_column}} | {{target_schema}}.{{target_table}} | {{target_column}} | {{constraint_name}} |

**Total Foreign Keys**: {{total_fk_count}}

### Relationship Clusters

Tables that are heavily interconnected within this source:

| Cluster | Tables | Relationship Count |
|---------|--------|--------------------|
| {{cluster_name}} | {{tables_in_cluster}} | {{relationship_count}} |

---

## Data Quality Flags

### Critical Issues

| # | Table | Issue | Details |
|---|-------|-------|---------|
| {{n}} | {{schema}}.{{table}} | {{issue_type}} | {{details}} |

Issue types to check for:
- `NO_PRIMARY_KEY` -- Table has no primary key defined.
- `HIGH_NULL_RATE` -- One or more columns have >90% null values.
- `ORPHANED_FK` -- Foreign key references a table not found in this source.
- `EMPTY_TABLE` -- Table has 0 rows.
- `MISSING_INDEX` -- Columns used in foreign keys have no supporting index.
- `WIDE_TABLE` -- Table has more than 50 columns.

### Quality Summary

| Metric | Value |
|--------|-------|
| Tables with no PK | {{no_pk_count}} |
| Tables with >90% null columns | {{high_null_count}} |
| Empty tables | {{empty_table_count}} |
| Orphaned foreign keys | {{orphaned_fk_count}} |
| Tables without indexes | {{no_index_count}} |

---

## Profiling Report

### Model Usage

| Metric | Value |
|--------|-------|
| **Total Tables Profiled** | {{total_profiled}} |
| **Profiled by Deep Agent** | {{deep_agent_count}} ({{deep_agent_model}}) |
| **Re-Profiled by Claude** | {{claude_reprofile_count}} |
| **Failed (Unrecoverable)** | {{failed_count}} |
| **Profiling Start Time** | {{profiling_start}} |
| **Profiling End Time** | {{profiling_end}} |
| **Total Profiling Duration** | {{profiling_duration}} |

### Re-Profiled Tables

| # | Table | Reason | Attempt | Result |
|---|-------|--------|---------|--------|
| {{n}} | {{schema}}.{{table}} | {{reason}} | {{attempt_number}} | {{result}} |

### Failed Tables

| # | Table | Error | Notes |
|---|-------|-------|-------|
| {{n}} | {{schema}}.{{table}} | {{error}} | {{notes}} |

---

## Batching Details

| Batch | Tables | Start Time | End Time | Duration | Status |
|-------|--------|-----------|----------|----------|--------|
| {{batch_number}} | {{table_list}} | {{start}} | {{end}} | {{duration}} | {{status}} |

---

## Notes

{{additional_notes}}
