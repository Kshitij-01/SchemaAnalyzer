# Table Profile: {{schema}}.{{table}}

| Property | Value |
|----------|-------|
| **Source** | {{source_name}} |
| **Schema** | {{schema}} |
| **Table** | {{table}} |
| **Type** | {{table_type}} |
| **Database** | {{database}} |

---

## Columns

| # | Column Name | Data Type | Max Length | Precision | Scale | Nullable | Default | Description |
|---|-------------|-----------|-----------|-----------|-------|----------|---------|-------------|
| {{ordinal_position}} | {{column_name}} | {{data_type}} | {{character_maximum_length}} | {{numeric_precision}} | {{numeric_scale}} | {{is_nullable}} | {{column_default}} | {{description}} |

**Total Columns**: {{column_count}}

---

## Constraints

### Primary Key

| Constraint Name | Columns |
|----------------|---------|
| {{pk_constraint_name}} | {{pk_columns}} |

### Unique Constraints

| Constraint Name | Columns |
|----------------|---------|
| {{unique_constraint_name}} | {{unique_columns}} |

### Check Constraints

| Constraint Name | Check Clause |
|----------------|--------------|
| {{check_constraint_name}} | {{check_clause}} |

---

## Indexes

| Index Name | Columns | Unique | Primary | Type |
|-----------|---------|--------|---------|------|
| {{index_name}} | {{index_columns}} | {{is_unique}} | {{is_primary}} | {{index_type}} |

**Total Indexes**: {{index_count}}

---

## Foreign Keys

### Outgoing (This Table References)

| Constraint Name | Column | Referenced Schema | Referenced Table | Referenced Column |
|----------------|--------|-------------------|-----------------|-------------------|
| {{fk_constraint_name}} | {{fk_column}} | {{ref_schema}} | {{ref_table}} | {{ref_column}} |

### Incoming (Referenced By)

| Constraint Name | Source Schema | Source Table | Source Column |
|----------------|--------------|-------------|---------------|
| {{inc_constraint_name}} | {{source_schema}} | {{source_table}} | {{source_column}} |

---

## Statistics

| Metric | Value |
|--------|-------|
| **Row Count** | {{row_count}} |
| **Total Size** | {{total_size}} |
| **Data Size** | {{data_size}} |
| **Index Size** | {{index_size}} |

### Null Percentages

| Column Name | Total Rows | Null Count | Null % |
|------------|-----------|-----------|--------|
| {{column_name}} | {{total_rows}} | {{null_count}} | {{null_pct}}% |

---

## Sample Data (5 Rows)

{{sample_data_table}}

---

## Profiling Metadata

| Property | Value |
|----------|-------|
| **Profiled By** | {{profiled_by}} |
| **Model** | {{model_name}} |
| **Timestamp** | {{timestamp}} |
| **Re-Profiled** | {{re_profiled}} |
| **Re-Profile Reason** | {{re_profile_reason}} |
| **Source Connector** | {{source_type}}_connector |
| **Profiling Duration** | {{profiling_duration}} |
