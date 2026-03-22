# SchemaAnalyzer — Architecture Proposal

> An agentic schema discovery & analysis system built on Claude Agent SDK
> Applies Deep Agent patterns for multi-phase, file-driven workflows

---

## High-Level Vision

```
┌─────────────────────────────────────────────────────────────────────┐
│                        SchemaAnalyzer CLI                           │
│                     (Python entry point)                            │
│                                                                     │
│  User provides: connection configs (YAML/JSON/ENV)                  │
│  System outputs: structured MD reports in output/                   │
└──────────────────────────────┬──────────────────────────────────────┘
                               │
                    ┌──────────▼──────────┐
                    │   ORCHESTRATOR      │
                    │   (ClaudeSDKClient) │
                    │   model: opus       │
                    │                     │
                    │   Owns the plan     │
                    │   Manages phases    │
                    │   Routes to agents  │
                    └──┬──────┬───────┬───┘
                       │      │       │
           ┌───────────▼┐  ┌──▼─────┐ ┌▼────────────┐
           │ DISCOVERY   │  │ANALYSIS│ │ REPORT       │
           │ AGENT       │  │ AGENT  │ │ AGENT        │
           │ model:sonnet│  │ sonnet │ │ sonnet/haiku │
           │             │  │        │ │              │
           │ Connects to │  │Reads   │ │ Reads all    │
           │ data sources│  │schemas │ │ findings     │
           │ Writes      │  │Finds   │ │ Writes final │
           │ schema MDs  │  │patterns│ │ reports      │
           └──────┬──────┘  └───┬────┘ └──────┬──────┘
                  │             │              │
                  ▼             ▼              ▼
         output/schemas/  output/analysis/ output/reports/
```

---

## Phase-Based Workflow (Deep Agent Pattern)

### Phase 1: PLAN
**Owner**: Orchestrator

```
Input:  connections.yaml (user-provided data source configs)
Action: Parse configs, validate connectivity, create task plan
Output: output/plan.md (todo list of what to discover/analyze)
```

- Read connection configs
- Test connectivity to each source (via MCP tools)
- Write a structured plan: which sources, what order, what depth
- Create todos for each data source

### Phase 2: DISCOVER
**Owner**: Discovery Agent (one per data source, can run in parallel)

```
Input:  Connection config for one data source
Action: Connect, introspect, extract all schema metadata
Output: output/schemas/{source_name}/*.md
```

For each data source:
- Connect via appropriate MCP connector
- Extract: databases, schemas, tables, columns, types, constraints
- Extract: indexes, views, stored procedures, functions
- Extract: row counts, sample data (optional)
- Write structured MD files per table/view

**Output structure:**
```
output/schemas/
  postgres_main/
    _overview.md          # Summary: 3 schemas, 47 tables, 12 views
    public/
      users.md            # Full table definition
      orders.md
      _relationships.md   # FK relationships within this schema
    analytics/
      daily_metrics.md
      _relationships.md
  snowflake_warehouse/
    _overview.md
    raw/
      events.md
    curated/
      dim_customers.md
```

### Phase 3: ANALYZE
**Owner**: Analysis Agent

```
Input:  output/schemas/**/*.md (all discovered schemas)
Action: Deep analysis — patterns, relationships, quality, issues
Output: output/analysis/*.md
```

Analysis tasks:
- **Cross-source relationships**: Same entities across different DBs
- **Data lineage hints**: Which tables feed which (naming patterns, column overlap)
- **Schema quality**: Missing PKs, no indexes, nullable FKs, orphan tables
- **Naming conventions**: Consistency analysis across sources
- **Type mismatches**: Same logical column with different types across sources
- **Redundancy detection**: Duplicate or near-duplicate tables

**Output:**
```
output/analysis/
  cross_source_relationships.md
  data_quality_issues.md
  naming_conventions.md
  type_mismatches.md
  redundancy_report.md
  lineage_hints.md
```

### Phase 4: SYNTHESIZE & REPORT
**Owner**: Report Agent

```
Input:  output/schemas/ + output/analysis/
Action: Generate comprehensive final report
Output: output/reports/
```

**Output:**
```
output/reports/
  executive_summary.md       # High-level overview for stakeholders
  full_schema_catalog.md     # Complete catalog of all discovered schemas
  relationship_map.md        # All relationships (intra + cross-source)
  recommendations.md         # Actionable improvement suggestions
  data_dictionary.md         # Column-level dictionary across all sources
```

### Phase 5: VERIFY
**Owner**: Orchestrator

```
Input:  output/reports/ + original connection configs
Action: Verify completeness — all sources covered, no gaps
Output: output/verification.md (pass/fail + gaps)
```

---

## Project Structure

```
SchemaAnalyzer/
├── pyproject.toml                 # Project config, dependencies
├── README.md
├── .gitignore
│
├── src/
│   └── schema_analyzer/
│       ├── __init__.py
│       ├── main.py                # CLI entry point
│       ├── orchestrator.py        # Main orchestrator (ClaudeSDKClient)
│       │
│       ├── agents/                # Agent definitions
│       │   ├── __init__.py
│       │   ├── discovery.py       # Discovery agent config
│       │   ├── analysis.py        # Analysis agent config
│       │   └── report.py          # Report agent config
│       │
│       ├── connectors/            # MCP servers for data sources
│       │   ├── __init__.py
│       │   ├── base.py            # Base connector interface
│       │   ├── postgres.py        # Postgres MCP server
│       │   ├── snowflake.py       # Snowflake MCP server
│       │   ├── mysql.py           # MySQL MCP server
│       │   ├── azure_blob.py      # Azure Blob Storage MCP server
│       │   ├── bigquery.py        # BigQuery MCP server
│       │   └── registry.py        # Connector registry (auto-detect source type)
│       │
│       ├── hooks/                 # Hook callbacks
│       │   ├── __init__.py
│       │   ├── safety.py          # Block destructive SQL
│       │   ├── progress.py        # Track agent progress
│       │   └── audit.py           # Log all tool invocations
│       │
│       ├── prompts/               # System prompts for agents
│       │   ├── orchestrator.md
│       │   ├── discovery.md
│       │   ├── analysis.md
│       │   └── report.md
│       │
│       └── config/                # Configuration
│           ├── __init__.py
│           ├── models.py          # Pydantic models for connections
│           └── loader.py          # YAML/JSON config loader
│
├── templates/                     # MD templates for output
│   ├── table_schema.md
│   ├── overview.md
│   ├── relationship_map.md
│   └── executive_summary.md
│
├── research/                      # Research notes (what we wrote)
│   ├── claude-agent-sdk.md
│   ├── deep-agents.md
│   └── architecture-proposal.md
│
├── docs/                          # Cloned reference repos (gitignored)
│
├── tests/
│   ├── test_connectors/
│   ├── test_agents/
│   └── test_hooks/
│
└── output/                        # Generated output (gitignored)
    ├── plan.md
    ├── schemas/
    ├── analysis/
    └── reports/
```

---

## Connector Architecture (MCP Servers)

Each connector is an SDK MCP server with standardized tools:

### Base Interface (every connector implements)

```python
# Every connector exposes these tools:
@tool("test_connection")       # Verify connectivity
@tool("list_databases")        # List available databases
@tool("list_schemas")          # List schemas in a database
@tool("list_tables")           # List tables in a schema
@tool("describe_table")        # Full table definition (columns, types, constraints)
@tool("list_views")            # List views
@tool("describe_view")         # View definition + underlying query
@tool("list_indexes")          # Indexes on a table
@tool("list_foreign_keys")     # FK relationships
@tool("get_row_count")         # Approximate row count
@tool("sample_data")           # Sample N rows (for type inference)
@tool("raw_query")             # Execute arbitrary read-only SQL
```

### Connector Registry

```python
CONNECTORS = {
    "postgres": PostgresConnector,
    "snowflake": SnowflakeConnector,
    "mysql": MySQLConnector,
    "azure_blob": AzureBlobConnector,
    "bigquery": BigQueryConnector,
}

def create_connectors(config: dict) -> dict[str, McpServer]:
    """Auto-create MCP servers from connection configs."""
    servers = {}
    for name, conn_config in config["connections"].items():
        connector_class = CONNECTORS[conn_config["type"]]
        servers[name] = connector_class.create_mcp_server(conn_config)
    return servers
```

### Connection Config Format

```yaml
# connections.yaml
connections:
  main_postgres:
    type: postgres
    host: localhost
    port: 5432
    database: myapp
    username: ${POSTGRES_USER}
    password: ${POSTGRES_PASSWORD}

  analytics_snowflake:
    type: snowflake
    account: myorg-account
    warehouse: COMPUTE_WH
    database: ANALYTICS
    username: ${SNOWFLAKE_USER}
    password: ${SNOWFLAKE_PASSWORD}

  raw_storage:
    type: azure_blob
    account_name: mystorageaccount
    container: raw-data
    connection_string: ${AZURE_STORAGE_CONNECTION_STRING}

settings:
  discovery_depth: deep          # shallow | deep
  sample_rows: 5                 # rows to sample per table
  max_concurrent_sources: 3      # parallel discovery agents
  output_dir: ./output
```

---

## Safety System

### Layer 1: Read-Only SQL Enforcement

```python
ALLOWED_SQL_PREFIXES = ["SELECT", "SHOW", "DESCRIBE", "EXPLAIN",
                        "INFORMATION_SCHEMA", "WITH"]

async def sql_safety_hook(input_data, tool_use_id, context):
    sql = input_data.get("tool_input", {}).get("sql", "").strip().upper()
    if not any(sql.startswith(prefix) for prefix in ALLOWED_SQL_PREFIXES):
        return {
            "hookSpecificOutput": {
                "permissionDecision": "deny",
                "permissionDecisionReason": f"Blocked non-read SQL: {sql[:50]}"
            }
        }
```

### Layer 2: Connection Credential Isolation

- Credentials loaded from env vars (never in prompts)
- Agents receive connection **names**, not raw credentials
- MCP servers handle credential injection internally

### Layer 3: Output Sanitization

- Hooks check that no credentials leak into output MD files
- Sample data is truncated (no PII in reports)

---

## MD File Strategy (Information Transfer)

### Why MD Files Are Critical

In Claude Agent SDK, agents communicate through the filesystem:
- Discovery agent **writes** `schemas/postgres/users.md`
- Analysis agent **reads** that file, **writes** `analysis/quality.md`
- Report agent **reads** everything, **writes** `reports/summary.md`

This is the Deep Agent "context offloading" pattern — large data goes to files, not messages.

### MD File Standards

Every MD file follows a consistent structure:

```markdown
# {Title}

> Source: {data_source_name}
> Generated: {timestamp}
> Agent: {agent_name}
> Phase: {discovery|analysis|report}

---

## Metadata
- **Type**: {table|view|schema|analysis|report}
- **Database**: {db_name}
- **Schema**: {schema_name}

---

## Content

{structured content here}

---

## Cross-References
- Related: [other_table.md](../other_table.md)
- Analysis: [quality.md](../../analysis/quality.md)
```

### Template System

Templates in `templates/` define the structure. Agents are instructed (via system prompts) to follow these templates exactly — ensuring consistency across all output files.

---

## Orchestrator Flow (Pseudocode)

```python
async def run_schema_analyzer(config_path: str):
    # 1. Load config
    config = load_config(config_path)
    connectors = create_connectors(config)

    # 2. Create agent options
    options = ClaudeAgentOptions(
        system_prompt=load_prompt("orchestrator.md"),
        model="opus",
        mcp_servers=connectors,
        agents={
            "discovery": create_discovery_agent(connectors),
            "analysis": create_analysis_agent(),
            "report": create_report_agent(),
        },
        hooks={
            "PreToolUse": [sql_safety_hook, audit_hook],
            "SubagentStart": [progress_hook],
            "SubagentStop": [progress_hook],
        },
        allowed_tools=["Read", "Write", "Edit", "Bash", "Glob", "Grep",
                       *[f"mcp__{name}__*" for name in connectors]],
        permission_mode="acceptEdits",
        max_budget_usd=10.0,
        cwd=config.output_dir,
    )

    # 3. Run orchestrator
    prompt = f"""
    Analyze all data sources defined in the connection config.

    Phase 1 - PLAN: Read the config, test all connections, write plan.md
    Phase 2 - DISCOVER: Use discovery agent for each source (parallel OK)
    Phase 3 - ANALYZE: Use analysis agent on all discovered schemas
    Phase 4 - REPORT: Use report agent to generate final reports
    Phase 5 - VERIFY: Check all sources are covered, no gaps

    Connection config:
    {yaml.dump(config.connections)}

    Write all output to the current directory following the MD templates.
    """

    async for message in query(prompt=prompt, options=options):
        handle_message(message)
```

---

## Technology Stack

| Component | Technology | Reason |
|---|---|---|
| Runtime | Python 3.11+ | Claude Agent SDK is Python-native |
| Agent Framework | `claude-agent-sdk` | Official SDK, full tool access |
| DB Connectors | `asyncpg`, `snowflake-connector-python`, `azure-storage-blob`, `google-cloud-bigquery` | Native async drivers |
| MCP Servers | SDK MCP (in-process) | No subprocess overhead |
| Config | PyYAML + Pydantic | Type-safe config loading |
| CLI | `click` or `typer` | User-friendly CLI |
| Package Manager | `uv` | Fast, modern Python packaging |
| Testing | `pytest` + `pytest-asyncio` | Async test support |

---

## Build Order (Implementation Phases)

### Sprint 1: Foundation
- [ ] Project scaffolding (pyproject.toml, src/, etc.)
- [ ] Config loader (YAML → Pydantic models)
- [ ] Base connector interface
- [ ] Postgres connector (first MCP server)
- [ ] Basic orchestrator with single-agent discovery

### Sprint 2: Multi-Source
- [ ] Snowflake connector
- [ ] Azure Blob connector
- [ ] MySQL connector
- [ ] Connector registry (auto-detection)
- [ ] Parallel discovery (multiple sources)

### Sprint 3: Analysis
- [ ] Analysis agent (relationship detection, quality checks)
- [ ] Cross-source relationship mapping
- [ ] MD template system
- [ ] Report agent

### Sprint 4: Polish
- [ ] Safety hooks (SQL enforcement, credential isolation)
- [ ] Progress tracking (hook-based)
- [ ] CLI interface
- [ ] Error handling & retry logic
- [ ] Testing suite

### Sprint 5: Advanced
- [ ] BigQuery connector
- [ ] S3 connector
- [ ] Data lineage analysis
- [ ] Incremental re-analysis (only changed schemas)
- [ ] HTML report generation (optional)

---

## Open Questions

1. **Should we use `query()` or `ClaudeSDKClient` for the orchestrator?**
   - `query()` is simpler but one-shot
   - `ClaudeSDKClient` allows multi-turn (can ask follow-up questions)
   - **Recommendation**: Start with `query()`, upgrade to `ClaudeSDKClient` if we need interactivity

2. **How to handle very large schemas (1000+ tables)?**
   - Batch discovery (100 tables per agent call)
   - Summarize before passing to analysis
   - Use `max_turns` to prevent runaway costs

3. **Should the analysis agent use an LLM or rule-based logic?**
   - LLM for fuzzy pattern matching (naming conventions, semantic relationships)
   - Rule-based for deterministic checks (missing PKs, type mismatches)
   - **Recommendation**: Hybrid — LLM agent with rule-based MCP tools

4. **Cost management?**
   - `max_budget_usd` per run
   - Use `haiku` for simple extraction, `sonnet` for analysis, `opus` for orchestration
   - Track cost in `ResultMessage` and report in final output
