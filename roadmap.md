# SchemaAnalyzer — Roadmap

> Agentic schema discovery, profiling & analysis system
> Built on **Claude Agent SDK** (orchestration) + **LangChain Deep Agents** (cheap sub-agents)

---

## 1. System Overview

```
User provides:
  - Natural text with credentials (or .env file upload)
  - Brief task description ("analyze my postgres and snowflake schemas")

System produces:
  - Per-table MD files (full profile)
  - Per-source summary MDs (rollup)
  - Master schema MD (the ultimate file)
  - Relationship maps, quality audits, lineage traces
  - Full context trail (every decision, every re-profile, every Q&A)
```

---

## 2. Architecture — Hybrid Agent Stack

### Why Hybrid?

| Concern | Solution |
|---------|----------|
| Claude Agent SDK has built-in Read/Write/Edit/Bash/Agent tools | Use for orchestration, analysis, reporting |
| Table profiling is grunt work (500+ tables) — expensive on Claude | Delegate to Deep Agents running cheap models |
| Deep Agents (LangGraph) support `init_chat_model()` — any LLM | Route to DeepSeek, Kimi, gpt-oss on Azure AI Foundry |
| Context windows fill up on large schemas | Sub-agents get isolated context, write results to MD files |

### Agent Hierarchy

```
┌──────────────────────────────────────────────────────────────────┐
│                    CLAUDE AGENT SDK LAYER                         │
│                    (Claude models — smart, expensive)             │
│                                                                   │
│  ORCHESTRATOR (ClaudeSDKClient, claude-opus/sonnet)               │
│  ├── Tools: Read, Write, Edit, Bash, Glob, Grep, Agent           │
│  ├── Reads user input, parses creds, creates plan                 │
│  ├── Spawns Discovery/Analysis/Report agents                      │
│  └── Writes master_schema.md at the end                           │
│       │                                                           │
│       ├── DISCOVERY AGENT (Claude sonnet)                         │
│       │   ├── Tools: Read, Write, Edit, Bash, Glob, Grep, Agent  │
│       │   ├── Connects to source, lists all tables/schemas        │
│       │   ├── Decides batch sizes based on table count            │
│       │   ├── Spawns Deep Agent profilers (cheap models) via Bash │
│       │   ├── Reads all table MDs when profilers finish           │
│       │   ├── Validates output quality                            │
│       │   ├── Re-profiles with Claude if quality is bad           │
│       │   └── Writes _summary.md for this source                  │
│       │       │                                                   │
│       │       │  ┌─────────────────────────────────────────────┐  │
│       │       │  │         DEEP AGENT LAYER                    │  │
│       │       │  │    (Cheap models — DeepSeek, Kimi, etc.)    │  │
│       │       │  │                                             │  │
│       │       └──┤  TABLE PROFILER (deep agent, batch of 25)   │  │
│       │          │  ├── Model: DeepSeek-V3.1 or Kimi-K2.5      │  │
│       │          │  ├── Tools: write_file, execute (SQL)        │  │
│       │          │  ├── Queries information_schema               │  │
│       │          │  ├── Runs sample queries, null checks         │  │
│       │          │  └── Writes one MD per table                  │  │
│       │          │                                              │  │
│       │          │  TABLE PROFILER (deep agent, next batch)     │  │
│       │          │  └── Same pattern, different tables           │  │
│       │          └─────────────────────────────────────────────┘  │
│       │                                                           │
│       ├── ANALYSIS AGENT (Claude sonnet)                          │
│       │   ├── Tools: Read, Write, Edit, Bash, Glob, Grep, Agent  │
│       │   ├── Reads all _summary.md files + master_schema.md      │
│       │   ├── Spawns specialist sub-agents:                       │
│       │   │   ├── Relationship Mapper Agent                       │
│       │   │   ├── Quality Auditor Agent                           │
│       │   │   └── Lineage Tracer Agent                            │
│       │   ├── FEEDBACK LOOP: if discrepancy found                 │
│       │   │   ├── Writes question to context/agent_comms/         │
│       │   │   ├── Triggers re-profile of specific table (Claude)  │
│       │   │   ├── Reads updated MD                                │
│       │   │   └── Continues analysis                              │
│       │   └── Writes analysis MDs                                 │
│       │                                                           │
│       └── REPORT AGENT (Claude sonnet/haiku)                      │
│           ├── Tools: Read, Write, Edit, Glob, Grep                │
│           ├── Reads all analysis + schema + context files          │
│           └── Writes final reports                                │
└──────────────────────────────────────────────────────────────────┘
```

### How Claude Spawns Deep Agents

The Discovery Agent (Claude) runs a Python script via **Bash** that launches a LangGraph deep agent:

```python
# deep_profiler.py — launched by Claude via Bash
from deepagents import create_deep_agent
from langchain.chat_models import init_chat_model

model = init_chat_model(
    model_provider="azure_ai",
    model="DeepSeek-V3.1",
    api_key=os.environ["AZURE_AI_KEY"],
    base_url=os.environ["AZURE_AI_ENDPOINT"],
)

agent = create_deep_agent(
    model=model,
    tools=[query_db_tool],
    system_prompt="You are a table profiler. For each table, query the schema and write a detailed MD file.",
    backend=FilesystemBackend(root_dir="output/schemas/postgres/public/"),
)

agent.invoke({"messages": [{"role": "user", "content": f"Profile these tables: {table_list}"}]})
```

Claude's Discovery Agent calls this via:
```
Bash: python deep_profiler.py --source postgres --tables "users,orders,products,..." --model "DeepSeek-V3.1"
```

Then reads the output:
```
Glob: output/schemas/postgres/public/*.md
Read: each file, validate quality
```

---

## 3. The MD File Pyramid

### Three Layers

```
                    ┌─────────────────────┐
                    │  master_schema.md    │  Layer 3: THE ultimate file
                    │  (all sources)       │  Written by: Orchestrator
                    └──────────┬──────────┘
                               │
              ┌────────────────┼────────────────┐
              ▼                ▼                 ▼
     ┌────────────┐   ┌────────────┐   ┌────────────┐
     │ _summary.md│   │ _summary.md│   │ _summary.md│  Layer 2: Source rollups
     │ (postgres) │   │(snowflake) │   │(azure blob)│  Written by: Discovery Agent
     └─────┬──────┘   └─────┬──────┘   └─────┬──────┘
           │                 │                 │
     ┌─────┼─────┐    ┌─────┼─────┐    ┌─────┼─────┐
     ▼     ▼     ▼    ▼     ▼     ▼    ▼     ▼     ▼
   users orders prods events dims    blob1 blob2 blob3  Layer 1: Table MDs
    .md   .md    .md   .md   .md     .md   .md   .md   Written by: Deep Agent Profilers
```

### Layer 1 — Table MD (written by Deep Agent on cheap model)

```markdown
# Table: public.users

## Source
- **Database**: postgres (sqltosnowflake.postgres.database.azure.com)
- **Schema**: public
- **Type**: TABLE

## Columns
| # | Name       | Type         | Nullable | Default        | Description |
|---|------------|------------- |----------|----------------|-------------|
| 1 | id         | uuid         | NO       | gen_random_uuid() | Primary key |
| 2 | email      | varchar(255) | NO       | —              | Unique email |
| 3 | name       | varchar(100) | YES      | —              | Display name |
| 4 | created_at | timestamptz  | NO       | now()          | Creation time |

## Constraints
- **PK**: id
- **UNIQUE**: email
- **CHECK**: email ~* '^.+@.+\..+$'

## Indexes
| Name              | Columns | Type   | Unique |
|-------------------|---------|--------|--------|
| users_pkey        | id      | btree  | YES    |
| users_email_idx   | email   | btree  | YES    |

## Foreign Keys
- **Outgoing**: none
- **Incoming**: orders.user_id → users.id, payments.user_id → users.id

## Statistics
- **Row count**: 1,247,832
- **Table size**: 245 MB
- **Null percentages**: name (12.3%), all others 0%

## Sample Data (5 rows)
| id | email | name | created_at |
|----|-------|------|------------|
| 550e8400-... | alice@example.com | Alice | 2024-01-15 |
| ... | ... | ... | ... |

## Profiling Metadata
- **Profiled by**: DeepSeek-V3.1 (deep agent)
- **Profiled at**: 2026-03-22T14:30:00Z
- **Re-profiled**: No
```

### Layer 2 — Source Summary (written by Discovery Agent on Claude)

```markdown
# Source Summary: PostgreSQL (sqltosnowflake)

## Connection
- **Host**: sqltosnowflake.postgres.database.azure.com
- **Database**: postgres
- **Schemas discovered**: public, analytics, staging

## Statistics
- **Total tables**: 47
- **Total views**: 12
- **Total rows**: ~15.2M
- **Total size**: 3.4 GB

## Schema Overview

### public (32 tables)
| Table | Rows | Size | Key Relationships |
|-------|------|------|-------------------|
| users | 1.2M | 245MB | → orders, payments, sessions |
| orders | 3.4M | 890MB | → users, products, payments |
| products | 45K | 12MB | → orders, categories |
| ... | ... | ... | ... |

### analytics (10 tables)
| Table | Rows | Size | Notes |
|-------|------|------|-------|
| ... | ... | ... | ... |

## Key Relationships (within source)
- users.id → orders.user_id (1:many)
- products.id → orders.product_id (1:many)
- ... (all FK relationships)

## Data Quality Flags
- 3 tables have no primary key
- users.name has 12.3% nulls
- orders.shipped_at has 45% nulls (expected for pending orders?)

## Profiling Report
- Tables profiled by Deep Agent: 47/47 (DeepSeek-V3.1)
- Tables re-profiled by Claude: 3 (quality issues in initial profile)
- Total profiling time: 4m 32s
```

### Layer 3 — Master Schema (written by Orchestrator)

```markdown
# Master Schema: SchemaAnalyzer Report

## Sources Analyzed
| Source | Type | Tables | Rows | Size |
|--------|------|--------|------|------|
| PostgreSQL (sqltosnowflake) | RDBMS | 47 | 15.2M | 3.4GB |
| Snowflake (MIGRATION_DB) | Cloud DW | 23 | 8.7M | — |
| Azure Blob (raw-data) | Object Store | 15 files | — | 1.2GB |

## Cross-Source Relationships
- postgres.users.email ↔ snowflake.dim_customers.email_address (shared key)
- postgres.orders → snowflake.fact_sales (ETL pipeline detected)
- azure_blob/events_*.parquet → snowflake.raw.events (ingestion pattern)

## Overall Data Quality Score: 7.8/10
- ...

## Architecture Observations
- ...

## Recommendations
- ...
```

---

## 4. Feedback Loop & Context System

### The Re-Profile Loop

```
Deep Agent profiles table (cheap model)
         │
         ▼
Discovery Agent validates output ──── good? ──── ✓ move on
         │
         no (missing columns, wrong types, incomplete)
         │
         ▼
Discovery Agent re-profiles WITH CLAUDE (expensive but accurate)
         │
         ▼
         ✓ writes updated MD + logs re-profile in context/feedback/
```

### The Analysis Feedback Loop

```
Analysis Agent (Linker) reads table MDs
         │
         ▼
Finds discrepancy (type mismatch, missing FK, etc.)
         │
         ▼
Writes question: context/agent_comms/linker_to_discovery_001.md
         │
         ▼
Triggers re-profile of SPECIFIC table (Claude, not cheap model)
         │
         ▼
Discovery Agent re-profiles → updates table MD → writes response
         │
         ▼
Writes answer: context/agent_comms/discovery_to_linker_001.md
         │
         ▼
Linker Agent reads answer + updated MD → continues
```

### The Context Storage System

```
output/
├── schemas/                          # Layer 1: Table MDs
│   ├── postgres/
│   │   ├── public/
│   │   │   ├── users.md
│   │   │   ├── orders.md
│   │   │   └── ...
│   │   └── _summary.md              # Layer 2
│   ├── snowflake/
│   │   ├── public/
│   │   │   └── ...
│   │   └── _summary.md
│   └── azure_blob/
│       └── _summary.md
│
├── master_schema.md                  # Layer 3
│
├── analysis/
│   ├── relationships.md              # All FK/join relationships
│   ├── cross_source_links.md         # Cross-source connections
│   ├── data_quality.md               # Quality audit results
│   └── lineage.md                    # Data flow patterns
│
├── context/                          # FULL AUDIT TRAIL
│   ├── plan.md                       # Orchestrator's plan (updated as work progresses)
│   ├── discovery/
│   │   ├── postgres_session_001.md   # Initial discovery log
│   │   ├── postgres_session_002.md   # Re-profile session
│   │   └── decisions.md              # Why certain choices were made
│   ├── analysis/
│   │   ├── linking_session_001.md
│   │   └── quality_session_001.md
│   ├── feedback/                     # Re-profile triggers & resolutions
│   │   ├── orders_reprofile_001.md   # Why orders was re-profiled
│   │   └── users_clarification_001.md
│   ├── agent_comms/                  # Inter-agent Q&A
│   │   ├── linker_to_discovery_001.md
│   │   ├── discovery_to_linker_001.md
│   │   └── ...
│   └── cost_tracking.md             # Token usage per agent, per model
│
└── reports/                          # Final deliverables
    ├── executive_summary.md
    ├── data_dictionary.md
    └── recommendations.md
```

---

## 5. Available Models (Tested & Working)

### Claude Models (via Anthropic Azure) — for smart agents

| Model | Role | Cost Tier |
|-------|------|-----------|
| claude-opus-4-5 | Orchestrator (if complex) | $$$ |
| claude-sonnet-4-5 | Discovery, Analysis, Report agents | $$ |
| claude-haiku-4-5 | Report sub-agents, simple tasks | $ |

### Cheap Models (via Azure AI Foundry) — for deep agent profilers

| Model | Type | Best For |
|-------|------|----------|
| DeepSeek-V3.1 | Chat (fast, no reasoning) | Table profiling — fast & cheap |
| DeepSeek-V3.2-Speciale | Chat | Table profiling — latest variant |
| Kimi-K2.5 | Reasoning | Profiling tasks needing more thought |
| Kimi-K2-Thinking | Reasoning | Complex schema interpretation |
| gpt-oss-120b | Reasoning | Fallback profiler |
| DeepSeek-R1 | Reasoning (heavy) | Complex re-profile tasks |
| gpt-5.1 (Azure OpenAI) | Reasoning | Premium fallback |

### Model Routing Strategy

```
Task                          → Model
─────────────────────────────────────────────
Orchestration & planning      → claude-sonnet-4-5
Table profiling (batch)       → DeepSeek-V3.1 (cheapest, fastest)
Table profiling (re-profile)  → claude-sonnet-4-5 (accuracy matters)
Relationship analysis         → claude-sonnet-4-5
Quality audit                 → claude-haiku-4-5
Report writing                → claude-sonnet-4-5
Simple sub-tasks              → DeepSeek-V3.2 or Kimi-K2.5
```

---

## 6. Project Structure

```
SchemaAnalyzer/
├── roadmap.md                    # This file
├── env_info_clean.json           # All credentials & model configs
│
├── src/
│   ├── __init__.py
│   ├── main.py                   # Entry point — ClaudeSDKClient setup
│   ├── orchestrator.py           # Orchestrator agent definition & system prompt
│   ├── agents/
│   │   ├── __init__.py
│   │   ├── discovery.py          # Discovery agent definition
│   │   ├── analysis.py           # Analysis agent definition
│   │   └── report.py             # Report agent definition
│   ├── deep_agents/
│   │   ├── __init__.py
│   │   ├── table_profiler.py     # Deep agent: profiles tables (cheap model)
│   │   ├── model_router.py       # Picks cheapest working model from config
│   │   └── connector_scripts/    # Python scripts for each source type
│   │       ├── postgres_connector.py
│   │       ├── snowflake_connector.py
│   │       ├── azure_blob_connector.py
│   │       ├── mysql_connector.py
│   │       ├── bigquery_connector.py
│   │       └── s3_connector.py
│   ├── prompts/
│   │   ├── orchestrator_system.md    # System prompt for orchestrator
│   │   ├── discovery_system.md       # System prompt for discovery agent
│   │   ├── analysis_system.md        # System prompt for analysis agent
│   │   ├── report_system.md          # System prompt for report agent
│   │   ├── table_profiler_system.md  # System prompt for deep agent profiler
│   │   └── templates/
│   │       ├── table_md_template.md  # Template for table MD files
│   │       ├── summary_template.md   # Template for _summary.md
│   │       └── master_template.md    # Template for master_schema.md
│   ├── tools/
│   │   ├── __init__.py
│   │   ├── db_query.py           # In-process MCP tool: execute SQL
│   │   └── schema_inspect.py     # In-process MCP tool: inspect schema
│   └── utils/
│       ├── __init__.py
│       ├── config_parser.py      # Parse natural text / env files for creds
│       ├── cost_tracker.py       # Track token usage per agent per model
│       └── md_validator.py       # Validate MD file completeness
│
├── output/                       # Generated at runtime (gitignored)
│   ├── schemas/
│   ├── analysis/
│   ├── context/
│   └── reports/
│
├── docs/                         # Cloned reference repos
│   ├── claude-agent-sdk-python/
│   ├── claude-agent-sdk-typescript/
│   ├── deepagents/
│   ├── deep-agents-from-scratch/
│   └── open-deep-research/
│
├── tests/
│   ├── test_connectors.py
│   ├── test_deep_profiler.py
│   └── test_model_router.py
│
├── research/                     # Existing research docs
│   ├── agent-sdk-and-deep-agents.md
│   ├── architecture-proposal.md
│   ├── claude-agent-sdk.md
│   └── deep-agents.md
│
├── pyproject.toml
├── .env                          # Runtime creds (gitignored)
└── .gitignore
```

---

## 7. Implementation Phases

### Phase 1: Foundation (Week 1)
> Get the basic pipeline working end-to-end with one source

**Tasks:**
1. Set up project structure (`src/`, `prompts/`, `tests/`)
2. Write `main.py` — ClaudeSDKClient entry point
3. Write `orchestrator.py` — system prompt, agent definitions
4. Write `config_parser.py` — parse natural text for creds
5. Write `postgres_connector.py` — connect, list tables, query schema
6. Write `table_profiler.py` — deep agent that profiles one table batch
7. Write `model_router.py` — pick cheapest working model from env_info
8. Write MD templates (table, summary, master)
9. Test: profile 5 tables from existing Postgres → get 5 MDs + 1 summary

**Milestone**: `python main.py "Connect to my postgres at sqltosnowflake... and profile 5 tables"` produces valid MDs.

### Phase 2: Scale & Multi-Source (Week 2)
> Handle large schemas, add Snowflake, add batching

**Tasks:**
1. Implement batched table profiling (25 tables per deep agent)
2. Write `snowflake_connector.py`
3. Write `azure_blob_connector.py`
4. Implement parallel deep agent spawning (multiple batches simultaneously)
5. Implement _summary.md generation (Discovery Agent reads all table MDs)
6. Implement master_schema.md generation (Orchestrator reads all summaries)
7. Test: profile full Postgres (all tables) + Snowflake → full MD pyramid

**Milestone**: Full MD pyramid generated for 2+ sources.

### Phase 3: Analysis & Feedback Loops (Week 3)
> Add relationship mapping, quality audits, and the feedback loop

**Tasks:**
1. Write `analysis.py` — Analysis Agent definition
2. Implement Relationship Mapper sub-agent
3. Implement Quality Auditor sub-agent
4. Implement the feedback loop:
   - Analysis agent detects discrepancy
   - Writes to `context/agent_comms/`
   - Triggers re-profile with Claude
   - Reads updated MD, continues
5. Implement `context/` folder system (session logs, decisions, comms)
6. Implement `cost_tracker.py` — log tokens per agent per model
7. Test: intentionally create a discrepancy, verify feedback loop resolves it

**Milestone**: Analysis agent finds real issues, triggers re-profiles, produces clean analysis.

### Phase 4: Reporting & Polish (Week 4)
> Generate final reports, add more connectors, harden the system

**Tasks:**
1. Write `report.py` — Report Agent definition
2. Implement executive summary generation
3. Implement data dictionary generation
4. Implement recommendations engine
5. Add connectors: MySQL, BigQuery, S3
6. Add MD validator (ensure all required sections exist)
7. Add error recovery (if deep agent fails, retry with different model)
8. End-to-end test: 3 sources → full output folder with all reports

**Milestone**: Complete SchemaAnalyzer run producing publication-ready reports.

---

## 8. Key Design Decisions

### 1. Why Claude Agent SDK for Orchestration?
- Built-in Read/Write/Edit/Bash/Agent tools — no custom code needed
- Agents natively understand MD files
- Sub-agent spawning with isolated context
- Session persistence for long-running tasks
- Hooks for monitoring and control (SubagentStart/SubagentStop)

### 2. Why Deep Agents for Table Profiling?
- `init_chat_model()` supports any LLM provider — route to cheapest
- Built-in filesystem middleware — write MDs natively
- Built-in todo tracking — manage large batches
- Auto-summarization — handle huge schema metadata without blowing context
- 20x cheaper than Claude for mechanical profiling tasks

### 3. Why MD Files as the Information Bus?
- Claude agents natively read/write/edit MD files
- Human-readable — debug by just opening the file
- Git-trackable — version control your schema knowledge
- Survives crashes — restart from where you left off
- Composable — agents at any level can read files from any other level
- Templates ensure consistency across all profilers

### 4. Why Feedback Loops?
- Cheap models make mistakes (wrong types, missing columns)
- Catching errors early prevents cascading issues in analysis
- Re-profiling specific tables with Claude is still cheaper than profiling everything with Claude
- Full context trail means you can audit why any decision was made

### 5. Why Context Storage?
- If the system crashes mid-run, agents can read context/ and resume
- If a user questions a finding, the audit trail explains everything
- Inter-agent communication is persisted — no lost context
- Cost tracking shows exactly where money was spent

---

## 9. Dependencies

```toml
[project]
name = "schema-analyzer"
version = "0.1.0"
requires-python = ">=3.11"

[project.dependencies]
# Claude Agent SDK
claude-agent-sdk = ">=0.1.0"

# Deep Agents (LangChain)
deepagents = ">=0.1.0"
langchain = ">=0.3.0"
langchain-openai = ">=0.3.0"     # For Azure AI Foundry models
langgraph = ">=0.4.0"

# Database connectors
asyncpg = ">=0.30.0"             # PostgreSQL
snowflake-connector-python = ">=3.0.0"
azure-storage-blob = ">=12.0.0"  # Azure Blob Storage
boto3 = ">=1.35.0"               # AWS S3
google-cloud-bigquery = ">=3.0.0"
pymysql = ">=1.1.0"              # MySQL

# Utilities
python-dotenv = ">=1.0.0"
pydantic = ">=2.0.0"
rich = ">=13.0.0"                # Pretty console output
```

---

## 10. Risk Mitigation

| Risk | Mitigation |
|------|------------|
| Deep agent produces garbage output | Validation step in Discovery Agent; re-profile with Claude |
| Azure AI Foundry model goes down | `model_router.py` falls back to next cheapest working model |
| Context window overflow on huge schemas | Batching (25 tables/batch); sub-agents get isolated context |
| Network timeout on large DBs | Retry logic in connector scripts; resume from last profiled table |
| MD files get corrupted | Templates enforce structure; `md_validator.py` checks completeness |
| Cost explosion | `cost_tracker.py` enforces budget limits; alerts at 80% budget |
| Feedback loop infinite cycle | Max 3 re-profile attempts per table; then flag for human review |

---

## 11. Success Criteria

- [ ] Profile 100+ tables from Postgres in under 10 minutes
- [ ] Profile across 2+ sources and generate cross-source relationship map
- [ ] Catch and resolve at least 1 discrepancy via feedback loop
- [ ] Total cost under $2 for a 100-table analysis
- [ ] Full context trail — every agent decision is logged
- [ ] Human-readable MD output — non-technical person can understand master_schema.md
- [ ] Crash recovery — restart from context/ folder without re-doing completed work
