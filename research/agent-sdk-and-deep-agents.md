# Agent SDK & Deep Agents — Research Reference

> Reference document for building SchemaAnalyzer. Covers Claude Agent SDK, LangChain Deep Agents, and architectural patterns for multi-agent systems.

---

## Table of Contents

1. [Claude Agent SDK](#1-claude-agent-sdk)
2. [LangChain Deep Agents](#2-langchain-deep-agents)
3. [Head-to-Head Comparison](#3-head-to-head-comparison)
4. [Agentic Loop Pattern](#4-agentic-loop-pattern)
5. [Multi-Agent Orchestration Patterns](#5-multi-agent-orchestration-patterns)
6. [Context Management & Compression](#6-context-management--compression)
7. [MCP Integration](#7-mcp-integration-model-context-protocol)
8. [Architectural Decision for SchemaAnalyzer](#8-architectural-decision-for-schemaanalyzer)

---

## 1. Claude Agent SDK

### What It Is

Programmatic Claude Code. The same autonomous capabilities (file ops, shell, web, sub-agents) exposed as a Python/TypeScript library.

### Installation

```bash
# TypeScript
npm install @anthropic-ai/claude-agent-sdk

# Python
pip install claude-agent-sdk  # Python 3.10+
```

### Built-in Tools (No Implementation Needed)

| Category | Tools | Function |
|----------|-------|----------|
| File ops | `Read`, `Edit`, `Write` | Read, modify, create files |
| Search | `Glob`, `Grep` | Pattern matching, regex search |
| Execution | `Bash` | Shell commands, scripts, git |
| Web | `WebSearch`, `WebFetch` | Web search, fetch/parse pages |
| Discovery | `ToolSearch` | Dynamically load tools on-demand |
| Orchestration | `Agent`, `Skill`, `AskUserQuestion`, `TodoWrite` | Sub-agents, skills, user input, task tracking |

### Core API — `query()` (One-off Tasks)

```python
import asyncio
from claude_agent_sdk import query, ClaudeAgentOptions, ResultMessage

async def main():
    async for message in query(
        prompt="Analyze the database schema and write findings to output/",
        options=ClaudeAgentOptions(
            allowed_tools=["Read", "Write", "Edit", "Bash", "Glob", "Grep"],
            permission_mode="acceptEdits",
            max_turns=30,
            max_budget_usd=1.00,
            model="claude-opus-4-1",
            cwd="/path/to/project",
        ),
    ):
        if isinstance(message, ResultMessage):
            print(f"Done: {message.result}")
            print(f"Cost: ${message.total_cost_usd}")

asyncio.run(main())
```

### Core API — `ClaudeSDKClient` (Multi-turn Sessions)

```python
async with ClaudeSDKClient(options=options) as client:
    await client.query("Discover all schemas in the database")
    async for msg in client.receive_response():
        print(msg)

    await client.query("Now analyze the relationships")  # Context preserved
    async for msg in client.receive_response():
        print(msg)
```

### Spawning Sub-agents

```python
async for message in query(
    prompt="Analyze the full database",
    options=ClaudeAgentOptions(
        allowed_tools=["Read", "Write", "Bash", "Agent"],
        agents={
            "schema-discoverer": AgentDefinition(
                description="Discovers and catalogs database schemas",
                prompt="You discover database schemas. Write findings to MD files.",
                tools=["Bash", "Write", "mcp__db__query"],
                model="sonnet"
            ),
            "relationship-mapper": AgentDefinition(
                description="Maps relationships between tables/entities",
                prompt="You analyze foreign keys, joins, and entity relationships.",
                tools=["Read", "Write", "Grep"],
                model="sonnet"
            ),
        }
    ),
):
    pass  # Claude orchestrates automatically
```

**Sub-agent Rules:**
- Each gets a **fresh context** (no parent history)
- Only the **final message** returns to parent
- **Cannot spawn their own sub-agents** (no recursion)
- Inherit project-level config (CLAUDE.md, skills, hooks)

### Configuration Options (Full)

```python
ClaudeAgentOptions(
    # Model & Reasoning
    model="claude-opus-4-1",
    effort="high",                    # low | medium | high | max

    # Limits
    max_turns=30,
    max_budget_usd=1.00,

    # Permissions
    allowed_tools=["Read", "Edit", "Bash"],
    disallowed_tools=["Bash(curl:*)", "Write(*.env)"],
    permission_mode="acceptEdits",    # default | acceptEdits | plan | dontAsk | bypassPermissions

    # Context
    cwd="/path/to/project",
    resume=session_id,                # Resume previous session
    setting_sources=["project"],      # Load CLAUDE.md, skills, hooks

    # Sub-agents
    agents={...},

    # MCP Servers
    mcp_servers={...},

    # Hooks
    hooks={
        "PreToolUse": [...],
        "PostToolUse": [...],
        "Stop": [...],
    },

    # Streaming
    include_partial_messages=True,

    # Environment
    env={"ENABLE_TOOL_SEARCH": "auto:5"},
)
```

### Hooks System

| Hook | Fires When | Use Case |
|------|-----------|----------|
| `PreToolUse` | Before tool runs | Validate/block/modify inputs |
| `PostToolUse` | After tool returns | Audit, log results |
| `Stop` | Agent finishes | Save state, cleanup |
| `SubagentStart` | Sub-agent spawns | Track parallel tasks |
| `SubagentStop` | Sub-agent completes | Aggregate results |
| `PreCompact` | Before context compression | Archive transcript |

### Session Resume

```python
# Capture session ID
async for message in query(prompt="...", options=options):
    if isinstance(message, ResultMessage):
        session_id = message.session_id

# Resume later with full context
async for message in query(
    prompt="Continue analyzing the remaining tables",
    options=ClaudeAgentOptions(resume=session_id)
):
    pass
```

### Cost Tracking

```python
if isinstance(message, ResultMessage):
    print(f"Cost: ${message.total_cost_usd}")
    print(f"Input tokens: {message.usage.input_tokens}")
    print(f"Output tokens: {message.usage.output_tokens}")
    print(f"Turns: {message.num_turns}")
```

---

## 2. LangChain Deep Agents

### What It Is

Open-source agent harness built on LangChain + LangGraph. Model-agnostic (100+ providers). Designed for long-horizon, multi-step tasks requiring planning, context management, and sub-agent delegation.

**Status**: Launched July 2025. Hit 9.9k GitHub stars in 5 hours (March 2026 update). Production-ready with NVIDIA enterprise integration.

### Installation

```bash
pip install deepagents
# Also: langchain, langgraph, langchain-mcp-adapters
```

### Architecture Stack

```
Deep Agents  →  Agent Harness (opinionated, battle-tested)
LangGraph    →  Agent Runtime (low-level orchestration, StateGraph)
LangChain    →  Agent Framework (high-level primitives, model adapters)
```

### Built-In Tools

- `write_todos` — Task planning and breakdown
- `read_file`, `write_file`, `edit_file` — File operations
- `ls`, `grep`, `glob` — Search/discovery
- `execute` — Shell command execution (sandboxed)
- `task` — Sub-agent delegation
- Auto-context summarization
- Auto-tool result offloading

### Core Usage

```python
from deepagents import create_deep_agent
from langchain.chat_models import init_chat_model

agent = create_deep_agent(
    model=init_chat_model("anthropic:claude-sonnet-4-6"),
    tools=[custom_db_tool, custom_analysis_tool],
    system_prompt="You are a database schema analysis expert...",
    subagents=[
        {
            "name": "schema-discoverer",
            "description": "Discovers and catalogs database schemas",
            "instructions": "You connect to databases and discover schemas..."
        },
        {
            "name": "relationship-mapper",
            "description": "Maps entity relationships and foreign keys",
            "instructions": "You analyze table relationships..."
        }
    ]
)

result = agent.invoke({
    "messages": [{"role": "user", "content": "Analyze the production database"}]
})
```

### LangGraph StateGraph (The Runtime)

```python
from langgraph.graph import StateGraph, MessagesState, START, END

class AnalyzerState(MessagesState):
    schemas_discovered: list = []
    relationships: dict = {}
    report_path: str = ""

builder = StateGraph(AnalyzerState)
builder.add_node("discover", discover_schemas)
builder.add_node("analyze", analyze_relationships)
builder.add_node("report", generate_report)

builder.add_edge(START, "discover")
builder.add_conditional_edges("discover", route_after_discovery)
builder.add_edge("analyze", "report")
builder.add_edge("report", END)

graph = builder.compile(checkpointer=checkpointer)
```

### Checkpointing (Persistence & Resume)

```python
from langgraph.checkpoint.postgres import AsyncPostgresSaver

checkpointer = AsyncPostgresSaver("postgresql://...")
graph = builder.compile(checkpointer=checkpointer)

# Invoke with thread ID
config = {"configurable": {"thread_id": "analysis-run-001"}}
result = graph.invoke({"messages": [...]}, config)

# Resume later — picks up from last checkpoint
result = graph.invoke({"messages": [...]}, config)
```

**Available checkpointers:** InMemory, SQLite, Postgres, DynamoDB, CosmosDB

### Memory Store (Cross-Thread State)

```python
graph = builder.compile(
    checkpointer=checkpointer,
    store=InMemoryStore()
)

# Store schemas for cross-thread access
store.put(("schemas", "prod_db"), "users_table", value=schema_def)
results = store.search(("schemas",), query="email validation")  # Semantic search
```

### Plan-and-Execute Patterns

**1. Basic Plan-and-Execute:**
```
Planner (big model) → Executors (small models) → Final Output
```

**2. ReWOO (Reasoning Without Observations):**
```
${1} = inspect_schema("users")
${2} = find_relationships("${1}")
${3} = generate_report("${1}", "${2}")
```

**3. LLMCompiler (Most Advanced — 3.6x speed):**
```
Planner → DAG of parallel tasks → Task Executor → Joiner → Replan if needed
```

### Three-Tier Context Compression

| Tier | Trigger | Action |
|------|---------|--------|
| 1 | Tool output > 20k tokens | Offload to filesystem with preview + path |
| 2 | Context > 85% capacity | Offload older file operation details to pointers |
| 3 | Agent-triggered | Autonomous summarization, full record saved to disk |

### Open Deep Research Pattern (Three-Phase)

```
Phase 1: Scoping      → Clarification questions, research brief
Phase 2: Research      → Supervisor delegates to parallel sub-agents
Phase 3: Writing       → Single LLM call synthesizes findings into report
```

**Key learning**: Sub-agents work best for parallelizable tasks. Single-agent writing avoids coordination failures.

---

## 3. Head-to-Head Comparison

| Feature | Claude Agent SDK | LangChain Deep Agents |
|---------|-----------------|----------------------|
| **Model Support** | Claude-only | Model-agnostic (100+ providers) |
| **Type** | Agent harness | Agent harness |
| **Built-in Tools** | Read, Write, Edit, Bash, Glob, Grep, WebSearch, Agent | read_file, write_file, edit_file, execute, write_todos, task |
| **Sub-agents** | Yes (AgentDefinition) | Yes (subagents list) |
| **MCP Integration** | Native (mcp_servers config) | Via langchain-mcp-adapters |
| **Session Resume** | Yes (session_id + resume) | Yes (checkpointing) |
| **Long-term Memory** | No built-in | Yes (Memory Store) |
| **Context Compression** | Auto-compaction | Three-tier offloading |
| **Observability** | Hooks | LangSmith (traces, evals, dashboards) |
| **Cost Tracking** | Built-in (ResultMessage) | Via LangSmith |
| **Hooks/Middleware** | Yes (PreToolUse, PostToolUse, etc.) | Yes (middleware, interrupts) |
| **Permission Model** | Fine-grained (6 modes) | Custom via LangGraph interrupts |
| **Effort Levels** | low/medium/high/max | N/A (model-dependent) |
| **Streaming** | include_partial_messages | LangGraph streaming modes |
| **Deployment** | Any (Docker, CI, web) | Any (Docker, CI, web, LangGraph Cloud) |
| **Language** | Python + TypeScript | Python + TypeScript (deepagentsjs) |
| **Custom Tools** | SDK MCP servers (in-process) | LangChain Tools + MCP |
| **Planning** | Agent decides (prompt-driven) | write_todos, Plan-and-Execute, LLMCompiler |
| **Open Source** | Partial (SDK open, Claude proprietary) | Fully open source |

### When to Choose Which

**Choose Claude Agent SDK when:**
- You're committed to Claude models
- You want the simplest setup (minimal code)
- You need fine-grained permission control
- Built-in tools (Read/Write/Edit) are sufficient
- You want native Claude Code parity

**Choose LangChain Deep Agents when:**
- You need model-agnostic support
- You want structured planning (write_todos, LLMCompiler)
- You need long-term memory (Memory Store)
- You want observability (LangSmith)
- You need checkpointing with multiple backends
- You want the Open Deep Research three-phase pattern

---

## 4. Agentic Loop Pattern

Both systems use the same fundamental loop:

```
Perceive → Reason → Act → Observe → Repeat
```

### Claude Agent SDK Loop

```
1. Receive prompt + system prompt + tool definitions
2. Claude evaluates → produces text + tool calls
3. SDK executes tools → collects results
4. Results fed back to Claude
5. Repeat 2-4 until Claude responds with no tool calls
6. Return ResultMessage (result, cost, session_id)
```

### LangGraph Loop

```
1. StateGraph receives input + state
2. Node executes (model call, tool call, logic)
3. State updated with node output
4. Conditional edges determine next node
5. Repeat 2-4 until END node reached
6. Return final state
```

### Key Difference

Claude Agent SDK: **implicit loop** — Claude decides when to stop calling tools.
LangGraph: **explicit loop** — you define the graph, edges, and termination conditions.

---

## 5. Multi-Agent Orchestration Patterns

### Pattern 1: Orchestrator-Workers (Both Support)

```
Orchestrator Agent
    ├── Worker A (Schema Discovery)
    ├── Worker B (Relationship Mapping)
    ├── Worker C (Quality Analysis)
    └── Worker D (Report Generation)
```

- Orchestrator plans and delegates
- Workers execute in parallel with isolated context
- Only final summaries return to orchestrator

### Pattern 2: Sequential Pipeline

```
Discovery → Analysis → Relationship Mapping → Report
```

- Each stage reads output of previous stage (via MD files)
- Deterministic, easy to debug
- Slower but predictable

### Pattern 3: LLMCompiler DAG (LangChain Only)

```
Planner generates DAG:
  ${1} = discover_postgres_schema()     ─┐
  ${2} = discover_snowflake_schema()     ├─→ ${4} = merge_schemas(${1}, ${2}, ${3})
  ${3} = discover_blob_structure()      ─┘
  ${5} = analyze_relationships(${4})
  ${6} = generate_report(${4}, ${5})
```

- 3.6x speed improvement through parallelization
- Automatic dependency resolution
- Dynamic replanning if tasks fail

### Pattern 4: Hierarchical (Both Support)

```
Coordinator (Opus)
    ├── Domain Expert A (Sonnet) — SQL databases
    │       ├── Postgres Worker (Haiku)
    │       └── Snowflake Worker (Haiku)
    └── Domain Expert B (Sonnet) — Blob storage
            ├── Azure Blob Worker (Haiku)
            └── S3 Worker (Haiku)
```

- Different models at different levels (cost optimization)
- Domain experts synthesize worker outputs
- Coordinator produces final consolidated report

---

## 6. Context Management & Compression

### The Problem

Long-running agents accumulate context: tool inputs, outputs, intermediate reasoning. Context windows fill up, causing degraded performance or failures.

### Claude Agent SDK Approach

- **Auto-compaction**: SDK summarizes older messages when context approaches limits
- **Compact boundary**: `SystemMessage(subtype="compact_boundary")` signals when compaction occurs
- **Customizable**: Add "Summary instructions" to CLAUDE.md to control what gets preserved
- **PreCompact hook**: Archive full transcript before compression

### LangChain Deep Agents Approach

- **Tier 1**: Tool outputs > 20k tokens → offloaded to filesystem
- **Tier 2**: Context > 85% → older file operations replaced with pointers
- **Tier 3**: Agent-triggered summarization → full record saved to disk, summary kept in context
- **Middleware**: `SummarizationMiddleware` with configurable triggers

### Best Practice for SchemaAnalyzer

Use **MD files as the primary context transfer mechanism**:

```
Agent A writes: output/schemas/users_table.md
Agent B reads:  output/schemas/users_table.md
```

This means:
- Each agent's context stays small (only what it's currently working on)
- Information persists on disk (survives context compression)
- Any agent can access any previous agent's output
- Human-readable audit trail

---

## 7. MCP Integration (Model Context Protocol)

### What Is MCP

Standardized protocol for connecting LLMs to external tools and data sources. Think "USB for AI" — one standard interface for databases, APIs, file systems, etc.

### Claude Agent SDK — Native MCP

```python
options = ClaudeAgentOptions(
    mcp_servers={
        "postgres": {
            "command": "npx",
            "args": ["-y", "@modelcontextprotocol/server-postgres", DB_URL]
        },
        "custom-db": {
            "command": "python",
            "args": ["mcp_server.py"],
        }
    },
    allowed_tools=[
        "mcp__postgres__query",
        "mcp__custom-db__inspect_schema"
    ]
)
```

### LangChain Deep Agents — MCP Adapters

```python
from langchain_mcp_adapters.client import MultiServerMCPClient

client = MultiServerMCPClient({
    "postgres": {
        "command": "python",
        "args": ["postgres_mcp_server.py"],
        "transport": "stdio",
    },
    "snowflake": {
        "url": "http://localhost:8000/mcp",
        "transport": "http",
    }
})

tools = await client.get_tools()
agent = create_deep_agent(tools=tools)
```

### Transport Types

| Transport | Use Case |
|-----------|----------|
| **stdio** | Local process, same machine |
| **HTTP/SSE** | Remote server, cloud deployment |

### Available MCP Servers (Relevant to SchemaAnalyzer)

| Server | Package | Purpose |
|--------|---------|---------|
| PostgreSQL | `@modelcontextprotocol/server-postgres` | Query Postgres databases |
| SQLite | `@modelcontextprotocol/server-sqlite` | Query SQLite databases |
| Filesystem | `@modelcontextprotocol/server-filesystem` | Read/write files |
| GitHub | `@modelcontextprotocol/server-github` | GitHub API access |

**Custom MCP servers needed for SchemaAnalyzer:**
- Snowflake connector
- Azure Blob Storage connector
- Generic JDBC/ODBC connector
- Schema introspection tools

---

## 8. Architectural Decision for SchemaAnalyzer

### Recommendation: Claude Agent SDK

**Rationale:**
1. **We're building ON Claude Code** — the SDK gives us native parity
2. **Built-in file tools** (Read/Write/Edit) are perfect for MD-based information transfer
3. **Simpler codebase** — less framework overhead than LangGraph
4. **Fine-grained permissions** — control exactly what each agent can do
5. **Sub-agents with context isolation** — each analyzer gets fresh context
6. **Session resume** — can pick up long analyses where they left off
7. **Hooks** — audit logging, validation, progress tracking

### What We'd Miss (And How to Compensate)

| Deep Agents Feature | Compensation |
|---------------------|-------------|
| write_todos planning | Use TodoWrite built-in tool |
| LangSmith observability | Use hooks for logging + custom dashboard |
| Checkpointing | Use session resume + MD files as state |
| Memory Store | Use MD files in output/ directory |
| Model-agnostic | Not needed — we're Claude-committed |
| LLMCompiler DAG | Use parallel sub-agents instead |

### Proposed Architecture (Claude Agent SDK)

```
Orchestrator Agent (Opus)
    │
    ├── Discovery Agent (Sonnet) — connects to data sources, writes schema MD files
    │       Uses: Bash, Write, mcp__postgres, mcp__snowflake, mcp__azure_blob
    │
    ├── Relationship Agent (Sonnet) — reads schema files, maps relationships
    │       Uses: Read, Write, Grep, Glob
    │
    ├── Quality Agent (Sonnet) — analyzes patterns, anti-patterns, issues
    │       Uses: Read, Write, Grep
    │
    └── Report Agent (Sonnet) — synthesizes final comprehensive report
            Uses: Read, Write, Glob
```

### MD File Structure (Information Transfer)

```
output/
├── discovery/
│   ├── postgres_public_users.md
│   ├── postgres_public_orders.md
│   ├── snowflake_analytics_events.md
│   └── azure_blob_inventory.md
├── relationships/
│   ├── entity_relationship_map.md
│   └── cross_source_links.md
├── quality/
│   ├── design_patterns.md
│   ├── anti_patterns.md
│   └── optimization_opportunities.md
└── reports/
    └── final_analysis.md
```

---

## Sources

### Claude Agent SDK
- [Agent SDK Overview](https://platform.claude.com/docs/en/agent-sdk/overview)
- [Quickstart Guide](https://platform.claude.com/docs/en/agent-sdk/quickstart)
- [Agent Loop](https://platform.claude.com/docs/en/agent-sdk/agent-loop)
- [Sub-agents](https://platform.claude.com/docs/en/agent-sdk/subagents)
- [MCP Integration](https://platform.claude.com/docs/en/agent-sdk/mcp)
- [Permissions](https://platform.claude.com/docs/en/agent-sdk/permissions)
- [Hooks](https://platform.claude.com/docs/en/agent-sdk/hooks)
- [Streaming](https://platform.claude.com/docs/en/agent-sdk/streaming-output)
- [GitHub: claude-agent-sdk-python](https://github.com/anthropics/claude-agent-sdk-python)
- [GitHub: claude-agent-sdk-typescript](https://github.com/anthropics/claude-agent-sdk-typescript)

### LangChain Deep Agents
- [Deep Agents Overview](https://docs.langchain.com/oss/python/deepagents/overview)
- [Deep Agents GitHub](https://github.com/langchain-ai/deepagents)
- [Deep Agents Blog Post](https://blog.langchain.com/deep-agents/)
- [Open Deep Research](https://github.com/langchain-ai/open_deep_research)
- [Deep Agents from Scratch](https://github.com/langchain-ai/deep-agents-from-scratch)
- [Context Management for Deep Agents](https://blog.langchain.com/context-management-for-deepagents/)
- [LangGraph Overview](https://docs.langchain.com/oss/python/langgraph/overview)
- [Plan-and-Execute Agents](https://blog.langchain.com/planning-agents/)
- [LangChain MCP Adapters](https://github.com/langchain-ai/langchain-mcp-adapters)

### General Agentic Patterns
- [Anthropic Multi-Agent Research System](https://www.anthropic.com/engineering/multi-agent-research-system)
- [2026 Agentic Coding Trends Report](https://resources.anthropic.com/hubfs/2026%20Agentic%20Coding%20Trends%20Report.pdf)
- [Building Agents with Claude Agent SDK](https://claude.com/blog/building-agents-with-the-claude-agent-sdk)
