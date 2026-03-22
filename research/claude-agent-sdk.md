# Claude Agent SDK - Research Notes

> Source: `docs/claude-agent-sdk-python/` (official Anthropic repo)
> Purpose: Core SDK for building SchemaAnalyzer's agentic system

---

## 1. Core Architecture

The SDK provides two entry points:

| Entry Point | Use Case |
|---|---|
| `query()` | One-shot async — fire a prompt, stream messages back |
| `ClaudeSDKClient` | Bidirectional — multi-turn, stateful conversations |

Both communicate with the Claude Code CLI via a **control protocol** that manages hooks, tool permissions, MCP servers, and sub-agents.

**For SchemaAnalyzer**: We'll use `query()` for independent analysis tasks and `ClaudeSDKClient` for the orchestrator that needs multi-turn coordination.

---

## 2. Agent Definition

Agents are defined declaratively and Claude decides when to invoke them:

```python
from claude_agent_sdk import AgentDefinition

AgentDefinition(
    description="Short description for Claude to understand when to use this agent",
    prompt="Detailed system prompt with instructions",
    tools=["Read", "Write", "Edit", "Bash", "mcp__db__query"],  # tool allowlist
    model="sonnet" | "opus" | "haiku" | "inherit",
    mcpServers=["db_connector"],  # which MCP servers this agent can access
)
```

**Key insight**: You don't "call" agents programmatically. You define them in `ClaudeAgentOptions.agents` and Claude's orchestrator decides to spawn them based on context.

---

## 3. MCP Tool Integration (Critical for DB Connectors)

### SDK MCP Servers (In-Process — Recommended)

```python
from claude_agent_sdk import create_sdk_mcp_server, tool

@tool("query_postgres", "Execute SQL on Postgres", {
    "connection_string": str,
    "sql": str,
    "params": list
})
async def query_postgres(args):
    result = await run_query(args["connection_string"], args["sql"])
    return {"content": [{"type": "text", "text": json.dumps(result)}]}

postgres_server = create_sdk_mcp_server(
    name="postgres",
    version="1.0.0",
    tools=[query_postgres]
)
```

### External MCP Servers (Separate Process)

```python
mcp_servers={
    "snowflake": {
        "type": "stdio",
        "command": "python",
        "args": ["-m", "snowflake_mcp_server"]
    }
}
```

### Tool Naming Convention

All MCP tools are accessed as: `mcp__{server_name}__{tool_name}`

Example: `mcp__postgres__query_postgres`

**For SchemaAnalyzer**: We'll create SDK MCP servers for each data source (Postgres, Snowflake, Azure Blob, etc.) — in-process for performance.

---

## 4. Sub-Agent System

### Defining Multiple Agents

```python
options = ClaudeAgentOptions(
    agents={
        "discovery-agent": AgentDefinition(
            description="Connects to data sources and discovers schemas",
            prompt="You discover database schemas...",
            tools=["Bash", "Read", "Write", "mcp__postgres__query", "mcp__snowflake__query"],
        ),
        "analysis-agent": AgentDefinition(
            description="Analyzes schema structure, relationships, and quality",
            prompt="You analyze discovered schemas...",
            tools=["Read", "Write", "Edit", "Grep", "Glob"],
        ),
        "report-agent": AgentDefinition(
            description="Synthesizes findings into structured markdown reports",
            prompt="You write comprehensive schema analysis reports...",
            tools=["Read", "Write", "Edit", "Glob"],
        ),
    }
)
```

### Sub-Agent Events

- `SubagentStart` — fires when a sub-agent is spawned (includes `agent_id`)
- `SubagentStop` — fires when a sub-agent completes

### Attribution via Hooks

```python
async def track_agent(input_data, tool_use_id, context):
    agent_id = input_data.get("agent_id")
    print(f"Agent {agent_id} running: {input_data['tool_name']}")
```

---

## 5. ClaudeAgentOptions — Full Configuration

```python
ClaudeAgentOptions(
    # Model
    model="opus",
    fallback_model="sonnet",

    # System prompt
    system_prompt="You are the SchemaAnalyzer orchestrator...",

    # Tools
    allowed_tools=["Read", "Write", "Edit", "Bash", "Glob", "Grep",
                   "mcp__postgres__*", "mcp__snowflake__*"],
    disallowed_tools=["WebFetch"],

    # MCP servers
    mcp_servers={"postgres": pg_server, "snowflake": sf_server},

    # Agents
    agents={...},

    # Permissions
    permission_mode="acceptEdits",  # auto-accept file writes
    can_use_tool=my_permission_callback,  # fine-grained control

    # Budget / limits
    max_turns=50,
    max_budget_usd=5.0,

    # Execution
    cwd="/path/to/output",
    env={"DB_HOST": "localhost"},

    # Hooks
    hooks={
        "PreToolUse": [...],
        "PostToolUse": [...],
        "SubagentStart": [...],
        "SubagentStop": [...],
    },

    # Thinking
    thinking=ThinkingConfig(type="enabled", budget_tokens=10000),
)
```

---

## 6. Permission System (3 Layers)

### Layer 1: Allowlist / Blocklist
```python
allowed_tools=["Read", "Write"]    # pre-approved
disallowed_tools=["Bash"]          # never allowed
permission_mode="acceptEdits"      # auto-accept file operations
```

### Layer 2: Permission Callback
```python
async def my_permission_callback(tool_name, input_data, context):
    if tool_name == "Bash" and "DROP" in input_data.get("command", ""):
        return PermissionResultDeny(message="Destructive SQL blocked")
    return PermissionResultAllow()
```

### Layer 3: Hooks (PreToolUse)
```python
async def safety_hook(input_data, tool_use_id, context):
    return {
        "hookSpecificOutput": {
            "hookEventName": "PreToolUse",
            "permissionDecision": "deny",
            "permissionDecisionReason": "Blocked for safety"
        }
    }
```

**For SchemaAnalyzer**: We'll use all 3 layers — allowlist for standard tools, callback to block destructive SQL (DROP/DELETE/TRUNCATE), and hooks for audit logging.

---

## 7. Message Types (What query() Returns)

| Type | Purpose |
|---|---|
| `AssistantMessage` | Claude's response — text, thinking, tool_use blocks |
| `UserMessage` | Input messages (can have `uuid` for checkpointing) |
| `SystemMessage` | Metadata — init, task_started, task_progress |
| `ResultMessage` | Final result — cost, duration, session_id, stop_reason |
| `RateLimitEvent` | Rate limit status changes |
| `StreamEvent` | Partial streaming updates |

### ResultMessage (for tracking cost/performance)
```python
ResultMessage(
    duration_ms=45000,
    total_cost_usd=0.15,
    num_turns=12,
    session_id="abc-123",
    stop_reason="end_turn",
    usage={"input_tokens": 5000, "output_tokens": 3000}
)
```

---

## 8. Hooks System (Event-Driven Control)

| Event | When | Use Case |
|---|---|---|
| `PreToolUse` | Before any tool runs | Block destructive SQL, modify inputs |
| `PostToolUse` | After tool succeeds | Log results, trigger next phase |
| `PostToolUseFailure` | After tool fails | Retry logic, error reporting |
| `SubagentStart` | Agent spawned | Progress tracking |
| `SubagentStop` | Agent done | Collect results, trigger synthesis |
| `Stop` | Execution ending | Cleanup, final report generation |
| `PreCompact` | Before message compaction | Save context before truncation |

---

## 9. Key Patterns for SchemaAnalyzer

### Pattern: Orchestrator + Specialist Agents
```
Orchestrator (opus)
  ├── Discovery Agent (sonnet) — connects to DBs, extracts raw schemas
  ├── Analysis Agent (sonnet) — analyzes structure, finds relationships
  └── Report Agent (haiku) — writes final markdown reports
```

### Pattern: File-Based Communication
Agents communicate via markdown files — discovery writes `schemas/*.md`, analysis reads them and writes `analysis/*.md`, report reads everything and writes `reports/*.md`.

### Pattern: Hook-Based Progress Tracking
Use `SubagentStart`/`SubagentStop` hooks to track which phase we're in and update a progress file.

### Pattern: Safety via Permission Callback
Block any SQL that isn't SELECT/SHOW/DESCRIBE/INFORMATION_SCHEMA — prevent accidental data modification.

---

## 10. Important Caveats

1. **Agents are suggestions, not commands** — Claude decides when to invoke them based on context
2. **ClaudeSDKClient can't cross async contexts** — complete all operations in the same nursery/task group
3. **Budget is checked post-execution** — final cost may exceed `max_budget_usd` by one API call
4. **Python keyword conflicts** — use `async_` and `continue_` in Python; SDK auto-converts
5. **File checkpointing requires two flags** — `enable_file_checkpointing=True` AND `extra_args={"replay-user-messages": None}`
