# Deep Agents - Research Notes

> Sources: `docs/deepagents/`, `docs/deep-agents-from-scratch/`, `docs/open-deep-research/`
> Purpose: Architectural patterns for multi-phase agentic workflows

---

## 1. What Are Deep Agents?

Deep Agents is an opinionated agent harness (built on LangGraph) that differs from regular agents by focusing on **context management at scale**.

### Regular Agent vs Deep Agent

| Regular Agent | Deep Agent |
|---|---|
| Simple tool-calling loop | Multi-phase workflow with state management |
| All context in messages | Context offloaded to virtual filesystem |
| Single agent execution | Supervisor + specialized sub-agents |
| No planning | Explicit planning with todo tracking |
| Stateless between runs | Checkpointed, resumable state |
| Token limit = hard wall | Auto-summarization middleware |

### Core Principle
> Treat agents as **context management systems**, not just tool-calling loops.

---

## 2. The 5-Phase Workflow Pattern

Every deep agent follows this pattern:

```
Phase 1: PLAN        → Decompose task into todos
Phase 2: DELEGATE     → Spawn sub-agents for each todo
Phase 3: EXECUTE      → Sub-agents work in parallel with isolated context
Phase 4: SYNTHESIZE   → Aggregate sub-agent results
Phase 5: VERIFY       → Check output against original request
```

### Applied to SchemaAnalyzer:

```
Phase 1: PLAN        → Parse connection configs, plan discovery order
Phase 2: DELEGATE     → Spawn discovery agents per data source
Phase 3: EXECUTE      → Each agent connects, extracts schemas, writes findings
Phase 4: SYNTHESIZE   → Merge all schema findings, find cross-source relationships
Phase 5: VERIFY       → Validate completeness, generate final report
```

---

## 3. State Architecture

### Extended State Pattern

```python
class DeepAgentState(MessagesState):
    todos: list[Todo]              # Task tracking (plan visibility)
    files: dict[str, str]          # Virtual filesystem (context offloading)
    messages: list[BaseMessage]    # Conversation history
```

### File Reducer (Last Write Wins)

```python
def file_reducer(left, right):
    if left is None: return right
    elif right is None: return left
    else: return {**left, **right}  # merge, right overwrites
```

### Why This Matters
- **Todos** keep the agent focused on the plan (prevents context drift)
- **Files** offload large results (prevents token explosion)
- **Messages** only hold recent context (auto-summarized)

---

## 4. Context Passing Mechanisms

### Mechanism 1: Virtual Filesystem

Instead of passing huge results in messages:

```python
# BAD: token explosion
messages.append(HumanMessage(content=huge_schema_dump))

# GOOD: offload to file, reference by path
write_file("schemas/postgres/users.md", huge_schema_dump)
# Agent later: read_file("schemas/postgres/users.md")
```

**Files act as state containers:**
- `schemas/*.md` — discovered schema definitions
- `analysis/*.md` — per-source analysis findings
- `relationships/*.md` — cross-source relationship maps
- `reports/*.md` — final synthesized reports

### Mechanism 2: Todo List as Shared Plan

```python
write_todos([
    {"content": "Discover Postgres schemas", "status": "completed"},
    {"content": "Discover Snowflake schemas", "status": "in_progress"},
    {"content": "Analyze cross-source relationships", "status": "pending"},
    {"content": "Generate final report", "status": "pending"},
])
```

Agent reads todos to remember what's done and what's next.

### Mechanism 3: Command-Based Transitions

```python
return Command(
    update={"files": {"schemas/pg.md": content}},
    goto="analysis_phase"  # deterministic routing
)
```

---

## 5. Sub-Agent Isolation (Context Quarantine)

Each sub-agent gets:
- **Own message history** (starts empty)
- **Own tool set** (scoped to its role)
- **Read access to parent files** (shared context)
- **Write access to own files** (isolated output)
- **Returns result as ToolMessage** to parent

```
Orchestrator Context:
  ├── messages: [user request, plan, delegation decisions]
  ├── files: {config.md, progress.md}
  │
  ├── SubAgent: postgres-discovery
  │     ├── messages: [focused task prompt]
  │     ├── tools: [pg_query, write_file]
  │     └── output → files: {schemas/postgres/*.md}
  │
  └── SubAgent: snowflake-discovery
        ├── messages: [focused task prompt]
        ├── tools: [sf_query, write_file]
        └── output → files: {schemas/snowflake/*.md}
```

**Prevents**: Context interference, token accumulation, task confusion between agents.

---

## 6. Middleware Stack

Deep agents compose behavior through middleware:

| Middleware | Purpose |
|---|---|
| `TodoListMiddleware` | Adds `write_todos` / `read_todos` tools for planning |
| `FilesystemMiddleware` | Adds `ls` / `read` / `write` / `edit` / `glob` / `grep` for file management |
| `SummarizationMiddleware` | Auto-compacts old messages when approaching token limit |
| `SkillsMiddleware` | Progressive disclosure — loads expertise on demand |
| `SubagentMiddleware` | Adds `task()` tool for delegating to sub-agents |

### Summarization (Token Management)

```python
SummarizationMiddleware(
    model="fast-model",
    trigger=("fraction", 0.85),  # Summarize at 85% token capacity
    keep=("fraction", 0.10),     # Keep 10% most recent messages
)
```

Old messages are summarized and offloaded to files. Recent context preserved for reasoning.

---

## 7. Skills Pattern (Progressive Disclosure)

Instead of loading all expertise upfront (wastes tokens), use skills:

```
AGENTS.md                    # Agent identity + general instructions
skills/
  ├── schema-discovery/
  │   └── SKILL.md          # Detailed discovery procedures
  ├── relationship-analysis/
  │   └── SKILL.md          # How to find FK/PK relationships
  └── report-generation/
      └── SKILL.md          # Report templates and formatting rules
```

Agent sees only skill **descriptions** initially. Full `SKILL.md` loaded on-demand when the agent determines it needs that expertise.

**For SchemaAnalyzer**: Each analysis capability is a skill — the agent loads the right expertise for the current data source type.

---

## 8. Supervisor-Researcher Pattern

```
SUPERVISOR (coordinator):
  1. Receives user request
  2. Plans research direction (write_todos)
  3. Delegates via task() tool
  4. Uses think_tool to assess progress
  5. Decides when synthesis is complete

RESEARCHER (executor):
  1. Receives focused task from supervisor
  2. Executes with specialized tools
  3. Uses think_tool to plan search strategy
  4. Writes findings to files
  5. Returns summary to supervisor
```

### think_tool (Reflection Mechanism)

A special tool that doesn't execute anything — just gives the agent space to reason:

```python
@tool("think")
def think(thought: str) -> str:
    """Use this to plan your next steps or reflect on progress."""
    return "Thought recorded."
```

**Prevents**: Repetitive searches, circular reasoning, premature synthesis.

---

## 9. Configuration-Driven Behavior

```python
class Configuration(BaseModel):
    max_concurrent_analyses: int = 5
    max_analysis_iterations: int = 10
    discovery_depth: Literal["shallow", "deep"] = "deep"
    output_format: Literal["json", "markdown", "html"] = "markdown"

    # Users change behavior without code changes
    config = Configuration.from_runnable_config(runtime_config)
```

---

## 10. Checkpointing & Resumability

```python
agent = create_deep_agent(
    checkpointer=PostgresSaver(...),   # Persist state to DB
    store=InMemoryStore(...),          # Shared context store
)
```

- Can resume from any phase on interruption
- State includes: todos, files, messages, phase
- Long-running schema analysis can survive crashes

---

## 11. Key Takeaways for SchemaAnalyzer

1. **State-first design** — Define `SchemaAnalyzerState` with todos, files, schemas fields
2. **Phase-based workflow** — Clear phases: Plan → Discover → Analyze → Synthesize → Report
3. **Context offloading to MD files** — Large schema dumps go to files, not messages
4. **Sub-agent isolation** — Each data source gets its own agent with scoped tools
5. **Todo-driven planning** — Agent maintains and updates a task list for focus
6. **Summarization middleware** — Handle large schemas without hitting token limits
7. **Skills for expertise** — Load data-source-specific knowledge on demand
8. **think_tool for reflection** — Give agents space to reason about complex schemas
9. **Configuration-driven** — Users control depth, concurrency, output format
10. **Checkpointing** — Resume long analyses from where they left off

---

## 12. Mapping Deep Agent Concepts → Claude Agent SDK

| Deep Agent Concept | Claude Agent SDK Equivalent |
|---|---|
| `write_todos` | Built-in `TodoWrite` tool |
| `task()` sub-agent | `AgentDefinition` in `agents={}` |
| `write_file` / `read_file` | Built-in `Write` / `Read` tools |
| `FilesystemMiddleware` | Built-in `Glob`, `Grep`, `Read`, `Write`, `Edit` |
| `SummarizationMiddleware` | Not built-in — implement via hooks or manage via `max_turns` |
| `think_tool` | Custom MCP tool or just prompt engineering |
| `Command(goto=...)` | Hook-based routing or prompt-driven phase transitions |
| `checkpointer` | `ClaudeSDKClient` session persistence |
| Skills directory | `.claude/agents/*.md` agent definition files |
| Configuration | `ClaudeAgentOptions` + env vars |

The Claude Agent SDK gives us the **primitives** (agents, tools, hooks, MCP). Deep Agent patterns teach us **how to compose them** into a robust multi-phase system.
