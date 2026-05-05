---
description: Resume krusch-dbos-mcp development with project-scoped context
---

# /continue — krusch-dbos-mcp

## Steps

1. **Context Load (Project-Scoped)**:
   - Read `GEMINI_INFLIGHT.md` in this project root.
   - Query `mcp_homelab-memory_mcp_homelab-memory_search(category: 'activity', query: 'krusch-dbos-mcp')`.
   - Query `mcp_homelab-memory_mcp_homelab-memory_search(category: 'lessons', query: 'krusch-dbos-mcp')`.
   - Query `mcp_nuggets-memory_nudges(kinds: ['project', 'user'], query: 'krusch-dbos-mcp')`.
   - **Zero-Trust**: Execute `pg_git_semantic_search(project: 'krusch-dbos-mcp')` to verify codebase state.

2. **Transient State Check**: Check `GEMINI_INFLIGHT.md` for any **Transient State** or **Fragile** blocks.

3. **Execution**: Generate `task.md` and begin work autonomously.
