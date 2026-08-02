# experimental/tools

Draft tool specifications before implementing in
`crates/codegen/xai-grok-tools`.

Each tool note should include:

1. **Name** (snake_case, unique)
2. **When the agent should call it**
3. **JSON parameters**
4. **Side effects** (filesystem, network, process)
5. **VERIFY** — how a human/orchestrator proves it worked
6. **Safety** — always-approve ok? needs confirm?

Copy `_template.md` when adding a tool.
