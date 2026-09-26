# minitrigger skills for AI agents

Skills that teach agents (Claude Code and compatible tools) to adopt `@rivia-labs/minitrigger`
in **other** projects, following the practices in this repository's own README and `CLAUDE.md`.
Each skill is self-contained.

| Skill | Use when |
| --- | --- |
| [`minitrigger-setup`](minitrigger-setup/SKILL.md) | installing and configuring minitrigger in a project (detects the framework/package manager, wires up the engine, `migrate()`/`start()`/`stop()`, and the worker process topology) |
| [`minitrigger-task`](minitrigger-task/SKILL.md) | writing or reviewing `defineTask`/`ctx.step.*` code (golden rules, replay/determinism, tokens, subtasks, debounce, concurrency keys, retries) |
| [`minitrigger-outbox`](minitrigger-outbox/SKILL.md) | finding dual writes (a DB write followed by a separate `engine.trigger()`) and refactoring them to the transactional outbox pattern (`trigger(..., { tx })`) |
| [`minitrigger-testing`](minitrigger-testing/SKILL.md) | testing code that triggers tasks and testing task logic itself (`runTask`, `createTestEngine`, `assertTriggered`) |
| [`minitrigger-debug`](minitrigger-debug/SKILL.md) | diagnosing stuck runs, stalled workers, orphaned schedules/waitpoints and other production incidents |

## Installation

### As a Claude Code plugin (recommended)

This repository is also a plugin marketplace:

```bash
/plugin marketplace add igorabreu29/minitrigger
/plugin install minitrigger@minitrigger
```

The skills become available as `/minitrigger:minitrigger-setup`, `/minitrigger:minitrigger-task`,
etc., and Claude Code also invokes them on its own whenever a task matches a skill's description.

### Copying into a project

The skills are published inside the npm package:

```bash
mkdir -p .claude/skills
cp -r node_modules/@rivia-labs/minitrigger/skills/minitrigger-* .claude/skills/
```

Or, to make them available to every project on the machine:

```bash
mkdir -p ~/.claude/skills
cp -r node_modules/@rivia-labs/minitrigger/skills/minitrigger-* ~/.claude/skills/
```

### Other agents (Cursor, Copilot, Codex)

Each `SKILL.md` is plain Markdown. Reference them from the agent's own rules file, for example in
`.cursor/rules` or `AGENTS.md`: "For background jobs and durable workflows, follow
`node_modules/@rivia-labs/minitrigger/skills/`".

## Typical usage

1. "Add minitrigger to this project" / "set up durable background jobs with BullMQ and Postgres" →
   `minitrigger-setup`
2. "Write a task that sends a welcome email after signup, with a retry" / "review this task for
   determinism bugs" → `minitrigger-task`
3. "We save the order but the email job never queued when the process crashes" / "wrap this trigger
   in the transaction" → `minitrigger-outbox` (leans on `minitrigger-task` and `minitrigger-testing`)
4. "How do I unit test this task?" / "write a test that asserts the receipt task gets triggered" →
   `minitrigger-testing`
5. "Runs are stuck in `waiting`" / "a queue looks paused and nothing drains" → `minitrigger-debug`
