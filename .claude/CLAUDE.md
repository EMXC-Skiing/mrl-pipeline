# MRL Pipeline

Builds the Mass Ranking List: Nordic ski race results → athlete penalties → race points → season totals and U16 / Eastern High School qualifying places. The code is partway through a v1 → v2 rewrite.

## Where things stand

- **v1** is the working reference: `src/mrl_pipeline/legacy/`. It writes to a Google Sheets "warehouse".
- **v2** is the target: a graph of **models** writing to DuckDB (local file or MotherDuck), run by Prefect or an `mrl` CLI. The v2 code in `io/`, `warehouse/`, `orchestration/` and `models/` is incomplete and **does not import** yet (defects D1–D3).
- The v2 design is in two documents. Read the relevant section before changing v2 code:
  - `claude/v2-spec.md`: what v2 must do. Requirement IDs (`FR-…`, `NFR-…`), resolved decisions (`R…`), open questions (`Q…`).
  - `claude/v2-project-structure.md`: every target file and class, with status (`KEEP`/`MOVE`/`FIX`/`ADD`/`DELETE`), known defects (`D…`), and the build phases (§13).
- Work proceeds phase by phase (structure doc §13). Say which phase and requirement IDs a change serves.

## Rules that matter most

1. **Don't change ranking behavior by accident.** Anything not marked **NEW** in the spec must reproduce v1 exactly. Port legacy logic unchanged first (Phases 5–6); add NEW rules (`_27`, season-aware names, disambiguation, `qa_name_conflicts`) only in Phase 7. Do not edit `legacy/` — it is the comparison baseline until cutover.
2. **Google is read-only.** Drive and Sheets are sources only, with read-only scopes. Never write, rename or delete anything in Google, and never run code that would against real credentials without asking.
3. **No Sheets warehouse.** Warehouse code targets DuckDB/MotherDuck only.
4. **Vocabulary:** a unit of pipeline work is a **model** (not "node" or "step"); classes are `Model`, `SourceModel`, `SqlModel`, `PythonModel`.
5. **Keep the design docs true.** If code deliberately departs from `claude/v2-*.md`, or a `Q…` gets answered, update the docs in the same change and say so.
6. **Ask, don't guess, on open questions** (`Q…` in spec §16). Don't invent sheet tab names, division codes or series codes.

## Code conventions

- Python ≥ 3.10, src layout (`src/mrl_pipeline/`), managed with `uv`.
- `transforms/` holds pure functions: pandas in, pandas out; no settings, DuckDB, Google or `print`.
- No work at import time: no connections, no Prefect setup, no `Settings()` singleton.
- Logging, not `print`. Quote every SQL identifier through `naming.quote_ident`; pass values as bound parameters, never by string formatting.
- One implementation per concept; delete superseded code rather than leaving parallel versions.

## Commands

```sh
uv sync                       # install
uv run pytest                 # tests (live Google tests are skipped by default)
uv run pytest -m live         # live tests: the owner runs these; Claude can't in this sandbox
uv run ruff check . && uv run ruff format --check .
```

Before saying a change is done, run `pytest` and `ruff` and report the actual result.

**What Claude can't run here.** The sandbox blocks `config/`, `data/`, `logs/` and `db/`, and nothing may run outside it. So Claude can't run live tests, `mrl run` against real sources or the local `db/` file, or anything that writes to `logs/`. Claude's tests use in-memory DuckDB and a temporary log directory. When a change needs one of the blocked runs to verify it, say so and give the owner the exact command to run.

## Secrets and local files

`config/`, `*.env`, `service.json`, `db/`, `*.duckdb`, `data/` and `logs/` are git-ignored. Never print, log, copy into docs, or commit their contents; don't read files in `config/secrets/` at all. Don't send project data or personal information (athlete names, the owner's email) to outside services.

## Git

The remote is `origin` (GitHub, `EMXC-Skiing/mrl-pipeline`). `main` is protected in practice: changes reach it only through pull requests merged by the owner.

**Branches**
- Never commit directly to `main`. Check the current branch before the first commit of a task; if it's `main`, create a branch first.
- Branch names: `feat/<topic>`, `fix/<topic>`, `docs/<topic>`; for the rewrite, `v2/phase-<n>-<topic>` (e.g. `v2/phase-1-settings`).
- One phase (or one coherent piece of a phase) per branch, so each PR is reviewable.

**The v2 rewrite uses an umbrella branch, `v2/rewrite`.**
- Each phase branch `v2/phase-<n>-…` is created from the current `v2/rewrite` and merged back into it by PR. Phase PRs target `v2/rewrite`, **not** `main`.
- `v2/rewrite` is merged into `main` once, at cutover (Phase 8), by the owner.
- `main` stays on working v1 code until then. Fixes v1 needs in the meantime go on a `fix/…` branch from `main`; afterwards, merge `main` into `v2/rewrite` (ask first).
- Before starting a phase, bring `v2/rewrite` up to date with the merged previous phase, and branch from that.

**Commits**
- Commit only when the owner asks, or at a checkpoint the owner has agreed to for the task. Never commit as a side effect of something else.
- Small, single-purpose commits; don't mix refactors, behavior changes and doc updates without saying so in the message.
- Message: a short imperative summary line (≤ 72 chars, lowercase is fine, matching repo history), a blank line, then what changed and why, with requirement or defect IDs (e.g. `fixes D7; FR-WH-02`).
- Stage files by name (`git add <paths>`), not `git add -A` / `git add .`, and check `git status` and `git diff --staged` before committing so no ignored, secret or unrelated file slips in.
- Tests and ruff should pass at each commit. If committing with failing tests is deliberate (e.g. a work-in-progress checkpoint), say so in the message.
- Never use `--no-verify`, and never amend or rewrite commits that have been pushed.

**Pushing and PRs**
- Ask before every push. Never force-push to a shared branch; never push to `main`.
- Open PRs with `gh pr create` only when asked: against `v2/rewrite` for phase branches, against `main` for everything else. The description lists what changed, the phase and requirement IDs covered, test results, and anything left undone or deliberately skipped.
- Don't merge PRs, delete remote branches, or change GitHub settings.

**Destructive operations** — `reset --hard`, `checkout -- <file>`, `clean`, `branch -D`, `rebase`, `stash drop`: ask first, and say what would be lost. Uncommitted work in this repo may be the owner's.
