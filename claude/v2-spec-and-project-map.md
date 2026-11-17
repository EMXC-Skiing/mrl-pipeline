# MRL Pipeline v2: Specification and Project Map

*Status: draft, 2026-11-17. Written from the `feat/legacy` branch at `93cc07b` plus uncommitted work.*

This document has two parts:

- **Part I: Specification.** What v2 is and how it behaves.
- **Part II: Project map.** How to get from the current code to v2.

The spec was reconstructed from code that is partway through a migration. Where the code already shows a design, the spec follows it. Where the code leaves something undecided, the spec makes a choice and labels it **Decision** so it can be changed. Questions only the project owner can answer are labeled **Open question** and collected in §II.6.

---

## Contents

**Part I: Specification**
1. [Purpose and scope](#1-purpose-and-scope)
2. [Glossary](#2-glossary)
3. [Architecture overview](#3-architecture-overview)
4. [Configuration and environments](#4-configuration-and-environments)
5. [Warehouse backend: DuckDB and MotherDuck](#5-warehouse-backend-duckdb-and-motherduck)
6. [Sources: Google Drive and Sheets, read-only](#6-sources-google-drive-and-sheets-read-only)
7. [Tables: logical names, references and lifecycle](#7-tables-logical-names-references-and-lifecycle)
8. [Transformations](#8-transformations)
9. [The DAG: discovery, selection and planning](#9-the-dag-discovery-selection-and-planning)
10. [Run execution and promotion](#10-run-execution-and-promotion)
11. [Prefect integration and entry points](#11-prefect-integration-and-entry-points)
12. [Node catalogue](#12-node-catalogue)
13. [Metadata and observability](#13-metadata-and-observability)
14. [Testing](#14-testing)
15. [What changes from v1](#15-what-changes-from-v1)

**Part II: Project map**
1. [Where the code stands today](#ii1-where-the-code-stands-today)
2. [Target package layout](#ii2-target-package-layout)
3. [File-by-file disposition](#ii3-file-by-file-disposition)
4. [Phased plan](#ii4-phased-plan)
5. [Risks](#ii5-risks)
6. [Open questions](#ii6-open-questions)

---

# Part I: Specification

## 1. Purpose and scope

### 1.1 What the pipeline does

The Mass Ranking List (MRL) pipeline takes race results from Massachusetts points-eligible Nordic ski races, standardizes them, and calculates athlete penalties, race points and seeds. The ranking logic is already written and working (`src/mrl_pipeline/legacy/`). v2 changes the code around that logic: where tables are stored, how runs are configured, and how steps are ordered and executed.

### 1.2 Goals for v2

| # | Goal |
|---|---|
| G1 | **Every warehouse table lives in DuckDB:** a local file for development, or MotherDuck for shared and production runs. |
| G2 | **The pipeline is a declared DAG.** Each table is built by a node that names its inputs. Run order, selection and partial rebuilds come from that graph. |
| G3 | **Runs are all-or-nothing.** A run writes only to temporary tables and swaps all its outputs in together at the end. A failed run leaves the current tables untouched. |
| G4 | **The environment comes from configuration.** dev, stg and prod are selected with one setting, and secrets come from files, never from function arguments. |
| G5 | **Transform logic doesn't know where tables live.** Transforms refer to logical table names, and the framework maps them to physical tables. |
| G6 | **Results match v1.** For the same inputs, v2 produces the same tables as v1. |

### 1.3 Scope change: Google Sheets is no longer a warehouse backend

The in-progress v1→v2 code treated Google Sheets as one of several interchangeable warehouse backends (`SheetService`, `TableVersionManager`, `SchemaGSheet`). **v2 drops that.** Specifically:

- v2 never writes a warehouse table to Google Sheets, and never reads one back from Sheets.
- There is one warehouse implementation, DuckDB. Local and MotherDuck differ only in how the connection is opened (§5).
- Google Drive and Sheets stay, **as read-only sources**: the hand-maintained prep sheets and the per-race `prep_results*` result sheets (§6).

What this makes simpler:

- The warehouse interface no longer has to fit both SQL and a spreadsheet API. It can assume SQL, transactions and schemas.
- Environments are separated by DuckDB schema alone. Table-name suffixes and per-environment Drive folders for the warehouse go away (§4.3).
- Promotion is always one database transaction. The non-atomic, sheet-by-sheet rename path is gone.
- About 650 lines of Sheets-warehouse code (`SheetService` warehouse methods, `TableVersionManager`, `SchemaGSheet`) can be deleted. Retry logic is still needed for source reads.

### 1.4 Non-goals

- Changing the ranking methods: penalty models, points formulas or seeding rules.
- Publishing outputs back to Google Sheets or any other consumer-facing format. If people currently read warehouse tables in Sheets, a separate export step is needed. That is outside v2; see Open question Q1.
- Supporting warehouse engines other than DuckDB and MotherDuck.
- Incremental or partial-table builds. Every node rebuilds its whole output table on every run.
- Running more than one pipeline process against the same environment at the same time (§10.6).

---

## 2. Glossary

| Term | Meaning |
|---|---|
| **Environment (env)** | One of `dev`, `stg`, `prod`. Decides which DuckDB schema a run reads and writes. |
| **Logical table** | The stable name for a table in the DAG, e.g. `dim_results`. Transforms only ever refer to logical names. |
| **Physical table** | The real DuckDB table backing a logical table at a moment in time, e.g. `prod.dim_results` or `prod.__tmp__dim_results__r20261117t140501`. |
| **Primary** | The current, promoted physical table for a logical table. What downstream users read. |
| **Temp** | A physical table written during a run, not yet promoted. |
| **Archive** | A previous primary, renamed and kept for rollback and auditing. |
| **Node** | One step of the DAG: a source or a transformation that produces one or more logical tables. |
| **Source node** | A node that reads from outside the warehouse (Drive or Sheets) and writes a warehouse table. |
| **Run** | One execution of the flow, identified by a `run_id`. Has its own temp tables and ends in at most one promotion. |
| **`TableRef`** | An immutable value pairing a logical name with a physical name and schema. |
| **`RunContext`** | Per-run state: which temp tables exist, what each logical input resolves to, and the promotion plan. |

---

## 3. Architecture overview

```
                 ┌──────────────────────────────────────────────┐
                 │  Entry points: CLI  /  Prefect deployment    │
                 │  (env, select, flags)                        │
                 └───────────────────────┬──────────────────────┘
                                         │
                       ┌─────────────────▼─────────────────┐
                       │ Settings (pydantic-settings)      │
                       │  config/{env}.env + secrets/      │
                       └─────────────────┬─────────────────┘
                                         │
   ┌─────────────────────────────────────▼──────────────────────────────────────┐
   │ Orchestration                                                              │
   │   Registry ──► DagPlanner (networkx, selectors) ──► Orchestrator.run()     │
   │                                                        │                   │
   │                                                   RunContext               │
   │                                       (resolve inputs, allocate temps,     │
   │                                        record outputs, promotion plan)     │
   └────────────────┬───────────────────────────────────────┬───────────────────┘
                    │ nodes                                  │ TableRefs / SQL / DataFrames
   ┌────────────────▼─────────────────┐        ┌────────────▼─────────────────────┐
   │ Nodes                            │        │ Warehouse (DuckDB only)          │
   │  SourceNode  ──► sources/        │        │  DuckDBWarehouse                 │
   │  SqlTransformation (*.sql)       │───────►│   ├─ LocalDuckDBConnector        │
   │  PythonTransformation            │        │   └─ MotherDuckConnector         │
   │   (wraps legacy pure functions)  │        │  read/write/CTAS/promote(txn)    │
   └────────────────┬─────────────────┘        └──────────────────────────────────┘
                    │
   ┌────────────────▼─────────────────┐
   │ Sources (read-only)              │
   │  DriveSource  (Drive API)        │
   │  SheetsSource (gspread + retry)  │
   │  gviz CSV via DuckDB httpfs      │
   └──────────────────────────────────┘
```

Data moves in one direction: **sources → warehouse temp tables → transforms → warehouse temp tables → promotion**. No node passes DataFrames directly to another node. Every intermediate result is a warehouse table, so any node can be rerun on its own against the current primaries.

---

## 4. Configuration and environments

### 4.1 Settings object

There is one `Settings` class (`mrl_pipeline/settings.py`, built on `pydantic-settings`), and it is the only place configuration is read. Nothing else in the package calls `os.getenv` or `load_dotenv`.

| Field | Type | Default | Notes |
|---|---|---|---|
| `environment_name` | `Literal["dev","stg","prod"]` | `"dev"` | Selects the schema and the env-specific behaviors in §4.4. |
| `warehouse_target` | `Literal["local","motherduck"] \| None` | `None` | **Decision.** An explicit setting. If `None`, it is inferred: `motherduck` when a token is present, otherwise `local`. That matches today's `make_default_connector()`. |
| `local_duckdb_path` | `Path` | `<root>/db/mrl.duckdb` | Used when the target is `local`. `":memory:"` is allowed, for tests. |
| `database_name` | `str` | `"mrl"` | The MotherDuck database. |
| `schema_name` | `str \| None` | `None` | **Decision.** If unset, it is derived as `environment_name`. Set it explicitly only for special cases such as a personal sandbox schema. |
| `archive_retention` | `int` | `5` | How many archives to keep per logical table (§10.5). |
| `dev_sample_races` | `bool` | `True` in dev, otherwise `False` | Replaces the hard-coded `if warehouse_env == "dev"` sampling in `build_dim_races`. |
| `google_drive_source_folder_id` | `str \| None` | `None` | Optional. Limits the `prep_results*` search to one Drive folder (and its subfolders). Replaces `google_drive_warehouse_folder_id`. |
| `motherduck_token` | `SecretStr \| None` | — | Secret. |
| `google_service_account_credentials` | `dict \| None` | — | Secret. Parsed from JSON. |

**Removed:** `drive_table_suffix`, `db_table_suffix`, `google_drive_warehouse_folder_id`, and the `MNP_GOOGLE_DRIVE_WAREHOUSE_FOLDER_IDS` JSON map. All of these only existed for the Sheets warehouse or for name suffixes.

### 4.2 Choosing an environment

**Decision.** Settings load in this order, with later sources overriding earlier ones:

1. Defaults in the class.
2. `config/{env}.env`, where `env` comes from the `MRL_ENV` process variable, or `--env` on the CLI, or the `env` flow parameter. If none of those is set, `config/dev.env` is used.
3. Process environment variables prefixed `MRL_`.
4. Secret files in `config/secrets/`, one file per field.
5. Explicit overrides passed to `load_settings(env=..., **overrides)`.

This adds one function, `load_settings(env: str | None = None, **overrides) -> Settings`, which passes the matching `_env_file` to `Settings`. The module-level `settings` singleton is replaced by an explicit `Settings` object created at the entry point (the CLI or the flow) and passed down. Tests can then build isolated settings without patching environment variables. A `get_settings()` helper can remain for convenience in interactive use.

Guardrails:

- If `environment_name` inside the loaded file doesn't match the requested env, startup fails with an error.
- Running against `prod` on a `local` target logs a warning. Local prod is allowed, for recovery and debugging.
- Settings are logged once at startup with secret values masked.

### 4.3 How environments are kept apart

**Decision.** Each environment is **its own DuckDB schema** (`dev`, `stg`, `prod`) inside one database, either the local file or the MotherDuck database `mrl`. **Table names carry no environment suffix.**

| | dev | stg | prod |
|---|---|---|---|
| Schema | `dev` | `stg` | `prod` |
| Primary table | `dev.dim_results` | `stg.dim_results` | `prod.dim_results` |

Why: when the warehouse was Sheets, there were no schemas, so the environment had to be put in the name. DuckDB has schemas, so keeping the suffix as well would just repeat the environment in two places (`dev."dim_results_dev"`). With one rule, a SQL template works the same in every environment.

### 4.4 Environment-specific behavior

Code never checks the environment name directly. Anything that should behave differently per environment gets its own setting (such as `dev_sample_races`), and each `config/{env}.env` sets it. This makes every behavior difference visible in config and testable.

### 4.5 Files in `config/`

```
config/
├── dev.env
├── stg.env
├── prod.env
└── secrets/
    ├── MRL_MOTHERDUCK_TOKEN
    └── MRL_GOOGLE_SERVICE_ACCOUNT_CREDENTIALS
```

`config/` is already git-ignored. A committed `config.example/` with placeholder values documents the expected layout.

---

## 5. Warehouse backend: DuckDB and MotherDuck

### 5.1 Connectors

The connectors in `io/database_connectors.py` stay as they are in concept:

| Connector | Connects to | Setup on connect |
|---|---|---|
| `LocalDuckDBConnector` | `duckdb.connect(local_duckdb_path)` | Load `httpfs`; `CREATE SCHEMA IF NOT EXISTS <schema>`; `SET schema` |
| `MotherDuckConnector` | `duckdb.connect("md:<database_name>")` with the token | Same |

`make_connector(settings)` picks one based on `warehouse_target` (§4.1). Connectors only handle connecting. They don't decide table names.

Changes from the current code:

- Pass the MotherDuck token in the connection config (`duckdb.connect("md:mrl", config={"motherduck_token": ...})`) or the `motherduck_token` env var. Setting it after `connect` is too late, because the `md:` connection authenticates when it opens.
- Loading `httpfs` failures are logged, not silently ignored. A source node that needs `httpfs` checks for it and raises an error if it's missing.
- Pin the `duckdb` version (already `==1.3.2`), and keep it matched to MotherDuck's supported client versions.

### 5.2 The `Warehouse` interface

The interface below is cut down from `warehouse/warehouse_backend.py`. Because DuckDB is the only engine, it is a **concrete class** (`DuckDBWarehouse`) rather than an ABC with one subclass. A `typing.Protocol` with the same methods can be defined for test doubles.

```python
class DuckDBWarehouse:
    def __init__(self, connector: DatabaseConnector, schema: str): ...

    # connection
    def connection(self) -> ContextManager[DuckDBPyConnection]   # one shared conn per run, see §10.6

    # identifiers
    def quote_ident(self, ident: str) -> str                     # "x" with "" escaping
    def qualify(self, ref: TableRef) -> str                      # "schema"."physical_name"

    # catalog
    def exists(self, ref: TableRef) -> bool                      # information_schema lookup, UNQUOTED names
    def list_tables(self, pattern: str | None = None) -> list[str]
    def summarize(self, ref: TableRef) -> TableSummary           # row/col counts, columns, dtypes

    # data
    def read_table(self, ref, *, columns=None, where=None, limit=None) -> DataFrame
    def write_table(self, ref, df_or_iterable, *, mode: Literal["replace","append","fail"]="replace") -> None
    def create_table_as_select(self, ref: TableRef, sql: str) -> None
    def execute(self, sql: str, params: Sequence | Mapping | None = None) -> None
    def render(self, template: str, refs: Mapping[str, TableRef]) -> str   # §8.3

    # lifecycle
    def drop(self, ref, *, if_exists=True) -> None
    def rename(self, src: TableRef, dst: TableRef) -> None
    def promote(self, plan: PromotionPlan) -> None               # one transaction, §10.4
    def comment(self, ref: TableRef, table: str | None, columns: Mapping[str, str]) -> None  # §13.1
```

Rules:

- Physical names are stored **unquoted** in `TableRef`. Quoting happens only in `quote_ident` and `qualify`, at the moment SQL is built. This fixes the current double-quoting bug (§II.1).
- Every SQL statement the warehouse builds uses `qualify()`. Nothing depends on the session's default schema, so a stray `SET schema` can't send writes to the wrong place.
- `write_table` registers the DataFrame under a unique view name, runs `CREATE TABLE … AS SELECT` (or `INSERT INTO … BY NAME` for append), then unregisters the view. This is how `DuckDBWarehouseService._write_dataframe` already works.

### 5.3 Local and MotherDuck behave the same

Running the same code with the same settings, except `warehouse_target`, produces the same tables. The only expected differences are speed and the URL shown in logs (`duckdb:///path#schema.table` versus `md:mrl#schema.table`).

---

## 6. Sources: Google Drive and Sheets, read-only

Google is used only as an input. Source access sits in a `sources/` package that **only reads**. Nothing in v2 creates, renames or deletes Drive files.

| Component | Built from | Responsibility |
|---|---|---|
| `GoogleCredentials` | the credential parsing in `DriveService` and `SheetService` | Build one `google.oauth2` credential from `settings.google_service_account_credentials`, with read-only scopes (`drive.metadata.readonly`, `spreadsheets.readonly`). |
| `DriveSource` | `io/drive_service.py` | List spreadsheets by name prefix, optionally limited to a folder (recursive), and return `race_id, file_id, name, modified_at, source_type`. Raise an error on duplicate titles. |
| `SheetsSource` | the `get_prep_table` and `iter_prep_tables` parts of `SheetService` / legacy `DataConnector` | Read one worksheet, or every worksheet, from a named prep sheet as DataFrames. Uses `ResilientGspreadClient` for retries. |
| gviz CSV reader | `models/stg_results.py`, legacy `process_one_sheet_into_duckdb` | DuckDB `read_csv('https://docs.google.com/spreadsheets/d/<id>/gviz/tq?tqx=out:csv', all_varchar=true)` over `httpfs`. |

Source reads happen **only inside source nodes** (§8.2). Transformations never call Google.

Prep sheets the legacy code reads, each of which becomes a source node:

| Prep sheet | Read how | Used by |
|---|---|---|
| `prep_all_races` | every worksheet, one per season | `dim_races` |
| `prep_results*` (one per race and gender) | Drive listing, then gviz CSV | `infra_result_files`, `stg_results` |
| names-equivalent-spellings | single worksheet | `dim_results`, `dim_registrations` |
| schools-equivalent-spellings | single worksheet | `dim_results`, `dim_registrations` |
| non-person competitor names | single worksheet | `dim_results` |
| `prep_penalty_publication_dates` | single worksheet | `ext_races_by_penalty_window` |
| `prep_penalty_contribution_windows` | single worksheet | `ext_races_by_penalty_window` |
| `prep_penalty_contributions` | single worksheet | `ext_results` |
| registrations export (`stg_registrations`) | **Open question Q2** | `dim_registrations` |

The exact titles of the three name-replacement sheets are defined where the legacy functions are called, not in the functions themselves. Confirm them in Phase 4.

---

## 7. Tables: logical names, references and lifecycle

### 7.1 `TableRef`

```python
@dataclass(frozen=True)
class TableRef:
    logical_name: str      # "dim_results"
    physical_name: str     # "dim_results" | "__tmp__dim_results__r20261117t140501" | "_archive__dim_results__20261117t140501"
    schema: str            # "prod"
    role: Literal["primary", "temp", "archive"]
```

The `backend` field from the current draft is dropped, because there is only one backend.

### 7.2 Naming rules

**Decision.** Temp names include the run ID, so they are unique per run (following the `"__tmp_c__run123"` example in the current `TableRef` docstring). No names include the environment.

| Role | Pattern | Example |
|---|---|---|
| primary | `{base}` | `dim_results` |
| temp | `__tmp__{base}__{run_id}` | `__tmp__dim_results__r20261117t140501` |
| archive | `_archive__{base}__{run_ts}` | `_archive__dim_results__20261117t140501` |

- `base` is `sanitize_table_name(logical_name)`: lowercased, with characters outside `[a-z0-9_]` removed. A `base` that ends up empty raises an error. The current pattern also allows `+`, which is not a valid unquoted SQL identifier, so v2 drops it.
- `run_id` is `r` followed by the UTC start time as `YYYYMMDDtHHMMSS`, plus a 4-character random suffix if two runs start in the same second.
- `run_ts` is the run's start time in the same format, so every archive from one run shares a timestamp and can be found together.
- All names are compared and looked up **unquoted**.
- The `__tmp__` and `_archive__` prefixes are reserved. A logical name may not start with `_`.

`TableVersionRef` is reduced to a naming function, `table_names(logical_name, run) -> dict[role, TableRef]`. It no longer reads settings or quotes anything.

### 7.3 Lifecycle of a table within a run

```
          allocate_output_temp            node succeeds               promote (txn)
 (none) ─────────────────────► ALLOCATED ──────────────► PRODUCED ─────────────────► PROMOTED
                                   │                          │
                                   │ node fails               │ run fails / aborted
                                   ▼                          ▼
                                FAILED                    ABANDONED  (temp kept for debugging,
                                                                      dropped by `mrl clean`)
```

This replaces the `empty → temp → promoted` states in `io/warehouse_table.py`. The state machine belongs to `RunContext` (§10.2) and is tracked per logical output. `WarehouseTable` as a separate class is dropped.

---

## 8. Transformations

### 8.1 The `Node` contract

```python
class Node(ABC):
    name: str                                  # unique; defaults to the single output's name
    inputs: Sequence[str]                      # logical names read
    outputs: Sequence[str]                     # logical names written (≥1)
    description: str
    column_descriptions: Mapping[str, Mapping[str, str]]   # output -> column -> text
    tags: Sequence[str] = ()                   # e.g. "source", "penalties"; usable in selectors

    @abstractmethod
    def run(self, ctx: NodeContext) -> None: ...
```

`NodeContext` gives a node only what it needs: `warehouse`, `inputs: Mapping[str, TableRef]`, `outputs: Mapping[str, TableRef]` (the temp targets), `settings`, `sources` (for source nodes only) and `logger`.

**Decision: a node can produce several tables.** Several legacy functions return more than one DataFrame (`build_penalties`, `build_stg_results_and_dim_races`, and the paired `build_dim_results`/`build_dim_registrations`). Splitting them would either repeat expensive work (the penalty model fit) or change the logic. So `outputs` is a list, and the DAG is built over tables (§9.1).

### 8.2 Node types

| Type | `run()` behavior | Used for |
|---|---|---|
| `SourceNode` | Subclass implements `extract(sources, settings) -> DataFrame \| dict[str, DataFrame]`. The framework writes the result to the temp outputs. `inputs` is empty, or limited to warehouse tables the node needs, such as `infra_result_files` for `stg_results`. | Prep sheets, the Drive listing, result CSVs |
| `SqlTransformation` | Renders a `.sql` template with the resolved input refs, then `create_table_as_select(output, sql)`. Exactly one output. | `ext_results`, `ext_points_contributing_results`, `ext_registrations` |
| `PythonTransformation` | The framework reads each input with `read_table`, then calls `compute(inputs: dict[str, DataFrame], settings) -> DataFrame \| dict[str, DataFrame]`, then writes each output with `write_table`. | The pandas-based legacy logic |

A node that returns a single DataFrame must declare exactly one output. A node that returns a dict must return exactly the declared outputs, no more and no fewer. Anything else is an error.

`stg_results` is a special case. It reads every result CSV straight into DuckDB (`read_csv` over `httpfs`) to avoid pulling hundreds of sheets through pandas. It's written as a `SourceNode` that overrides `run()` and uses `ctx.warehouse.execute` directly against `ctx.outputs["stg_results"]`. Overriding `run()` is allowed for any node, as long as it only writes to `ctx.outputs`.

### 8.3 SQL templates

- Templates sit next to their node as `.sql` files, loaded with `importlib.resources`, as `legacy/build_ext_*.py` already does.
- Inputs are referenced as `{{ ref('dim_results') }}`. `warehouse.render()` replaces each one with `qualify(input_ref)`. Rendering is plain string substitution (a regex for `ref('…')`), not Jinja, so no extra dependency is needed.
- Rendering fails if a template references a table that isn't listed in `inputs`, or lists an input it never references. This keeps the DAG edges honest.
- When porting, the legacy templates' bare names (`df_ext_results`, `df_dim_registrations`, …) become `{{ ref('ext_results') }}`, `{{ ref('dim_registrations') }}` and so on. Nothing else in the SQL changes.
- Templates must be one `SELECT` (CTEs allowed). DDL is not allowed. The framework wraps the query in `CREATE TABLE … AS`.

### 8.4 Wrapping the legacy functions

The legacy functions are already pure (DataFrames in, DataFrames out). The port moves them into `transforms/` unchanged where possible and gives each a small `PythonTransformation` wrapper that:

1. maps logical inputs to the function's parameter names,
2. turns settings into keyword arguments (e.g. `dev_sample_races`),
3. maps the return value to the named outputs.

Logic changes are kept to what the framework requires: removing direct IO (for example, `build_dim_races` taking `data` and calling `iter_prep_tables` becomes taking a `prep_all_races` DataFrame) and removing environment-name checks.

---

## 9. The DAG: discovery, selection and planning

### 9.1 Building the graph

- **Registry.** Nodes are registered explicitly in `nodes/__init__.py` as a `NODES: list[Node]`. **Decision:** this replaces discovery by importing every module (`pkgutil` in `model_selector.py`). It makes order and membership visible, and avoids import side effects at discovery time.
- **Table graph.** For each node, an edge goes from every input table to every output table. Each logical table has exactly one producer, and a second producer is an error at registration. An input with no producer is an error, unless it's listed in `EXTERNAL_TABLES` (tables expected to exist already; none are planned today).
- **Node graph.** Built from the table graph: node A comes before node B if B reads a table A produces. This is the graph that gets executed.
- Cycles are reported at registration with the full cycle path.

### 9.2 Selectors

The selector grammar from `model_selector.py` is kept, extended to tags:

| Selector | Meaning |
|---|---|
| `dim_results` | the node that produces `dim_results` (a table name or node name) |
| `+dim_results` | it plus everything upstream |
| `dim_results+` | it plus everything downstream |
| `+dim_results+` | both directions |
| `tag:penalties` | every node tagged `penalties` |
| `a b` | union (space-separated) |
| `a,b` | intersection (comma-separated, within one union group) |
| *(empty)* | every node |

`--exclude <selector>` (same grammar) is removed from the result. A selection that matches nothing is an error.

### 9.3 Planning

`plan(selection) -> Plan` returns:

- `nodes`: the selected nodes in a deterministic topological order, with registry order as the tiebreak (`lexicographical_topological_sort`, as today);
- for each input of each node, whether it comes from **this run** (an upstream selected node) or from the **current primary** (an upstream node that wasn't selected);
- the list of primaries that must already exist. The plan fails before any work starts if one is missing, e.g. selecting `ext_results` alone in a fresh schema.

`mrl plan …` (§11.2) prints the plan without running it.

---

## 10. Run execution and promotion

### 10.1 Steps of a run

1. **Setup.** Load settings, create the connector and warehouse, create the `RunContext` (with `run_id` and `run_ts`), write a `started` row to `_mrl_runs` (§13.2).
2. **Plan.** §9.3. If any required primaries are missing, the run fails here.
3. **Execute** the nodes in plan order. For each node:
   - `inputs = {t: ctx.resolve_input(t)}`. This returns the run's temp table if the run already produced `t`, and the primary otherwise.
   - `outputs = {t: ctx.allocate_output_temp(t)}`
   - `node.run(NodeContext(...))`
   - **Validate** each output (§10.3).
   - `ctx.record_produced(t)` for each output, and write a row to `_mrl_node_runs`.
4. **Promote** (§10.4), unless `--no-promote` was passed.
5. **Clean up.** Prune archives past `archive_retention` (§10.5) and write a `succeeded` row to `_mrl_runs`.

If a node fails, the nodes that depend on it are skipped. Independent branches may still run (useful for debugging), but **the run as a whole fails and nothing is promoted.** There is no partial promotion.

### 10.2 `RunContext`

This implements the abstract class in `orchestration/orchestration.py`:

| Method | Behavior |
|---|---|
| `run_id`, `run_ts` | Fixed when the context is created. |
| `resolve_input(logical) -> TableRef` | The run's temp table if the run has produced `logical`, otherwise the primary. Raises an error if the primary doesn't exist. |
| `allocate_output_temp(logical) -> TableRef` | Returns the temp ref and drops any leftover table with the same name. Marks the table `ALLOCATED`. |
| `record_produced(logical)` | Requires the table to be `ALLOCATED` and to exist. Marks it `PRODUCED`. |
| `promotion_plan() -> PromotionPlan` | One entry per `PRODUCED` table: (temp, primary, archive). |
| `promote(warehouse, archive=True)` | Calls `warehouse.promote(plan)`, then marks the tables `PROMOTED`. |

### 10.3 Checking outputs before promotion

Every output must pass these checks before it can be promoted:

- the table exists and has at least one column;
- if the node declares `column_descriptions` for that output, every declared column is present (extra columns are allowed and logged);
- if the node sets `allow_empty = False` (the default for everything except `stg_*`), the row count is greater than 0.

Nodes may define `checks(ctx) -> list[str]` for their own assertions, such as uniqueness of `result_id`. A non-empty list fails the node. This is also where the legacy ad-hoc validations (`test_one_to_one_race_gender_to_sheet`, the duplicate-title checks) end up.

### 10.4 Promotion

Promotion runs **inside one DuckDB transaction**. This follows `DuckDBWarehouseService.promote_temp_tables`, extended so it can handle several outputs:

```sql
BEGIN TRANSACTION;
-- for each (temp, primary, archive) in plan:
ALTER TABLE "prod"."dim_results" RENAME TO "_archive__dim_results__20261117t140501";  -- if primary exists and archive=True
-- or: DROP TABLE "prod"."dim_results";                                               -- if archive=False
ALTER TABLE "prod"."__tmp__dim_results__r20261117t140501" RENAME TO "dim_results";
-- ...
COMMIT;      -- on any error: ROLLBACK, re-raise, run marked failed, temps retained
```

- Either every output from the run is promoted, or none is.
- No views are defined on primary tables, so renames don't break any dependencies. If views are added later, they must be recreated after promotion.
- Phase 2 must check that DuckDB 1.3.x and MotherDuck both support renaming inside a transaction (risk R1). If not, the fallback is a "swap schema": create `"{schema}__staging_{run_id}"`, then swap tables within a short transaction.

### 10.5 Archives and retention

- Archives stay in the same schema as the primary, which keeps renames cheap.
- After a successful promotion, archives past the `archive_retention` most recent for each logical table are dropped.
- **Rolling back** (`mrl rollback <table> [--to <run_ts>]`) renames the current primary to an archive name and an archive back to the primary, in one transaction. Rolling back a whole run (`--run <run_id>`) does this for every table that run promoted, using `_mrl_node_runs`.
- `mrl clean` drops abandoned `__tmp__*` tables from runs that are no longer active.

### 10.6 Concurrency

- **Within a run:** the run uses one DuckDB connection. Nodes run in sequence by default. Inside a node, threads (e.g. the parallel CSV reads in `stg_results`) use `conn.cursor()` from that one connection, as `models/stg_results.py` already does. Running independent nodes in parallel is an optional later improvement and is off by default.
- **Between runs:** one run per environment at a time. The local DuckDB file only allows one writing process anyway. For MotherDuck, a `_mrl_runs` row with status `started` and a heartbeat less than 30 minutes old blocks a new run unless `--force` is passed.
- Different environments can run at the same time on MotherDuck, since they use different schemas. On a local file they can't.

---

## 11. Prefect integration and entry points

### 11.1 Prefect

- One flow, `mrl_pipeline(env: str = "dev", select: str | None = None, exclude: str | None = None, promote: bool = True, archive: bool = True)`.
- The flow does steps 1, 2, 4 and 5 of §10.1 itself. Step 3 submits **one Prefect task per node**, named after the node, with `wait_for` set to the upstream tasks in the plan, as `flows.py` does today. Tasks return small metadata dicts (row counts, timing), never DataFrames.
- Task retries are off by default. A source node may set `retries=2` for Google transport errors.
- **Building the registry and plan must have no side effects at import time.** Today `flows.py` calls `load_models()` and `build()` when the module is imported. In v2, the plan is built inside the flow.
- Prefect is the default runner, not a requirement. `Orchestrator.run()` works without Prefect, for tests and the CLI's `--no-prefect` mode.

### 11.2 CLI

This adds a console script, `mrl`, defined in `pyproject.toml` `[project.scripts]` and implemented with `argparse` so no dependency is added:

```
mrl run      [--env dev|stg|prod] [--select SEL] [--exclude SEL] [--no-promote] [--no-archive] [--no-prefect] [--force]
mrl plan     [--env …] [--select …] [--exclude …]
mrl list     [--env …]                         # nodes, tables, tags, primary row counts
mrl describe <table> [--env …]                 # columns, comments, last run, archives
mrl rollback <table> [--to RUN_TS] | --run RUN_ID [--env …]
mrl clean    [--env …]                         # drop abandoned temps
mrl compare  <table> --legacy-sheet <title> [--env …]   # parity helper, Phase 6
```

---

## 12. Node catalogue

These are the target nodes, in registry order. "From" names the legacy function or current module being ported.

| # | Node | Type | Inputs | Outputs | From |
|---|---|---|---|---|---|
| 1 | `prep_all_races` | Source | — | `prep_all_races` | `iter_prep_tables("prep_all_races")`, concatenated, with a `race_season` column |
| 2 | `prep_name_replacements` | Source | — | `prep_names_equivalent_spellings`, `prep_schools_equivalent_spellings`, `prep_non_people` | the inputs to `build_name_replacement_artifacts` |
| 3 | `prep_penalty_config` | Source | — | `prep_penalty_publication_dates`, `prep_penalty_contribution_windows`, `prep_penalty_contributions` | prep sheets |
| 4 | `stg_registrations` | Source | — | `stg_registrations` | **Q2** |
| 5 | `dim_races` | Python | `prep_all_races` | `dim_races` | `build_dim_races` |
| 6 | `infra_result_files` | Source | — | `infra_result_files` | `models/infra_result_files.py`, legacy `DriveService.get_gsheets_by_prefix` |
| 7 | `stg_results` | Source (custom `run`) | `infra_result_files`, `dim_races` | `stg_results` | `models/stg_results.py` and legacy `build_stg_results_and_dim_races` (resolve, validate, gviz CSV, standardize, union) |
| 8 | `dim_results_and_registrations` | Python | `stg_results`, `dim_races`, `stg_registrations`, `prep_names_equivalent_spellings`, `prep_schools_equivalent_spellings`, `prep_non_people` | `dim_results`, `dim_registrations` | `build_dim_results`, `build_dim_registrations` |
| 9 | `dim_athletes` | Python | `dim_registrations` | `dim_athletes` | `build_dim_athletes` |
| 10 | `ext_races_by_penalty_window` | Python | `prep_penalty_publication_dates`, `prep_penalty_contribution_windows`, `dim_races` | `ext_races_by_penalty_window` | `build_ext_races_by_penalty_window` |
| 11 | `penalties` | Python | `dim_registrations`, `dim_results`, `ext_races_by_penalty_window` | `ext_results_by_penalty_window`, `ext_athlete_penalties` | `build_penalties` |
| 12 | `ext_results` | SQL | `prep_penalty_contributions`, `ext_athlete_penalties`, `dim_results`, `dim_races`, `dim_registrations` | `ext_results` | `build_ext_results.sql` |
| 13 | `ext_points_contributing_results` | SQL | `ext_results`, `dim_registrations` | `ext_points_contributing_results` | `build_ext_points_contributing_results.sql` |
| 14 | `ext_registrations` | SQL | `ext_points_contributing_results`, `dim_registrations` | `ext_registrations` | `build_ext_registrations.sql` |

Notes:

- The legacy `build_stg_results_and_dim_races` both built `dim_races` and loaded the results. v2 splits it into node 5 (pure) and node 7 (source), and adds the results-sheet URLs to `dim_races` in a later step, or keeps them in `stg_results`. **Decision:** the URLs stay on `stg_results` / `infra_result_files`, and `dim_races` doesn't read from Drive. If downstream SQL needs `prep_results_url_*` on `dim_races`, add a small `dim_races_enriched` SQL node.
- Node 7 should combine both current versions: the legacy duplicate-title and missing-sheet checks, and the `column_map` standardization from `models/stg_results.py`. The two versions differ (the legacy one is driven by `dim_races` and records race and gender; the current one is driven by `infra_result_files`). **Decision:** the legacy behavior is the source of truth for parity, because its output is what the downstream legacy transforms expect.

---

## 13. Metadata and observability

### 13.1 Documentation stored in the database

After writing each output, the framework runs `COMMENT ON TABLE` with the node's `description` and `COMMENT ON COLUMN` with each `column_descriptions` entry, on the temp table. DuckDB keeps comments through renames, so they're carried over at promotion. Documentation is then available to anyone querying MotherDuck through `duckdb_columns()`. This replaces the schema/description tab that `SchemaGSheet` wrote in Sheets.

### 13.2 Run log tables

Two tables per schema record runs, and are written outside the promotion transaction:

| Table | Columns |
|---|---|
| `_mrl_runs` | `run_id, env, started_at, heartbeat_at, finished_at, status (started/succeeded/failed/aborted), selection, promoted (bool), git_sha, package_version, target (local/motherduck), error` |
| `_mrl_node_runs` | `run_id, node, output, temp_name, started_at, finished_at, status, row_count, column_count, error` |

### 13.3 Logging

- Use the Prefect run logger inside the flow, and Python `logging` otherwise. `print` goes away, including in the ported legacy code.
- One line per node: `node=dim_results status=ok rows=12345 cols=21 secs=3.2`.
- Warnings from source checks (orphaned sheets, missing resolutions, stale penalty dates) are logged at `WARNING` level and also counted in `_mrl_node_runs.error` as a short summary.

---

## 14. Testing

| Layer | Approach |
|---|---|
| Naming, selectors, planning | Pure unit tests. No database needed. |
| `DuckDBWarehouse` | Tests against `:memory:`: write, read, CTAS, exists, rename, promotion with a failure injected partway (to check rollback), quoting of hostile identifiers. |
| `RunContext` / orchestrator | Small fake nodes in a `:memory:` warehouse, checking that inputs resolve to temp or primary as expected, failure stops downstream nodes, nothing is promoted on failure, and multi-output nodes work. |
| Transforms | Each legacy function gets a fixture test. Small input DataFrames are saved as Parquet under `tests/fixtures/`, along with the expected outputs. |
| SQL nodes | The same fixtures, loaded into `:memory:`, with the node run through the framework. |
| Sources | A `FakeSources` object returning saved DataFrames and local CSV paths, so no Google access is needed in CI. One `@pytest.mark.live` smoke test uses real credentials and is skipped by default. |
| End to end | `mrl run --env test` against `:memory:` using `FakeSources`. The fixtures need to be small but complete. |
| Parity | §II.4 Phase 6. Not part of CI. |

`pytest` moves from runtime dependencies to a `dev` dependency group.

---

## 15. What changes from v1

| Area | v1 (legacy) | v2 |
|---|---|---|
| Warehouse storage | Google Sheets: one spreadsheet per table per environment folder | DuckDB only, as a local file or MotherDuck |
| Google's role | Both source and warehouse | **Read-only source** |
| Promotion | Sheets renamed one at a time, not atomic | One DuckDB transaction across every output of the run |
| Temp naming | `__{t}_TEMP{_env}`, shared by every run | `__tmp__{t}__{run_id}`, unique per run |
| Keeping environments apart | Folder per environment, plus a name suffix | Schema per environment, no suffix |
| Archive timestamp | America/New_York, `%y-%m-%d_%H:%M` | UTC, `YYYYMMDDtHHMMSS`, shared by the whole run |
| Configuration | Credentials and folder-ID JSON passed as arguments, `os.getenv`, `.env` | One `Settings`; env file chosen by `MRL_ENV`; secret files |
| Orchestration | Calls in a handwritten script | Registry → table DAG → Prefect tasks, with dbt-style selectors |
| Running only part of the pipeline | Not possible | `--select`, with unselected inputs read from current primaries |
| Where SQL transforms run | Throwaway in-memory DuckDB on DataFrames | In the warehouse, as `CREATE TABLE … AS SELECT` |
| Environment-specific logic | `if warehouse_env == "dev"` in transforms | Named settings per environment |
| Table and column documentation | Schema tab in each Sheet | `COMMENT ON` in DuckDB |
| Record of runs | stdout | `_mrl_runs`, `_mrl_node_runs` |
| Rollback | Manual renaming in Drive | `mrl rollback` |

---

# Part II: Project map

## II.1 Where the code stands today

The package **doesn't import** right now. The known problems, all of which are fixed somewhere in the plan below:

| # | Problem | Where | Fixed in |
|---|---|---|---|
| D1 | `models/pipeline_model.py` was deleted in `93cc07b`, but `models/__init__.py`, `infra_result_files.py` and `stg_results.py` still import `PipelineModel` from it | `models/` | Phase 0 |
| D2 | `model_selector.py` imports `PipelineModel` from `warehouse_operation`, which now only defines `WarehouseOperation` | `orchestration/model_selector.py` | Phase 0 |
| D3 | `warehouse_operation.py` imports `make_table_ref` from `duckdb_service.py`, which doesn't define it | `models/warehouse_operation.py` | Phase 0 |
| D4 | `WarehouseTable` calls `TableVersionRef(table_name=…)`, but the parameter is `base_name` | `io/warehouse_table.py` | Phase 2 (file deleted) |
| D5 | Identifiers are quoted twice: `TableVersionRef` returns quoted names, which `DuckDBWarehouseService` quotes again and also compares against unquoted `information_schema` values, so `_table_exists` is always false | `io/table_version_ref.py`, `io/duckdb_service.py` | Phase 2 |
| D6 | `DuckDBWarehouseService.__init__` requires `schema` as a keyword with no default, and doesn't use `settings.schema_name` | `io/duckdb_service.py` | Phase 2 |
| D7 | The MotherDuck token is set after `connect("md:…")`, too late to authenticate | `io/database_connectors.py` | Phase 1 |
| D8 | `sklearn` (used by `build_penalties`), `numpy` and `python-dotenv` aren't declared in `pyproject.toml`. `pytest` is listed as a runtime dependency | `pyproject.toml` | Phase 0 |
| D9 | `requires-python = ">=3.9"`, but pydantic fields use `str \| None`, which needs Python 3.10+ at runtime | `pyproject.toml` | Phase 0 |
| D10 | `Settings` has no `env_file`, so `config/*.env` files are never loaded | `settings.py` | Phase 1 |
| D11 | Two `WarehouseService`s, two `TableSummary`s, two `DriveService`s | `io/`, `warehouse/`, `legacy/` | Phases 2 and 4 |
| D12 | `flows.py` builds every Prefect task when the module is imported | `orchestration/flows.py` | Phase 5 |
| D13 | `orchestration/__init__.py` imports `.orchestration`, which imports back from `mrl_pipeline.orchestration`. This only works because of import order | `orchestration/` | Phase 3 |

## II.2 Target package layout

```
src/mrl_pipeline/
├── __init__.py
├── settings.py                 # Settings, load_settings()                    (§4)
├── cli.py                      # `mrl` entry point                             (§11.2)
├── naming.py                   # sanitize, quote, table_names(), run ids       (§7.2)
├── warehouse/
│   ├── __init__.py
│   ├── connectors.py           # Local / MotherDuck, make_connector()          (§5.1)
│   ├── duckdb_warehouse.py     # DuckDBWarehouse                               (§5.2)
│   ├── refs.py                 # TableRef, TableSummary, PromotionPlan         (§7.1)
│   └── metadata.py             # _mrl_runs / _mrl_node_runs, COMMENT ON        (§13)
├── sources/
│   ├── __init__.py             # Sources facade passed to SourceNodes
│   ├── credentials.py
│   ├── drive.py                # DriveSource
│   ├── sheets.py               # SheetsSource
│   └── resilient_gspread.py    # (moved from io/google_sheets_warehouse_manager/)
├── orchestration/
│   ├── __init__.py
│   ├── node.py                 # Node, SourceNode, SqlTransformation, PythonTransformation, NodeContext
│   ├── registry.py             # registration, table/node graph construction  (§9.1)
│   ├── selector.py             # selector grammar                              (§9.2)
│   ├── planner.py              # Plan                                          (§9.3)
│   ├── run_context.py          # RunContext                                    (§10.2)
│   ├── orchestrator.py         # Orchestrator.run()                            (§10.1)
│   └── flow.py                 # Prefect flow                                  (§11.1)
├── nodes/
│   ├── __init__.py             # NODES = [...] in registry order              (§12)
│   ├── sources.py              # prep_* , infra_result_files, stg_registrations
│   ├── stg_results.py
│   ├── dims.py                 # dim_races, dim_results_and_registrations, dim_athletes
│   ├── penalties.py            # ext_races_by_penalty_window, penalties
│   └── sql/
│       ├── ext_results.sql
│       ├── ext_points_contributing_results.sql
│       └── ext_registrations.sql
├── transforms/                 # pure functions ported from legacy/, no IO
│   ├── dim_races.py
│   ├── dim_results_registrations.py
│   ├── dim_athletes.py
│   ├── penalty_windows.py
│   ├── penalties.py
│   └── stg_results.py          # resolve/validate/standardize helpers
└── legacy/                     # unchanged until Phase 7; reference for parity
tests/
├── fixtures/
├── unit/
├── integration/
└── live/
```

## II.3 File-by-file disposition

| Current file | Disposition | Destination / notes |
|---|---|---|
| `settings.py` | **Rewrite** | §4. Remove the suffix and warehouse-folder fields; add `load_settings`. |
| `utils.py` | **Delete** | Paths go into `settings.py`; `fetch_environ` is replaced by `Settings`. |
| `io/database_connectors.py` | **Move and fix** | `warehouse/connectors.py`; fix D7. |
| `io/duckdb_service.py` | **Rework** | Becomes `warehouse/duckdb_warehouse.py`. Keep `_write_dataframe`, the transaction pattern in `promote_temp_tables` and `_iter_dataframes`. Drop the `temp_tables` tracking (moves to `RunContext`), `_extract_database_path`, `_prep_table_name` and the env suffix. |
| `io/warehouse_service.py` | **Delete** | Its abstract interface only existed to make Sheets and DuckDB interchangeable. |
| `io/warehouse_table.py` | **Delete** | Its state machine moves to `RunContext` (§7.3). |
| `io/table_version_ref.py` | **Replace** | `naming.table_names()` (§7.2). |
| `io/utils.py` | **Move** | `naming.py`, without `+` in the allowed characters. |
| `io/drive_service.py` | **Move** | `sources/drive.py`; credentials go into `sources/credentials.py`. |
| `io/google_sheets_warehouse_manager/sheet_service.py` | **Split** | `get_prep_table` and `iter_prep_tables` move to `sources/sheets.py`. **Delete** `TableVersionManager`, `put/get_warehouse_table` and `promote_temp_tables`. |
| `io/google_sheets_warehouse_manager/schema_gsheet.py` | **Delete** | Only used for Sheets warehouse tables. |
| `io/google_sheets_warehouse_manager/resilient_gspread_client.py` | **Move** | `sources/resilient_gspread.py`. |
| `warehouse/warehouse_backend.py` | **Fold in** | `TableRef` and `TableSummary` go to `warehouse/refs.py`; the method list becomes `DuckDBWarehouse` (§5.2). |
| `orchestration/transformation.py` | **Rework** | `orchestration/node.py`, with multiple outputs and `SourceNode` (§8). |
| `orchestration/orchestration.py` | **Implement** | Its abstract classes become `run_context.py` and `orchestrator.py`. |
| `orchestration/model_selector.py` | **Split** | Selector grammar → `selector.py`; graph → `registry.py`; topological sort → `planner.py`. Drop `pkgutil` discovery. |
| `orchestration/flows.py` | **Rewrite** | `flow.py` (§11.1); fix D12. |
| `models/warehouse_operation.py` | **Delete** | Replaced by `Node`. |
| `models/infra_result_files.py` | **Port** | `nodes/sources.py`. |
| `models/stg_results.py` | **Port** | `nodes/stg_results.py`, combined with the legacy validations (§12 note). |
| `models/__init__.py` | **Delete** | Replaced by `nodes/__init__.py`. |
| `legacy/build_*.py` | **Port, then delete** | `transforms/*.py` and `nodes/*.py`. |
| `legacy/build_*.sql` | **Port, then delete** | `nodes/sql/*.sql`, with `{{ ref() }}` substitution. |
| `legacy/sheets_service.py` | **Delete** | Its read path is already ported to `sources/`. |
| `pyproject.toml` | **Edit** | Fix D8 and D9; add `[project.scripts] mrl = "mrl_pipeline.cli:main"`; add a dev dependency group. |

## II.4 Phased plan

Each phase ends with something runnable and testable. Phases 1–3 don't depend on Google access, so they can be developed and run in CI with no credentials.

### Phase 0: Make the package import again (½ day)
- Fix D1–D3 as small as possible. Restore `PipelineModel` as an alias of `WarehouseOperation` (or point the imports at it) and remove the `make_table_ref` call. This is temporary, so the current branch can be committed and checked in a working state before the structural work starts.
- Fix D8 and D9 in `pyproject.toml`. Add `ruff` and `pytest` to a dev group.
- Add `tests/` with one smoke test that imports every module.
- **Done when:** `uv run python -c "import mrl_pipeline.orchestration"` succeeds and `uv run pytest` passes.

### Phase 1: Configuration and connectors (1 day)
- Rewrite `settings.py` per §4. Add `load_settings`, the env-mismatch guard and masked logging.
- Update `config/*.env`: remove the suffix and warehouse-folder keys; add `MRL_DEV_SAMPLE_RACES`. Add `config.example/`.
- Move the connectors to `warehouse/connectors.py`; fix D7; add `make_connector`.
- **Done when:** unit tests cover the settings load order and the env-mismatch error, and `make_connector(load_settings("dev"))` opens a local DB in the `dev` schema. A manual MotherDuck connection test succeeds.

### Phase 2: The warehouse layer (2 days)
- `naming.py` (§7.2), with tests including hostile identifiers.
- `warehouse/refs.py`: `TableRef`, `TableSummary`, `PromotionPlan`.
- `warehouse/duckdb_warehouse.py` per §5.2, built from the useful parts of `duckdb_service.py`. Fixes D5 and D6.
- `promote()` per §10.4. **Spike first:** check that `ALTER TABLE … RENAME` works inside a transaction on local DuckDB 1.3.2 **and** MotherDuck (R1).
- `warehouse/metadata.py`: the run log tables and the `COMMENT ON` helper.
- **Done when:** `:memory:` tests pass for every warehouse method, including a promotion that is made to fail partway and leaves the primaries unchanged.

### Phase 3: Orchestration framework (2–3 days)
- `orchestration/node.py`: `Node`, `NodeContext`, `SourceNode`, `SqlTransformation` (with the `ref()` renderer and its checks), `PythonTransformation`.
- `registry.py`, `selector.py` (port the existing grammar and its edge cases; add `tag:` and `--exclude`), `planner.py`.
- `run_context.py` and `orchestrator.py` per §10, without Prefect.
- Fixes D13.
- **Done when:** a toy DAG of 4–5 fake nodes, including a multi-output node and a SQL node, runs end to end on `:memory:`. Selected runs read unselected inputs from the primaries. A failing node prevents any promotion. The run log tables are filled in.

### Phase 4: Sources (1–2 days)
- `sources/credentials.py`, `drive.py`, `sheets.py`, `resilient_gspread.py`, all read-only with read-only scopes.
- A `Sources` facade, plus `FakeSources` for tests.
- Answer Q2 (registrations) and confirm the exact prep-sheet titles (§6).
- **Done when:** a `live` test lists `prep_results*` files and reads `prep_all_races` with the service account. `FakeSources` can provide every source table.

### Phase 5: Port the nodes (4–6 days)
Port in dependency order. Each node gets a saved-fixture test before moving on:

1. Source nodes 1–4 and 6.
2. `dim_races` (with `build_dim_races` taking a DataFrame instead of `data`, and `dev_sample_races` in place of the environment check).
3. `stg_results`, combining the current module with the legacy validations (§12 note).
4. `dim_results_and_registrations`, `dim_athletes`.
5. `ext_races_by_penalty_window`, `penalties`.
6. SQL nodes 12–14 (convert names to `{{ ref() }}`).

Then:
- `orchestration/flow.py` (Prefect) and `cli.py` (`run`, `plan`, `list`, `describe`).
- Fix D12.
- **Done when:** `mrl run --env dev` completes against a local file with real sources, and `mrl run --env test` (in memory, `FakeSources`) passes in CI.

### Phase 6: Check results against v1 (2–4 days, plus fixes)
- Freeze one set of inputs: run the legacy pipeline once and record which prep and result sheets it read, with their `modifiedTime`.
- Run v2 in `stg` against the same inputs. For every output table, compare against the matching v1 Sheets table (`mrl compare`): row counts, key sets, and column values within a float tolerance for penalty and points columns. Sort on keys before comparing. Order is not part of the contract.
- Expect small dtype differences, because Sheets turned everything into strings and DuckDB keeps types. Normalize types on the comparison side; don't make v2 match Sheets.
- **Done when:** every table matches, or each difference is documented and accepted by the project owner.

### Phase 7: Cut over and clean up (1 day)
- Run `prod` on MotherDuck, then `mrl describe` each table and spot-check.
- Point any downstream consumers at MotherDuck, or build the export in Q1.
- Delete `legacy/`, `io/`, `models/`, `utils.py`, and the files marked Delete in §II.3.
- Update `README.md`: setup, config layout, `mrl` commands.
- Stop writing Sheets warehouse tables. The existing Drive warehouse folders can be kept read-only for a season as a fallback (Q4).
- **Done when:** a scheduled prod run succeeds on MotherDuck and the repo contains no Sheets-warehouse code.

### Rough timeline

```
Phase 0  ▌
Phase 1   ██
Phase 2     ████
Phase 3         ██████
Phase 4     ███            (can run alongside Phases 2–3)
Phase 5               ████████████
Phase 6                           ██████
Phase 7                                 ██
```

About 3–4 weeks of focused work. Phase 6 is the hardest to estimate.

## II.5 Risks

| # | Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|---|
| R1 | Renaming tables inside a transaction is restricted, or behaves differently on MotherDuck than locally | Medium | High (G3) | Spike at the start of Phase 2; fall back to a swap schema (§10.4). |
| R2 | v1 results depended on Sheets turning everything into strings (e.g. IDs with leading zeros, times read as text) | High | Medium | `stg_*` keeps every column as `VARCHAR` (`all_varchar`), as v1 effectively did. Type conversion happens only in the ported transforms. |
| R3 | The legacy `stg_results` and the current `models/stg_results.py` produce different rows | High | Medium | Treat the legacy version as the reference (§12 note); fixture-test both before combining them. |
| R4 | Local DuckDB allows one writing process, so a Prefect worker and an interactive session can lock each other out | Medium | Low | Document it; the CLI catches the lock error and explains it; use MotherDuck for shared work. |
| R5 | Google API quotas or flaky responses during source reads | Medium | Low | `ResilientGspreadClient`, retries on source nodes, gviz CSV via httpfs (no Sheets API quota). |
| R6 | People or tools currently read warehouse tables in Sheets | Unknown | High | Q1. Answer it before Phase 7. |
| R7 | The DuckDB version pin (1.3.2) falls behind what MotherDuck supports | Medium | Medium | Check MotherDuck's supported client versions in Phase 1; upgrade on purpose, with the parity tests as a check. |

## II.6 Open questions

| # | Question | Needed by |
|---|---|---|
| Q1 | Does anyone (coaches, the website, other tools) read the current Sheets warehouse tables directly? If so, which tables, and does v2 need a separate one-way export step for them? | Phase 7 |
| Q2 | Where does `stg_registrations` come from (a prep sheet, a registration-platform export, something else), and how is it loaded today? | Phase 4 |
| Q3 | Is one schema per environment in one MotherDuck database acceptable, or should prod be its own database (`mrl` vs `mrl_dev`) for stronger isolation and permissions? | Phase 1 |
| Q4 | How long should the v1 Sheets warehouse folders be kept after cutover? | Phase 7 |
| Q5 | Is `archive_retention = 5` right? Should archives older than a season be pruned no matter what? | Phase 2 |
| Q6 | Should the `prep_results*` search be limited to one Drive folder (`google_drive_source_folder_id`), or keep searching every drive the service account can see, as it does today? | Phase 4 |
| Q7 | Does `dim_races` need the result-sheet URLs (`prep_results_url_b/g`) downstream, or can they stay on `stg_results` / `infra_result_files` (§12 note)? | Phase 5 |
