# MRL Pipeline v2: Target Project Structure

*Status: draft 3, 2026-11-17. Based on the `feat/legacy` branch (`93cc07b` plus uncommitted work), revised with the owner's review.*
*Companion document: [v2-spec.md](v2-spec.md) defines the requirements (`FR-…`, `NFR-…`) and open questions (`Q…`) referenced here.*

This document describes **every file, class and function** in the target v2 codebase. For each one it gives what it does, which requirements it satisfies, what current code it comes from, and its **status** compared with today's code.

## Status labels

| Label | Meaning |
|---|---|
| `KEEP` | Exists and works as specified. No change beyond import paths. |
| `MOVE` | Exists and works, but moves to a new module. The logic is unchanged; small mechanical edits only (imports, `print` → logging). |
| `FIX` | Exists but is wrong, incomplete, or doesn't match the spec. The notes say what to change. |
| `ADD` | Doesn't exist yet. The notes say what to build and which existing code to reuse, if any. |
| `DELETE` | Exists and is removed in v2. |
| **NEW** | Marks work for a ranking rule that v1 doesn't have (spec items marked NEW). It is built in **Phase 7**, after the port is shown to match v1. |

"Origin" gives the file and symbol that the v2 code starts from. `legacy/` refers to `src/mrl_pipeline/legacy/`.

**Terminology.** A **model** is one unit of pipeline work (spec glossary). The pipeline's models live in the `models/` package and derive from `orchestration.model.Model`. The current `models/` directory (`WarehouseOperation`-based files) is **replaced in place** by the new package.

---

## Contents

1. [Tree overview](#1-tree-overview)
2. [Repository root](#2-repository-root)
3. [Package core: `settings.py`, `naming.py`, `cli.py`](#3-package-core)
4. [`warehouse/`](#4-warehouse)
5. [`sources/`](#5-sources)
6. [`orchestration/`](#6-orchestration)
7. [`models/`](#7-models)
8. [`transforms/`](#8-transforms)
9. [`tests/`](#9-tests)
10. [Files removed in v2](#10-files-removed-in-v2)
11. [Known defects in the current code](#11-known-defects-in-the-current-code)
12. [Requirement-to-code traceability](#12-requirement-to-code-traceability)
13. [Build order](#13-build-order)
14. [Risks](#14-risks)

---

## 1. Tree overview

```
mrl-pipeline/
├── pyproject.toml                         FIX
├── uv.lock                                KEEP (regenerate)
├── README.md                              FIX
├── .gitignore                             FIX   (add logs/)
├── config/                                (git-ignored)
│   ├── dev.env / stg.env / prod.env       FIX
│   └── secrets/                           KEEP
├── config.example/                        ADD
├── db/                                    KEEP (local DuckDB file location)
├── logs/                                  ADD (git-ignored; per-run log directories)
├── claude/                                docs (this file, v2-spec.md)
├── src/mrl_pipeline/
│   ├── __init__.py                        FIX
│   ├── settings.py                        FIX
│   ├── naming.py                          ADD   ← io/utils.py, io/table_version_ref.py
│   ├── cli.py                             ADD
│   ├── warehouse/
│   │   ├── __init__.py                    FIX
│   │   ├── connectors.py                  MOVE+FIX ← io/database_connectors.py
│   │   ├── refs.py                        FIX   ← warehouse/warehouse_backend.py
│   │   ├── duckdb_warehouse.py            ADD   ← io/duckdb_service.py, warehouse/warehouse_backend.py
│   │   ├── lifecycle.py                   ADD
│   │   └── metadata.py                    ADD   (run lock + publication record only)
│   ├── sources/
│   │   ├── __init__.py                    ADD
│   │   ├── credentials.py                 ADD   ← io/drive_service.py, sheet_service.py
│   │   ├── drive.py                       MOVE+FIX ← io/drive_service.py
│   │   ├── sheets.py                      MOVE+FIX ← sheet_service.py (prep methods)
│   │   ├── gviz.py                        ADD   ← legacy/build_stg_results_and_dim_races.py
│   │   ├── resilient_gspread.py           MOVE  ← io/google_sheets_warehouse_manager/resilient_gspread_client.py
│   │   └── fake.py                        ADD
│   ├── orchestration/
│   │   ├── __init__.py                    FIX
│   │   ├── model.py                       FIX   ← orchestration/transformation.py
│   │   ├── registry.py                    ADD   ← orchestration/model_selector.py (graph)
│   │   ├── selector.py                    MOVE+FIX ← orchestration/model_selector.py (grammar)
│   │   ├── planner.py                     ADD   ← orchestration/model_selector.py (toposort)
│   │   ├── run_context.py                 ADD   ← orchestration/orchestration.py (ABC)
│   │   ├── run_log.py                     ADD
│   │   ├── orchestrator.py                ADD   ← orchestration/orchestration.py (ABC)
│   │   └── flow.py                        FIX   ← orchestration/flows.py
│   ├── models/                            (directory reused; current files replaced)
│   │   ├── __init__.py                    FIX   (becomes the MODELS registry list)
│   │   ├── sources.py                     ADD   ← models/infra_result_files.py
│   │   ├── stg_results.py                 FIX   ← models/stg_results.py + legacy ingestion
│   │   ├── dims.py                        ADD
│   │   ├── penalties.py                   ADD
│   │   ├── ext.py                         ADD
│   │   ├── qa.py                          ADD (NEW)
│   │   └── sql/
│   │       ├── ext_results.sql                       MOVE+FIX ← legacy/build_ext_results.sql
│   │       ├── ext_points_contributing_results.sql   MOVE+FIX ← legacy/…
│   │       └── ext_registrations.sql                 MOVE+FIX ← legacy/…
│   └── transforms/
│       ├── __init__.py                    ADD
│       ├── ids.py                         MOVE+FIX ← legacy/build_dim_results_and_dim_registrations.py
│       ├── names.py                       ADD (NEW) ← legacy/build_name_replacement_artifacts
│       ├── name_conflicts.py              ADD (NEW)
│       ├── dim_races.py                   MOVE+FIX ← legacy/build_stg_results_and_dim_races.py
│       ├── results_ingest.py              MOVE+FIX ← legacy/build_stg_results_and_dim_races.py
│       ├── dim_results_registrations.py   MOVE+FIX ← legacy/build_dim_results_and_dim_registrations.py
│       ├── dim_athletes.py                MOVE+FIX ← legacy/build_dim_athletes.py
│       ├── penalty_windows.py             MOVE+FIX ← legacy/build_ext_races_by_penalty_window.py
│       └── penalties.py                   MOVE+FIX ← legacy/build_penalties.py
└── tests/                                 ADD (directory exists, empty)
```

The packages are layered. A module may import only from layers **below** it:

```
cli.py, orchestration/flow.py
        │
orchestration/  ──►  models/  ──►  transforms/  (pure pandas, no project imports)
        │               │
        ▼               ▼
   warehouse/        sources/
        │               │
        └──► naming.py, settings.py ◄──┘
```

---

## 2. Repository root

### `pyproject.toml`: `FIX`

| Item | Status | Change | Req |
|---|---|---|---|
| `requires-python = ">=3.9"` | `FIX` | Change to `>=3.10`. `settings.py` and connectors use `X \| None` in pydantic fields, which is evaluated at runtime. | NFR-02 |
| `dependencies` | `FIX` | Add `scikit-learn`, `numpy`. Remove `typing` (it's part of the standard library), `pytest`, and `pytz` (no longer needed once timestamps are UTC). Keep the `duckdb==1.3.2` pin, but check it against MotherDuck's supported client versions. `python-dotenv` isn't needed after `utils.py` is deleted. | NFR-02 |
| `[dependency-groups] dev` | `ADD` | `pytest`, `ruff`. | FR-QA-01 |
| `[project.scripts]` | `ADD` | `mrl = "mrl_pipeline.cli:main"` | FR-RUN-09 |
| `[tool.pytest.ini_options]` | `ADD` | `markers = ["live: needs real Google credentials"]`, `addopts = "-m 'not live'"` | FR-QA-03 |
| `[tool.hatch.build]` | `FIX` | Make sure `models/sql/*.sql` is included in the wheel. | FR-DEV-02 |
| `[tool.ruff]` | `KEEP` | | NFR-06 |

### `.gitignore`: `FIX`
Add `logs/`. Req: FR-OBS-02.

### `config/<env>.env`: `FIX`

Every setting in spec FR-ENV-11 can appear here; only non-defaults need to. Keys use the `MRL_` prefix.

| Key | Status | Notes |
|---|---|---|
| `MRL_ENVIRONMENT_NAME` | `KEEP` | Must equal the file's environment (FR-ENV-08). |
| `MRL_SCHEMA_NAME` | `KEEP` | Optional. Defaults to the environment name. |
| `MRL_PROJECT_NAME` | `ADD` | Optional, default `mrl`. Replaces `MRL_DATABASE_NAME` if present. |
| `MRL_GOOGLE_DRIVE_SOURCE_FOLDER_ID` | `ADD` | Required for any run that reads sources (FR-SRC-02). |
| `MRL_WAREHOUSE_TARGET` | `ADD` | `local` / `motherduck`, optional. |
| `MRL_SAMPLE_RACES`, `MRL_SAMPLE_SEED` | `ADD` | Optional. Not tied to the environment (FR-DOM-02, FR-ENV-07). |
| Input sheet keys (`MRL_RACES_SHEET`, `MRL_REGISTRATIONS_SHEET`, `MRL_PEOPLE_SHEET`, …) | `ADD` | Optional; defaults in `Settings`. |
| `MRL_LOG_DIR` | `ADD` | Optional, default `<project root>/logs`. |
| `MRL_DRIVE_TABLE_SUFFIX` | `DELETE` | Only used by the Sheets warehouse. |
| `MRL_DB_TABLE_SUFFIX` | `DELETE` | FR-ENV-04. |
| `MRL_GOOGLE_DRIVE_WAREHOUSE_FOLDER_ID` | `DELETE` | There is no Sheets warehouse. |

### `config.example/`: `ADD`
A copy of `config/` with placeholder values and every FR-ENV-11 setting commented with its default, plus `secrets/README.md` listing the expected secret filenames (`MRL_MOTHERDUCK_TOKEN`, `MRL_GOOGLE_SERVICE_ACCOUNT_CREDENTIALS`). Req: FR-ENV-10, -11.

### `README.md`: `FIX`
Add sections on setup (`uv sync`), the config layout and input settings, choosing an environment, the `mrl` commands, where run logs go, archive management, and running tests.

---

## 3. Package core

### `src/mrl_pipeline/__init__.py`: `FIX`

| Member | Status | Notes |
|---|---|---|
| `from .settings import settings` | `FIX` | Today this creates the `Settings()` singleton at import time. Replace it with `from .settings import Settings, load_settings` and `__version__`. Req: NFR-04. |

### `src/mrl_pipeline/settings.py`: `FIX`

Origin: the current `settings.py`.

| Member | Kind | Status | Notes | Req |
|---|---|---|---|---|
| `PROJECT_ROOT`, `SECRETS_DIR` | const | `KEEP` | | FR-ENV-06 |
| `PROJECT_DUCKDB_PATH` | const | `DELETE` | Replaced by `Settings.resolved_duckdb_path`, which uses the project name. | FR-WH-02 |
| `CONFIG_DIR` | const | `ADD` | `PROJECT_ROOT / "config"`. Can be overridden with `MRL_CONFIG_DIR` for containers. | NFR-05 |
| `Environment` | `Literal["dev","stg","prod"]` | `ADD` | | FR-ENV-01 |
| `Settings` | class | `FIX` | See the field table below. `model_config` keeps `env_prefix="MRL_"` and `secrets_dir`, and **adds** env-file support through `load_settings`. | FR-ENV-* |
| `Settings.schema` | property | `ADD` | `schema_name or environment_name`. | FR-ENV-03 |
| `Settings.database` | property | `ADD` | `project_name`. The MotherDuck database name. | FR-WH-02 |
| `Settings.resolved_duckdb_path` | property | `ADD` | `local_duckdb_path or PROJECT_ROOT / "db" / f"{project_name}.duckdb"`. | FR-WH-02 |
| `Settings.resolved_target` | property | `ADD` | `warehouse_target`, or `"motherduck" if motherduck_token else "local"`. | FR-WH-02 |
| `Settings.masked_dict()` | method | `ADD` | All values with secrets replaced by `"***"`, for logging. | FR-ENV-09 |
| `Settings.validate_for_run(needs_sources: bool)` | method | `ADD` | Raises `ConfigError` if the target is `motherduck` without a token, or if `needs_sources` and Google credentials or `google_drive_source_folder_id` are missing. | FR-ENV-08 |
| `load_settings(env=None, **overrides)` | function | `ADD` | Works out `env` from the argument, then `MRL_ENV`, then `"dev"`. Builds `Settings(_env_file=CONFIG_DIR / f"{env}.env", **overrides)`. Raises `ConfigError` if `environment_name` doesn't match `env`. | FR-ENV-02, -05, -08 |
| `ConfigError` | exception | `ADD` | | FR-ENV-08 |
| `settings = Settings()` | module global | `DELETE` | Settings are created explicitly at the entry points. | NFR-04 |

`Settings` fields:

| Field | Status | Notes | Req |
|---|---|---|---|
| `environment_name: Environment` | `FIX` | Currently `str`. | FR-ENV-01 |
| `project_name: str = "mrl"` | `FIX` ← `database_name` | Renamed: one database per project. | FR-WH-02, FR-ENV-11 |
| `schema_name: str \| None = None` | `FIX` | Currently defaults to `"mrl_dev"`, which doesn't match the `config/*.env` values. | FR-ENV-03 |
| `warehouse_target: Literal["local","motherduck"] \| None = None` | `ADD` | | FR-WH-02 |
| `local_duckdb_path: Path \| None = None` | `KEEP` | Overrides the derived path. `:memory:` allowed for tests. | FR-WH-02 |
| `google_drive_source_folder_id: str \| None = None` | `ADD` | Required when reading sources (`validate_for_run`). | FR-SRC-02 |
| `results_sheet_prefix: str = "prep_results"` | `ADD` | | FR-SRC-02, FR-ENV-11 |
| `races_sheet: str = "prep_all_races"` | `ADD` | | FR-DOM-01, FR-ENV-11 |
| `registrations_sheet: str = "prep_qualifier_registrations"` | `ADD` | | FR-SRC-09, FR-ENV-11 |
| `people_sheet: str = "prep_people_and_spellings"` | `ADD` | | FR-SRC-08, FR-ENV-11 |
| `people_name_spellings_tab: str` | `ADD` | Default to be set from Q4. | FR-SRC-08 |
| `people_non_persons_tab: str` | `ADD` | Default to be set from Q4. | FR-SRC-08 |
| `schools_sheet: str = "prep_schools"` | `ADD` | | FR-SRC-08, FR-ENV-11 |
| `penalty_publication_dates_sheet: str = "prep_penalty_publication_dates"` | `ADD` | | FR-SRC-08, FR-ENV-11 |
| `penalty_contribution_windows_sheet: str = "prep_penalty_contribution_windows"` | `ADD` | | FR-SRC-08, FR-ENV-11 |
| `penalty_contributions_sheet: str = "prep_penalty_contributions"` | `ADD` | | FR-SRC-08, FR-ENV-11 |
| `exclusive_league_series: list[str] = ["BERK","MBE","MBW"]` | `ADD` | Set as JSON in the env file. | FR-DOM-04d |
| `ec_series: str = "EC"` | `ADD` | Also replaces the hard-coded `"EC"` in the title-case rule of `build_dim_results`. | FR-DOM-04, -04d |
| `ec_age_groups: dict[str, Literal["U16","U18/U20"]]` | `ADD` | `division` value → age group. Default set from Q7. | FR-DOM-04d |
| `u16_champs_series: str` | `ADD` | `series` value of U16 Championships races. Default set from Q7. | FR-DOM-04d |
| `sample_races: bool = False` | `ADD` | Independent of environment. | FR-DOM-02, FR-ENV-07 |
| `sample_seed: int \| None = None` | `ADD` | | FR-DOM-02 |
| `source_max_workers: int \| None = None` | `ADD` | | NFR-01 |
| `log_dir: Path = PROJECT_ROOT / "logs"` | `ADD` | | FR-OBS-02, NFR-05 |
| `lock_stale_after_minutes: int = 120` | `ADD` | A lock older than this, with no heartbeat, can be taken over. | FR-RUN-11 |
| `motherduck_token: SecretStr \| None` | `FIX` | Currently `str`. | FR-ENV-06 |
| `google_service_account_credentials: dict \| None` | `KEEP` | Read from the secret file as JSON. | FR-ENV-06 |
| `drive_table_suffix`, `db_table_suffix`, `google_drive_warehouse_folder_id` | `DELETE` | | FR-WH-01, FR-ENV-04 |

### `src/mrl_pipeline/naming.py`: `ADD`

Origin: `io/utils.py` (all members) and `io/table_version_ref.py` (`TableVersionRef`). This becomes the only place table names are built and quoted.

| Member | Kind | Status | Notes | Req |
|---|---|---|---|---|
| `sanitize_identifier(text) -> str` | fn | `FIX` ← `io/utils.sanitize_table_name` | Remove `+` from the allowed characters (`[a-z0-9_]` only). Raise `ValueError` if the result is empty. | NFR-03 |
| `quote_ident(name) -> str` | fn | `MOVE` ← `io/utils.escape_table_name` | Unchanged behavior: doubles embedded `"` and wraps in quotes. | NFR-03 |
| `utc_stamp(dt=None) -> str` | fn | `FIX` ← `io/utils._default_timestamp` | Format becomes `YYYYMMDDtHHMMSS`, already safe as an identifier. | FR-SAFE-04 |
| `make_run_id(started_at) -> str` | fn | `ADD` | `"r" + utc_stamp(started_at) + "_" + 4 random hex characters`. Also used as the run's log directory name. | FR-SAFE-09, FR-OBS-02 |
| `TEMP_PREFIX = "__tmp__"`, `ARCHIVE_PREFIX = "_archive__"`, `META_PREFIX = "_mrl_"` | const | `ADD` | Reserved prefixes. | FR-WH-06 |
| `validate_logical_name(name)` | fn | `ADD` | Rejects names that start with `_` or that `sanitize_identifier` would change. | FR-WH-06 |
| `primary_name(logical) -> str` | fn | `ADD` | `logical` | FR-ENV-04 |
| `temp_name(logical, run_id) -> str` | fn | `ADD` | `__tmp__{logical}__{run_id}` | FR-SAFE-09 |
| `archive_name(logical, run_stamp) -> str` | fn | `ADD` | `_archive__{logical}__{run_stamp}` | FR-SAFE-04 |
| `parse_physical_name(name) -> (role, logical, tag) \| None` | fn | `ADD` | The reverse of the three functions above. Used for listing, pruning and cleanup. | FR-SAFE-05, -08 |
| `TableVersionRef` | class | `DELETE` | Replaced by the functions above. It quoted names (defect D5), added environment suffixes (against FR-ENV-04), and read the settings global. | |

### `src/mrl_pipeline/cli.py`: `ADD`

Built on `argparse`. Every subcommand calls `load_settings(args.env)` first.

| Member | Kind | Notes | Req |
|---|---|---|---|
| `main(argv=None) -> int` | fn | Entry point for `mrl`. Sets up console logging and dispatches to the subcommand. | FR-RUN-09 |
| `cmd_run(args)` | fn | Flags: `--env --select --exclude --no-promote --no-archive --no-prefect --force`. Calls `flow.mrl_pipeline(...)`, or `Orchestrator.run(...)` directly with `--no-prefect`. Prints the run log path at the end. Exits non-zero on failure. | FR-RUN-01…-03, -10, -11, FR-OBS-02 |
| `cmd_plan(args)` | fn | Prints `Plan.render()`. | FR-RUN-05 |
| `cmd_list(args)` | fn | Every model and table, with tags, inputs, outputs and primary row counts. | FR-OBS-05 |
| `cmd_describe(args)` | fn | `<table>`: columns, comments, last publishing run (`metadata.last_publication`), archives. | FR-OBS-06 |
| `cmd_runs(args)` | fn | `[--limit N]`: recent runs for the environment from `run_log.list_runs`. | FR-OBS-07 |
| `cmd_archives(args)` | fn | Subcommands: `list [<table>]`; `prune --keep N [<table>…] [--dry-run]`; `drop <archive_name>`. Manual only. | FR-SAFE-05 |
| `cmd_rollback(args)` | fn | `<table> [--to STAMP]` or `--run RUN_ID`. | FR-SAFE-06, -07 |
| `cmd_clean(args)` | fn | Drops abandoned temp tables. | FR-SAFE-08 |
| `cmd_compare(args)` | fn | `<table> --legacy-sheet <title> [--key cols] [--rtol]`. Reads v1 Sheets read-only. Temporary, deleted after cutover. | FR-QA-05 |

---

## 4. `warehouse/`

### `warehouse/__init__.py`: `FIX`
Currently re-exports `WarehouseService, TableRef, TableSummary` from `warehouse_backend`. The new exports are `DuckDBWarehouse`, `TableRef`, `TableSummary`, `PromotionPlan`, `make_connector`.

### `warehouse/connectors.py`: `MOVE+FIX`

Origin: `io/database_connectors.py`.

| Member | Kind | Status | Notes | Req |
|---|---|---|---|---|
| `DatabaseConnector` | ABC | `KEEP` | `connect() -> DuckDBPyConnection`. Add a `location: str` property for logs. | FR-WH-02 |
| `LocalDuckDBConnector` | class | `FIX` | Takes `database_path` and `schema`; stop reading from the `settings` global. Keep the parent-directory creation and `:memory:` support. `location` = the path. | FR-WH-02 |
| `MotherDuckConnector` | class | `FIX` | Takes `database` (the project name) and `schema`. **Defect D7:** the token is set with `SET motherduck_token` *after* `duckdb.connect("md:…")`, too late to authenticate. Pass it in `config={"motherduck_token": token}` instead. Accept a `SecretStr`. `location` = `md:{database}`. Creates the database if it doesn't exist. | FR-WH-02 |
| `_prepare(conn, schema)` | fn | `FIX` | Pulls out the shared post-connect setup: load `httpfs` (see below), `CREATE SCHEMA IF NOT EXISTS`, `SET schema`. Use `quote_ident` for the schema in `SET schema`. Today it is passed as a bound parameter. | FR-ENV-03 |
| `_try_load_httpfs(conn)` | fn | `FIX` | Log a warning on failure instead of silently ignoring it. Also add the legacy fix for the "already registered" race when several threads load it (`legacy/…_is_httpfs_double_init_error`, `load_httpfs_serialized`). | FR-SRC-07 |
| `make_default_connector()` | fn | `FIX` → `make_connector(settings)` | Choose by `settings.resolved_target`; use `settings.database`, `settings.resolved_duckdb_path`, `settings.schema`. | FR-WH-02 |

### `warehouse/refs.py`: `FIX`

Origin: `warehouse/warehouse_backend.py` (`TableRef`, `TableSummary`). The `WarehouseService` ABC from that file is folded into `duckdb_warehouse.py`.

| Member | Kind | Status | Notes | Req |
|---|---|---|---|---|
| `Role` | `Literal["primary","temp","archive"]` | `ADD` | | |
| `TableRef` | frozen dataclass | `FIX` | Fields become `logical_name`, `physical_name` (always **unquoted**), `schema` (required), `role`. Remove `backend`. Add the classmethods `primary(logical, schema)`, `temp(logical, schema, run_id)` and `archive(logical, schema, stamp)`, which call `naming`. | FR-DEV-01 |
| `TableSummary` | frozen dataclass | `KEEP` | `row_count, column_count, columns, dtypes, notes`. There is a duplicate in `io/warehouse_table.py`, which is deleted. | FR-OBS-03 |
| `PromotionStep` | frozen dataclass | `ADD` | `temp: TableRef, primary: TableRef, archive: TableRef, primary_existed: bool` | FR-SAFE-01, -07 |
| `PromotionPlan` | frozen dataclass | `ADD` | `run_id, run_stamp, steps: tuple[PromotionStep, ...], archive: bool` | FR-SAFE-01 |

### `warehouse/duckdb_warehouse.py`: `ADD`

Origin: the method list of `warehouse/warehouse_backend.WarehouseService`, plus implementations from `io/duckdb_service.DuckDBWarehouseService`. This is the **only** warehouse implementation (FR-WH-01).

| Member | Status | Origin / notes | Req |
|---|---|---|---|
| `class DuckDBWarehouse(connector, schema)` | `ADD` | Keeps **one** open connection for its lifetime and works as a context manager. Replaces the open-and-close-per-call `_connection()` in `DuckDBWarehouseService`. | NFR-01 |
| `.conn` / `.cursor()` | `ADD` | `cursor()` gives a thread-local cursor for parallel source reads (the pattern in `models/stg_results.write_from_thread`). | NFR-01 |
| `.schema`, `.location` | `ADD` | | FR-OBS-02 |
| `.quote_ident(name)` | `ADD` | Delegates to `naming.quote_ident`. | NFR-03 |
| `.qualify(ref) -> str` | `ADD` | `"schema"."physical"`. Every SQL statement the class builds uses this. | FR-ENV-03 |
| `.exists(ref) -> bool` | `FIX` ← `DuckDBWarehouseService._table_exists` | Compare **unquoted** names against `information_schema.tables` (fixes D5). Use `ref.schema`, not a value stored on the instance. | |
| `.list_tables(prefix=None) -> list[str]` | `ADD` | | FR-OBS-05 |
| `.read_table(ref, *, columns=None, where=None, limit=None) -> DataFrame` | `FIX` ← `_read_table` | Add the column, filter and limit options from the ABC. | FR-DEV-03 |
| `.write_table(ref, data, *, mode="replace")` | `FIX` ← `put_warehouse_table`, `_write_dataframe`, `_iter_dataframes` | Keep the register-view-then-CTAS/INSERT approach and support iterables of chunks. Add `mode="fail"`. Use `INSERT … BY NAME` for append. **Remove** the temp-table bookkeeping (now in `RunContext`) and the `TableVersionRef` naming. | FR-DEV-03 |
| `.create_table_as_select(ref, sql)` | `ADD` | `CREATE TABLE <qualified> AS <sql>`. Fails if the table exists (temp tables are always fresh). | FR-DEV-02 |
| `.execute(sql, params=None)` / `.fetch_df(sql, params=None)` | `ADD` | | |
| `.render(template, refs) -> str` | `ADD` | Replaces `{{ ref('x') }}` with `qualify(refs['x'])` using a regex. Raises `TemplateError` for an unknown ref, or a declared ref that the template never uses. | FR-DEV-02 |
| `.drop(ref, if_exists=True)` / `.rename(src, dst)` | `ADD` | Within one schema only. | FR-SAFE-05, -06 |
| `.summarize(ref) -> TableSummary` | `ADD` | `count(*)` plus `DESCRIBE`. Replaces the row-count check in `WarehouseOperation.run`. | FR-OBS-03, FR-SAFE-03 |
| `.comment(ref, table_comment, column_comments)` | `ADD` | `COMMENT ON TABLE` / `COMMENT ON COLUMN`. | FR-OBS-01 |
| `.promote(plan: PromotionPlan)` | `FIX` ← `DuckDBWarehouseService.promote_temp_tables` | Keep `BEGIN` / renames / `COMMIT`, with `ROLLBACK` on error. Use the archive name from the plan. Fix the quoting (D5). Check that each temp exists *before* `BEGIN`. Inside the same transaction, call `metadata.record_publication(plan)`. **R1:** confirm DuckDB 1.3.2 and MotherDuck support renames inside a transaction; the fallback is a staging schema. | FR-SAFE-01, -02, -04, -07 |
| `.transaction()` | `ADD` | Context manager, shared by `promote`, `lifecycle` and `metadata`. | FR-SAFE-01 |
| `class TemplateError` | `ADD` | | FR-DEV-02 |
| `DuckDBWarehouseService._extract_database_path`, `._resolve_location`, `._prep_table_name`, `._table_url`, `.env_suffix`, `.temp_tables`, `._temp_lock` | `DELETE` | Unused, replaced by `connector.location`, or only meaningful for the Sheets/suffix model. | |

### `warehouse/lifecycle.py`: `ADD`

Archive and temp-table maintenance. There is no current equivalent; v1 had to rename sheets by hand. **Nothing here runs automatically**; every function is called from a CLI command (FR-SAFE-05).

| Member | Notes | Req |
|---|---|---|
| `list_archives(wh, logical=None) -> list[TableRef]` | Newest first, found with `naming.parse_physical_name`. All tables when `logical` is `None`. | FR-SAFE-05, FR-OBS-06 |
| `drop_archive(wh, physical_name)` | Refuses names that aren't archives. | FR-SAFE-05 |
| `prune_archives(wh, keep: int, logicals=None, dry_run=False) -> list[str]` | Drops archives past `keep` for each given (or every) table. Returns the names dropped or, with `dry_run`, that would be dropped. | FR-SAFE-05 |
| `rollback_table(wh, logical, to_stamp=None)` | In one transaction: rename the current primary to `archive_name(logical, now)`, then rename the chosen archive (default: newest) to primary. | FR-SAFE-06 |
| `rollback_run(wh, run_id)` | Reads `metadata.publications_for_run(run_id)`. In one transaction, for each table: if `primary_existed`, swap the run's archive back in (current version archived as in `rollback_table`); otherwise drop the primary the run created. Refuses if a later run has since published the same table, unless forced. | FR-SAFE-07 |
| `clean_abandoned_temps(wh, active_run_ids) -> list[str]` | Drops `__tmp__*` tables whose run isn't active (`RunLock.holder()`). | FR-SAFE-08 |

### `warehouse/metadata.py`: `ADD`

The run history lives in local log files (`orchestration/run_log.py`, FR-OBS-02/-03). Only two small tables stay in the warehouse, because they must be shared by everyone using the same MotherDuck database: the run lock and the record of what each run published.

| Member | Notes | Req |
|---|---|---|
| `LOCK_TABLE = "_mrl_lock"`, `PUBLICATIONS_TABLE = "_mrl_publications"` | Created in each environment's schema. Hidden by the `_mrl_` prefix. | FR-WH-06 |
| `ensure_metadata_tables(wh)` | `CREATE TABLE IF NOT EXISTS` for both. | |
| `class RunLock(wh, stale_after)` | `.acquire(run_id, host, force=False)`: in a transaction, inserts the lock row, or raises `RunLockedError` naming the holder if a non-stale lock exists and `force` is false. `.heartbeat(run_id)` (called after each model), `.release(run_id)`, `.holder() -> (run_id, host, started_at) \| None`. | FR-RUN-11 |
| `class RunLockedError` | | FR-RUN-11 |
| `record_publication(wh, plan)` | One row per published table: `run_id, run_stamp, logical_name, archive_name (null if new), primary_existed, published_at`. Called inside `promote`'s transaction, so the record and the publish succeed or fail together. | FR-SAFE-07 |
| `publications_for_run(wh, run_id)`, `last_publication(wh, logical)` | | FR-SAFE-07, FR-OBS-06 |

---

## 5. `sources/`

All modules here are **read-only** (FR-SRC-01). Only `SourceModel`s receive a `Sources` object (FR-DEV-04).

### `sources/__init__.py`: `ADD`

| Member | Notes | Req |
|---|---|---|
| `Sources` (Protocol) | `drive: DriveSource`, `sheets: SheetsSource`, `gviz_url(file_id) -> str`, `folder_id: str`. | FR-DEV-04 |
| `GoogleSources(settings)` | The real implementation. Builds credentials once and creates its clients on first use. | FR-SRC-01 |
| `open_prep_sheet(sources, title) -> SheetHandle` | `drive.find_sheet_by_title(title, folder_id)` then `sheets.open_by_key`. Returns the file ID, title and modified time with the handle, for the run log. | FR-SRC-08, -11, -12 |

### `sources/credentials.py`: `ADD`

| Member | Status | Origin / notes | Req |
|---|---|---|---|
| `READONLY_SCOPES` | `ADD` | `drive.metadata.readonly`, `spreadsheets.readonly`, `drive.readonly`. Today `SheetService` asks for full `drive` and `spreadsheets` scopes. | FR-SRC-01 |
| `load_service_account(payload, scopes)` | `MOVE` ← `io/drive_service.DriveService._build_credentials` | Accepts a dict, a JSON string, or a path. | FR-ENV-06 |

### `sources/drive.py`: `MOVE+FIX`

Origin: `io/drive_service.py` (`DriveService`). The duplicate `legacy/build_stg_results_and_dim_races.DriveService` is deleted.

| Member | Status | Notes | Req |
|---|---|---|---|
| `class DriveSource(credentials)` | `FIX` | Renamed. Takes built credentials instead of looking them up through `settings` and `os.getenv`. | FR-SRC-01 |
| `._list_files(q, params, fields, page_size)` | `KEEP` | Pagination with `supportsAllDrives`. | |
| `._traverse_folder_tree(root, params)` | `KEEP` | Breadth-first through subfolders. | FR-SRC-02 |
| `.list_sheets_by_prefix(prefix, *, folder_id, recursive=True) -> DataFrame` | `FIX` ← `get_gsheets_by_prefix` | `folder_id` becomes **required** (FR-SRC-02). Columns become `sheet_name, file_id, modified_at (UTC timestamp), source_type`. The current version renames the title to `race_id` (D15). Escape `'` in the prefix inside the Drive query. | FR-SRC-02, NFR-03 |
| `.find_sheet_by_title(title, *, folder_id) -> FileInfo` | `ADD` | Exact title match within the folder tree. Raises `SourceLookupError` for zero or several matches. | FR-SRC-12 |
| `find_duplicate_titles(df) -> DataFrame` | `ADD` ← part (A) of `legacy/validate_drive_lookup_against_races` | | FR-SRC-03 |
| `class SourceLookupError` | `ADD` | | FR-SRC-12 |

### `sources/sheets.py`: `MOVE+FIX`

Origin: `io/google_sheets_warehouse_manager/sheet_service.SheetService.get_prep_table` / `iter_prep_tables` (the same code as legacy `DataConnector`).

| Member | Status | Notes | Req |
|---|---|---|---|
| `class SheetsSource(credentials, retries=10, delay=5, backoff=2)` | `ADD` | Wraps `gspread.authorize(creds)` in `ResilientGspreadClient`. | FR-SRC-10 |
| `.open_by_key(file_id)` | `ADD` | Always open by key, never by title, so the file found by `DriveSource` is the one read. | FR-SRC-12 |
| `.read_worksheet(file_id, worksheet=None) -> DataFrame` | `MOVE+FIX` ← `get_prep_table` | Keep the logic for treating `worksheet` as an index or a title; take a file ID instead of a title. Drop `return_url`. All values as text. Replace `print` with logging. | FR-SRC-08, FR-WH-05 |
| `.read_all_worksheets(file_id) -> Iterator[tuple[str, DataFrame]]` | `MOVE+FIX` ← `iter_prep_tables` | Used for the races and registrations sheets (one tab per season). | FR-SRC-08, -09 |
| `SheetService.__init__` (credential and folder-ID setup), `.put_warehouse_table`, `.get_warehouse_table`, `.promote_temp_tables`, `TableVersionManager` | `DELETE` | Sheets is no longer a warehouse (FR-WH-01). | |

### `sources/gviz.py`: `ADD`

Origin: `legacy/build_stg_results_and_dim_races.py`.

| Member | Status | Origin / notes | Req |
|---|---|---|---|
| `gviz_csv_url(file_id) -> str` | `ADD` | `https://docs.google.com/spreadsheets/d/{id}/gviz/tq?tqx=out:csv`. Built inline today in `models/stg_results.py` and legacy `resolve_sheet_names_to_ids`. | FR-DOM-03 |
| `sheet_url(file_id) -> str` | `ADD` | The `/edit` URL, for `prep_results_url` and `dim_races`. | FR-DOM-01, -03 |
| `read_csv_into(cursor, url, target_qualified)` | `ADD` ← `legacy/process_one_sheet_into_duckdb` (fetch part), `models/stg_results.transform_results_and_insert_into_staging_table` (first statement) | `CREATE TABLE … AS SELECT * FROM read_csv(?, all_varchar=true, header=true)`. Raises `SourceFetchError` on `duckdb.IOException`. | FR-DOM-03, FR-SRC-07 |
| `class SourceFetchError` | `ADD` | | FR-SRC-07 |

The gviz endpoint only works for sheets readable by link. Today it works because the result sheets are shared by link. See R6.

### `sources/resilient_gspread.py`: `MOVE`

Origin: `io/google_sheets_warehouse_manager/resilient_gspread_client.py`.

| Member | Status | Notes | Req |
|---|---|---|---|
| `RetryingMethod` | `MOVE` | Replace `print` with logging. Keep the retries on 429, 500 and 503 with backoff. | FR-SRC-10, FR-OBS-04 |
| `ResilientGspreadClient` | `MOVE` | Unchanged. It already wraps `Spreadsheet` and `Worksheet` results, including inside returned iterables. | FR-SRC-10 |

### `sources/fake.py`: `ADD`

| Member | Notes | Req |
|---|---|---|
| `FakeSources(fixtures_dir)` | Implements `Sources`. `drive.list_sheets_by_prefix` and `find_sheet_by_title` read a fixture file listing. `sheets.read_*` reads Parquet fixtures (one per tab). `gviz_url` returns a local `file://` CSV path, which DuckDB `read_csv` handles the same way. | FR-QA-01 |

---

## 6. `orchestration/`

### `orchestration/__init__.py`: `FIX`

Today it imports `.transformation` and `.orchestration`, which imports back from `mrl_pipeline.orchestration` (D13), and then `.flows`, which builds every task at import (D12). The new version exports only `Model`, `SourceModel`, `SqlModel`, `PythonModel`, `Registry`, `Orchestrator`. It does **not** import `flow`. Req: NFR-04.

### `orchestration/model.py`: `FIX`

Origin: `orchestration/transformation.py` (`Transformation`, `SqlTransformation`, `PythonTransformation`), renamed to the "model" vocabulary.

| Member | Status | Notes | Req |
|---|---|---|---|
| `ModelContext` (frozen dataclass) | `ADD` | `warehouse, inputs: Mapping[str, TableRef], outputs: Mapping[str, TableRef], settings, sources: Sources \| None, logger, run_id, note_source: Callable[[FileInfo], None]`. Replaces the separate keyword arguments of `Transformation.run`. `note_source` feeds the run log (FR-SRC-11). | FR-DEV-01 |
| `class Model(ABC)` | `FIX` ← `Transformation` | Class attributes instead of abstract properties: `name: str`, `inputs: tuple[str, ...]`, `outputs: tuple[str, ...]` (**more than one allowed**, replacing `output_logical_name`), `description: str`, `column_descriptions: Mapping[str, Mapping[str, str]]` (output → column → text), `tags: tuple[str, ...]`, `allow_empty: frozenset[str]`, `needs_sources: bool = False`. Abstract `run(ctx)`. Optional `checks(ctx) -> Mapping[str, list[str]]` (per output; failures block publishing). Optional `warning_checks(ctx) -> list[str]` (run after `checks`; logged as warnings, never blocking). Optional `warnings` list filled during `run`. | FR-DEV-01, -05, FR-SAFE-03 |
| `Model.__init_subclass__` checks | `ADD` | Logical names pass `naming.validate_logical_name`, there is at least one output, no duplicate names, and every output has a `column_descriptions` entry. | FR-DEV-07, FR-OBS-01 |
| `class SourceModel(Model)` | `ADD` | `needs_sources = True`. Abstract `extract(ctx) -> DataFrame \| Mapping[str, DataFrame]`. The default `run` writes the result through `_write_outputs`. | FR-DEV-04 |
| `class SqlModel(Model)` | `FIX` ← `SqlTransformation` | `sql_file: str`, a resource path relative to `models/sql/`. `sql_template` property loads it with `importlib.resources`. `run`: `ctx.warehouse.render(...)` then `create_table_as_select(ctx.outputs[only])`. Exactly one output is required. The current version calls `warehouse.render_query(…, identifiers=…)` and `.sql` on the result, and neither exists (D14). | FR-DEV-02 |
| `class PythonModel(Model)` | `FIX` ← `PythonTransformation` | `compute(inputs: Mapping[str, DataFrame], settings) -> DataFrame \| Mapping[str, DataFrame]`. `run`: reads each input, calls `compute`, then `_write_outputs`. The current version allows only one output and doesn't pass `settings`. | FR-DEV-03, -05, FR-ENV-07 |
| `_write_outputs(ctx, result)` | `ADD` | Normalizes a single DataFrame or a dict, checks the keys equal `outputs` exactly (a missing or extra output is a `ModelError`), and calls `write_table` for each. | FR-DEV-05 |
| `class ModelError` | `ADD` | Wraps failures with the model name. | FR-OBS-03 |

**Multiple outputs (FR-DEV-05) across the framework.** Each layer works per table, not per model:

| Layer | Per-table behavior |
|---|---|
| `Registry` | `producer_of[table]`; `table_graph` has an edge from every input to every output. |
| `selector` | A table name selects its producing model; `+table` walks upstream from that model. |
| `planner` | `input_sources` is classified per input table, so a downstream model can depend on one output of a multi-output model. |
| `RunContext` | One temp per output; state tracked per table. |
| `Orchestrator.validate_outputs` | Checks, `allow_empty` and column checks per output. Any failing output fails the whole model. |
| `DuckDBWarehouse.comment` | One description and column set per output. |
| Promotion, archives, rollback, publications | One `PromotionStep` and one `_mrl_publications` row per output table. |
| Run log | Row and column counts per output table. |

### `orchestration/registry.py`: `ADD`

Origin: the graph construction in `ModelSelector.__init__` and `_discover_models` in `orchestration/model_selector.py`.

| Member | Status | Notes | Req |
|---|---|---|---|
| `class Registry(models: Sequence[Model])` | `ADD` | Keeps registration order (used as the tiebreak, as `import_order_index` is today). | FR-RUN-07 |
| `.producer_of: dict[str, Model]` | `ADD` | Raises `GraphError` if two models produce the same table. | FR-DEV-07 |
| `.table_graph: nx.DiGraph` | `ADD` | Edges from each input table to each output table. | FR-DEV-05 |
| `.model_graph: nx.DiGraph` | `FIX` ← `ModelSelector.graph` | An edge from A to B if B reads a table A produces. Today the edges come from `deps` lists. | FR-DEV-07 |
| `.validate()` | `FIX` ← the unknown-dependency check in `ModelSelector.__init__` and the cycle check in `topological_order` | Reports every unproduced input and every cycle path (`nx.find_cycle`), all at once. | FR-DEV-07 |
| `.model(name_or_table) -> Model` | `ADD` | Looks up by model name or by output table name. | FR-RUN-02 |
| `default_registry() -> Registry` | `ADD` | `Registry(models.MODELS)`. | FR-DEV-06 |
| `class GraphError` | `ADD` | | FR-DEV-07 |
| `_discover_models`, `ALL_MODELS_BY_NAME`, `load_models` | `DELETE` | Replaced by the explicit `models.MODELS` list (FR-DEV-06, NFR-04). | |

### `orchestration/selector.py`: `MOVE+FIX`

Origin: `ModelSelector._resolve_selector` / `_resolve_selection` in `orchestration/model_selector.py`.

| Member | Status | Notes | Req |
|---|---|---|---|
| `select(registry, select: str \| None, exclude: str \| None = None) -> set[str]` | `FIX` | The union/intersection grammar is `KEEP`. Add `exclude`. Return model names. | FR-RUN-02, -03 |
| `_resolve_atom(registry, atom) -> set[str]` | `FIX` ← `_resolve_selector` | Keep `+x`, `x+`, `+x+`. Add `tag:<t>`. Accept table names as well as model names. | FR-RUN-02 |
| `class SelectionError` | `ADD` | Currently a `ValueError`. Raised for empty matches and unknown names. | FR-RUN-02 |

### `orchestration/planner.py`: `ADD`

Origin: `ModelSelector.topological_order`.

| Member | Status | Notes | Req |
|---|---|---|---|
| `InputSource` | `ADD` | `Literal["run", "primary"]` | FR-RUN-04 |
| `PlannedModel` (dataclass) | `ADD` | `model, input_sources: dict[str, InputSource], upstream: tuple[str, ...]` | FR-RUN-05 |
| `Plan` (dataclass) | `ADD` | `models: tuple[PlannedModel, ...]`, `required_primaries: frozenset[str]`, `needs_sources: bool`, `.render() -> str` (a text table for `mrl plan`), `.to_dict()` (for the run log). | FR-RUN-05, FR-OBS-02 |
| `build_plan(registry, selected) -> Plan` | `FIX` ← `topological_order` | Keep `lexicographical_topological_sort` with the registration-order key. Add the input-source classification. | FR-RUN-04, -05, -07 |
| `check_required_primaries(plan, warehouse)` | `ADD` | Raises `PlanError` listing every missing primary. | FR-RUN-06 |
| `class PlanError` | `ADD` | | FR-RUN-06 |

### `orchestration/run_context.py`: `ADD`

Origin: the abstract `RunContext` in `orchestration/orchestration.py` (interface only).

| Member | Status | Notes | Req |
|---|---|---|---|
| `TableState` (enum) | `ADD` | `ALLOCATED, PRODUCED, FAILED, PROMOTED, ABANDONED`. Replaces the `empty/temp/promoted` states in `io/warehouse_table.WarehouseTable`. | FR-SAFE-02 |
| `class RunContext(warehouse, run_id, run_stamp)` | `ADD` | Implements the ABC. | |
| `.run_id`, `.run_stamp` | `ADD` | From the abstract property `run_id`. | FR-SAFE-04, -09 |
| `.resolve_input(logical) -> TableRef` | `ADD` | The run's temp table if `PRODUCED`, otherwise the primary. Raises if the primary is missing. | FR-RUN-04 |
| `.allocate_output_temp(logical) -> TableRef` | `ADD` | Drops any leftover table with the same name; sets `ALLOCATED`. | FR-SAFE-09 |
| `.record_produced(logical)` | `ADD` ← abstract `record_produced_temp` | Requires `ALLOCATED` and that the table exists. | |
| `.mark_failed(logicals)` | `ADD` | | FR-SAFE-02 |
| `.promotion_plan(archive: bool) -> PromotionPlan` | `ADD` | Sets `primary_existed` per step. | FR-SAFE-01, -07 |
| `.promote(archive=True)` | `ADD` ← abstract `promote_temp_tables` | Calls `warehouse.promote(plan)` and sets `PROMOTED`. | FR-SAFE-01 |
| `.produced_tables() -> list[str]` | `ADD` | | FR-OBS-03 |

### `orchestration/run_log.py`: `ADD`

The per-run local log (FR-OBS-02, -03, -04). One directory per run: `<log_dir>/<env>/<run_id>/`.

| Member | Notes | Req |
|---|---|---|
| `class RunLogWriter(log_dir, env, run_id)` | Context manager. On enter, creates the directory and attaches a `logging.FileHandler` (`run.log`, one line per record, with timestamps) to the `mrl_pipeline` logger. On exit, **always** writes `run.json` (by writing a temp file and renaming it) with the final status, including the exception if one escaped, and detaches the handler. | FR-OBS-02, -04 |
| `.record_start(settings_masked, selection, plan, target, location, git_sha)` | | FR-OBS-02, FR-ENV-09 |
| `.record_source(FileInfo)` | Collects file ID, title and modified time for every Drive file read. Bound to `ModelContext.note_source`. | FR-SRC-11 |
| `.record_model(ModelResult)` | Per-model outputs (row and column counts per table), timing, status, error, warnings. | FR-OBS-03 |
| `.record_finish(RunResult)` | Status, promoted flag, end time, error. | FR-OBS-02 |
| `.path` | Shown by the CLI at the end of a run. | FR-OBS-02 |
| `list_runs(log_dir, env, limit=20) -> list[RunSummary]` | Reads `run.json` files, newest first. Runs with no `run.json` (killed process) are listed as `incomplete`. | FR-OBS-07 |
| `git_sha() -> str \| None` | For the `code_version` field. | FR-OBS-02 |
| `format_model_line(result) -> str` | `model=… status=… rows=… cols=… secs=…`. Multi-output models list rows per table. | FR-OBS-04 |

On a Prefect worker the log directory is local to the worker; mount `log_dir` on a volume, or rely on Prefect's own copy of the console log (R10).

### `orchestration/orchestrator.py`: `ADD`

Origin: the abstract `Orchestrator` in `orchestration/orchestration.py` (`register` / `plan` / `run`).

| Member | Status | Notes | Req |
|---|---|---|---|
| `RunResult` (dataclass) | `ADD` | `run_id, status, promoted, model_results: dict[str, ModelResult], error, log_path` | FR-OBS-02 |
| `ModelResult` (dataclass) | `ADD` | `status (ok/failed/skipped), outputs: dict[str, TableSummary], seconds, error, warnings` | FR-OBS-03 |
| `class Orchestrator(registry, settings)` | `ADD` | `register` from the ABC is dropped, because registration belongs to `Registry`. | |
| `.plan(select, exclude) -> Plan` | `ADD` | `selector.select`, then `planner.build_plan`. | FR-RUN-05 |
| `.run(select=None, exclude=None, promote=True, archive=True, force=False, executor=None) -> RunResult` | `ADD` | Steps, in order: make run ID → open `RunLogWriter` → plan → `settings.validate_for_run(plan.needs_sources)` → open warehouse → `ensure_metadata_tables` → `RunLock.acquire(force)` → `check_required_primaries` → execute each model (or hand it to `executor`, which is how Prefect plugs in) → if every model succeeded and `promote`: `RunContext.promote(archive)` → `RunLock.release` → `record_finish`. **No archive pruning** (FR-SAFE-05). The lock is released in a `finally`. | FR-RUN-01…-12, FR-SAFE-01…-05 |
| `.run_model(planned, ctx) -> ModelResult` | `ADD` | Resolve inputs → allocate temps → `model.run(ModelContext)` → `validate_outputs` → `warehouse.comment` → `record_produced` → `RunLock.heartbeat` → `RunLogWriter.record_model`. Called by Prefect tasks. | FR-OBS-01, -03 |
| `validate_outputs(model, ctx) -> dict[str, list[str]]` | `ADD` | Per output: the table exists, the documented columns are present, it isn't empty unless allowed, and `model.checks(ctx)`. Then `model.warning_checks(ctx)`, whose results go to the model's warnings. | FR-SAFE-03, FR-DOM-09b |
| Skip handling | `ADD` | When a model fails, its descendants in `model_graph` are marked `skipped`. Independent branches keep running. | FR-RUN-12 |

### `orchestration/flow.py`: `FIX`

Origin: `orchestration/flows.py`.

| Member | Status | Notes | Req |
|---|---|---|---|
| Module-level `imported_models = load_models()` / `model_tasks = {...}` | `DELETE` | Defect D12, against NFR-04. | NFR-04 |
| `@flow mrl_pipeline(env="dev", select=None, exclude=None, promote=True, archive=True, force=False)` | `FIX` ← `run_pipeline` | Builds settings, a `Registry` and an `Orchestrator` inside the flow, and passes a Prefect executor to `Orchestrator.run`. | FR-RUN-08, FR-ENV-02 |
| `@task run_model_task(model_name, ...)` | `FIX` ← `WarehouseOperation.build.run_model` | One task per model, named `model.name`. Calls `Orchestrator.run_model`. Returns the `ModelResult` as a dict. Uses `get_run_logger()` in addition to the run log file. | FR-RUN-08, FR-OBS-04 |
| `PrefectExecutor` | `ADD` | Submits tasks in plan order with `wait_for=[upstream futures]`. This dependency logic is `KEEP` from `run_pipeline`. | FR-RUN-08 |
| `if __name__ == "__main__"` | `KEEP` | | |

---

## 7. `models/`

Each model is a thin wrapper: it declares inputs, outputs, documentation and tags, and calls a pure function in `transforms/` (FR-DEV-08). Column descriptions come from the existing `column_descriptions` dicts in the current `models/*.py` files where they exist, and are **added** for the rest (FR-OBS-01).

### `models/__init__.py`: `FIX`
Today it imports `PipelineModel` (D1). It becomes `MODELS: list[Model]`, in the registration order shown in §7.6. Req: FR-DEV-06.

### `models/sources.py`: `ADD`

Every prep-sheet model finds its sheet with `sources.open_prep_sheet(settings.<x>_sheet)` (exact title inside the configured folder, FR-SRC-12) and calls `ctx.note_source(...)` (FR-SRC-11). All values are kept as text.

| Class | Type | Outputs | Origin | Status notes | Req |
|---|---|---|---|---|---|
| `PrepAllRaces` | `SourceModel` | `prep_all_races` | `legacy/build_dim_races` (its `data.iter_prep_tables("prep_all_races")` part) | `ADD`. Sheet `settings.races_sheet`. Concatenates every worksheet with `race_season = int(worksheet title)`. | FR-SRC-08, FR-DOM-01 |
| `PrepPeopleAndSpellings` | `SourceModel` | `prep_names_equivalent_spellings`, `prep_non_people` | Inputs to `legacy/build_name_replacement_artifacts` | `ADD`. Sheet `settings.people_sheet`; tabs `people_name_spellings_tab` and `people_non_persons_tab` (Q4). Two outputs from one sheet (FR-DEV-05). | FR-SRC-08 |
| `PrepSchools` | `SourceModel` | `prep_schools_equivalent_spellings` | Input to `legacy/build_name_replacement_artifacts` | `ADD`. Sheet `settings.schools_sheet`, first worksheet. | FR-SRC-08 |
| `PrepPenaltyConfig` | `SourceModel` | `prep_penalty_publication_dates`, `prep_penalty_contribution_windows`, `prep_penalty_contributions` | Inputs to `legacy/build_ext_races_by_penalty_window`, `build_ext_results` | `ADD`. One sheet setting per output (FR-ENV-11). | FR-SRC-08 |
| `StgRegistrations` | `SourceModel` | `stg_registrations` | Input to `legacy/build_dim_registrations` | `ADD`. Sheet `settings.registrations_sheet`. Reads every tab; `race_season = int(tab title)`, failing on a non-year title. Concatenates with all columns as text. | FR-SRC-09 |
| `InfraResultFiles` | `SourceModel` | `infra_result_files` | `models/infra_result_files.run_infra_result_files` | `FIX`. Uses `sources.drive.list_sheets_by_prefix(settings.results_sheet_prefix, folder_id=settings.google_drive_source_folder_id)` and `find_duplicate_titles` (raise on any). Writes through the framework instead of `CREATE OR REPLACE`. Notes every file as a source. Column descriptions: `KEEP` from the current model, renaming `race_id` → `sheet_name`. | FR-SRC-02, -03, -11 |

### `models/stg_results.py`: `FIX`

The file name is reused; its content is replaced.

| Member | Status | Origin / notes | Req |
|---|---|---|---|
| `class StgResults(SourceModel)` | `ADD` | Input `dim_races` (which already carries each results sheet's file ID, §7 `DimRaces`). Output `stg_results`. `allow_empty = {"stg_results"}` only when `settings.sample_races` is true. Overrides `run` (it reads straight into DuckDB). | FR-DOM-03 |
| `StgResults.run(ctx)` | `ADD` | (1) `transforms.results_ingest.build_needed_results_list(dim_races)`, which now includes file IDs. (2) For each task, in a thread pool (`settings.source_max_workers`) with `ctx.warehouse.cursor()`: `gviz.read_csv_into` into a scratch table, then `standardize_select_sql` and an insert into `ctx.outputs["stg_results"]`. (3) On `SourceFetchError`, log and count (FR-SRC-07). (4) Drop the scratch tables. (5) Add the failure count to the model's warnings. | FR-DOM-03, FR-SRC-07, NFR-01 |
| `run_infra_result_files`, `transform_results_and_insert_into_staging_table`, `write_from_thread`, `PipelineModel` subclass | `DELETE` | The thread-cursor pattern and `column_map` (same as legacy `STANDARD_COL_MAP`) are carried over. The **legacy behavior is the reference** where the two versions differ (FR-QA-04, R3). | |

### `models/dims.py`: `ADD`

| Class | Type | Inputs → Outputs | Calls | Req |
|---|---|---|---|---|
| `DimRaces` | `PythonModel` | `prep_all_races, infra_result_files` → `dim_races` | `transforms.dim_races.build_dim_races(df, sample=settings.sample_races, seed=settings.sample_seed)` on the full list first; then `results_ingest.resolve_results_sheets(full_races, infra_result_files)`, which raises for FR-SRC-03/-06 and returns warnings for FR-SRC-05 (orphans, against the **unsampled** list); then sampling; then FR-SRC-04 warnings for the kept races; then `add_results_files_to_dim_races` (file IDs and `prep_results_url_b/_g`). Moving sheet resolution here (it needs only tables, not Google) removes the `dim_races` ↔ `stg_results` coupling that forced legacy to build both in one function. | FR-DOM-01, -02, FR-SRC-03…-06 |
| `DimResultsAndRegistrations` | `PythonModel` | `stg_results, dim_races, stg_registrations, prep_names_equivalent_spellings, prep_schools_equivalent_spellings, prep_non_people` → `dim_results, dim_registrations` | `transforms.names.build_name_lookup` (NEW, Phase 7; before that the legacy `build_name_replacement_artifacts`), then `build_dim_results`, `build_dim_registrations`. Name-table warnings go to the model's warnings. `checks`: `result_id` and `registration_id` are unique. | FR-DOM-04…-06, FR-DEV-05 |
| `DimAthletes` | `PythonModel` | `dim_registrations` → `dim_athletes` (Phases 5–6); **`dim_results, dim_registrations` → `dim_athletes`** (NEW, Phase 7) | `transforms.dim_athletes.build_dim_athletes`. `checks`: `athlete_id` is unique. | FR-DOM-07 |

### `models/penalties.py`: `ADD`

| Class | Type | Inputs → Outputs | Calls | Req |
|---|---|---|---|---|
| `ExtRacesByPenaltyWindow` | `PythonModel` | `prep_penalty_publication_dates, prep_penalty_contribution_windows, dim_races` → `ext_races_by_penalty_window` | `transforms.penalty_windows.build_ext_races_by_penalty_window` | FR-DOM-08 |
| `Penalties` | `PythonModel` | `dim_registrations, dim_results, ext_races_by_penalty_window` → `ext_results_by_penalty_window, ext_athlete_penalties` | `transforms.penalties.build_penalties`, with the returned tuple mapped to a dict. Adds `_27` columns in Phase 7. `warning_checks` (Phase 7): any `standardized_athlete_penalty_27 <= 0`, listing up to 20 rows. | FR-DOM-09, -09a, -09b, -10, -15, FR-DEV-05 |

### `models/ext.py` and `models/sql/*.sql`: `ADD` / `MOVE+FIX`

| Class | SQL file | Inputs → Output | Phase 5 change from legacy | Phase 7 change (NEW) | Req |
|---|---|---|---|---|---|
| `ExtResults` | `ext_results.sql` ← `legacy/build_ext_results.sql` | `prep_penalty_contributions, ext_athlete_penalties, dim_results, dim_races, dim_registrations` → `ext_results` | Replace `df_<name>` with `{{ ref('<name>') }}`. Remove the trailing `;` and any `ORDER BY` that only affects display. No other changes. | Add, alongside the `_26` versions: `athlete_penalty_27`, `is_penaltied_athlete_finish_27`, `penaltied_athlete_place_27`, `penaltied_athlete_count_27`; CTE `race_penalties_and_times_27` (`AVG(athlete_penalty_27)`, `GEOMETRIC_MEAN(time_float)`, places 1–6, count ≥ 3); `race_penalty_27`, `race_penalty_time_27`; `compression_factor_27` (same `CASE` as `_26`); `ma_time_penalty_ratio_27`; `race_points_27 = ROUND(race_penalty_27 + compression_factor_27 * LN(ma_time_penalty_ratio_27) / LN(1.001), 2)` (the base-1.001 log); both `_27` ranks; `ma_points_2027_str`; `reason_for_no_points_27` (season ≥ 2027). `ExtResults.warning_checks`: any `race_points_27 <= 0`. | FR-DOM-09b, -11, -11a, -12, -15 |
| `ExtPointsContributingResults` | `ext_points_contributing_results.sql` ← legacy | `ext_results, dim_registrations` → `ext_points_contributing_results` | Same as above | Add `race_rank_excluding_maq_27`, the `maq_*_27` fields, a `best27` join, the `contributing_*_27` and `coalesced_*_27` fields, and `race_penalty_27` in the `WHERE` filters. | FR-DOM-13, -15 |
| `ExtRegistrations` | `ext_registrations.sql` ← legacy | `ext_points_contributing_results, dim_registrations` → `ext_registrations` | Same as above | Add the `_27` technique points, total, MAQ-finisher flag and qualifying places. | FR-DOM-14, -15 |

The legacy Python wrappers (`legacy/build_ext_*.py`, which register DataFrames on `duckdb.connect(":memory:")`) are `DELETE`. The framework runs the SQL directly in the warehouse.

### `models/qa.py`: `ADD` (NEW)

| Class | Type | Inputs → Output | Calls | Req |
|---|---|---|---|---|
| `NameConflicts` | `PythonModel` | `dim_results, dim_races` → `qa_name_conflicts` | `transforms.name_conflicts.find_name_conflicts(dim_results, dim_races, exclusive_leagues=settings.exclusive_league_series, ec_series=settings.ec_series, ec_age_groups=settings.ec_age_groups, u16_champs_series=settings.u16_champs_series)`. `allow_empty = {"qa_name_conflicts"}`. `warning_checks`: the count of unresolved conflicts per rule, with a sample of names. Never blocks publishing. | FR-DOM-04d |

### 7.6 Registration order and resulting graph

| # | Model | Type | Reads | Writes | Tags |
|---|---|---|---|---|---|
| 1 | `PrepAllRaces` | source | Google: races sheet | `prep_all_races` | `source` |
| 2 | `PrepPeopleAndSpellings` | source | Google: people sheet | `prep_names_equivalent_spellings`, `prep_non_people` | `source` |
| 3 | `PrepSchools` | source | Google: schools sheet | `prep_schools_equivalent_spellings` | `source` |
| 4 | `PrepPenaltyConfig` | source | Google: 3 penalty sheets | `prep_penalty_publication_dates`, `prep_penalty_contribution_windows`, `prep_penalty_contributions` | `source`, `penalties` |
| 5 | `StgRegistrations` | source | Google: registrations sheet | `stg_registrations` | `source` |
| 6 | `InfraResultFiles` | source | Google: Drive folder | `infra_result_files` | `source` |
| 7 | `DimRaces` | Python | 1, 6 | `dim_races` | `dims` |
| 8 | `StgResults` | source | 7; Google: results sheets | `stg_results` | `source` |
| 9 | `DimResultsAndRegistrations` | Python | 8, 7, 5, 2, 3 | `dim_results`, `dim_registrations` | `dims` |
| 10 | `DimAthletes` | Python | 9 | `dim_athletes` | `dims` |
| 11 | `ExtRacesByPenaltyWindow` | Python | 4, 7 | `ext_races_by_penalty_window` | `penalties` |
| 12 | `Penalties` | Python | 9, 11 | `ext_results_by_penalty_window`, `ext_athlete_penalties` | `penalties` |
| 13 | `ExtResults` | SQL | 4, 12, 9, 7 | `ext_results` | `points` |
| 14 | `ExtPointsContributingResults` | SQL | 13, 9 | `ext_points_contributing_results` | `points` |
| 15 | `ExtRegistrations` | SQL | 14, 9 | `ext_registrations` | `points` |
| 16 | `NameConflicts` (NEW, Phase 7) | Python | 9, 7 | `qa_name_conflicts` | `qa`, `dims` |

Examples: `mrl run --select tag:points` rebuilds 13–15 from published inputs; `mrl run --select dim_results+` rebuilds 9 and everything after it (selecting the table `dim_results` selects model 9, which also rebuilds `dim_registrations`).

---

## 8. `transforms/`

These are pure functions: pandas in, pandas out. They don't import `settings`, `duckdb`, Google libraries or `print`. Each is ported from `legacy/` with **no change to its logic** in Phase 5 (FR-QA-04). The rows marked **NEW** are Phase 7 changes.

### `transforms/ids.py`: `MOVE+FIX`

From `legacy/build_dim_results_and_dim_registrations.py`.

| Function | Status | Notes | Req |
|---|---|---|---|
| `calc_athlete_id(df)` | `MOVE`, then **NEW** `FIX` | Phase 5: unchanged (`gender,athlete_name`). Phase 7: hash `f"{gender},{athlete_name},{disambiguation}"`, with a missing disambiguation treated as `""`. | FR-DOM-06 |
| `calc_result_id`, `calc_registration_id` | `MOVE` | Unchanged formulas; values change in Phase 7 only because `athlete_id` changes. | FR-DOM-06 |

A test pins hash values for known inputs, for both the Phase 5 and the Phase 7 formula.

### `transforms/names.py`: `ADD` (NEW)

Origin: `legacy/build_name_replacement_artifacts`, which builds a positional `dict[variant → canonical]` with no season.

| Function | Notes | Req |
|---|---|---|
| `NAME_TABLE_COLUMNS = ("name_raw", "race_season", "athlete_name", "disambiguation")` | Header names to be confirmed (Q4). | FR-DOM-04a |
| `build_name_lookup(df_names) -> DataFrame` | Selects the four columns by header; trims whitespace; a non-blank `race_season` to integer, a blank one to null ("every season"); blank `disambiguation` → `""`. Raises `NameTableError` for a non-integer season, or for a key (`name_raw`, `race_season`, with null as its own value) that has two different targets. Exact duplicates are dropped. | FR-DOM-04a, -04c |
| `name_lookup_warnings(lookup, observed) -> list[str]` | Warns when a canonical name and gender appears in a season both with and without a disambiguation. `observed` is the resolved results and registrations. | FR-DOM-04c |
| `apply_name_lookup(df, *, name_col, season_col, lookup) -> DataFrame` | Two left joins: first on (`name`, `season`) against season-specific rows, then on `name` alone against blank-season rows for the rows still unmatched. Adds `athlete_name` and `disambiguation`; rows matched by neither get the cleaned name and `""`. Used by both `build_dim_results` and `build_dim_registrations`. | FR-DOM-04a, -05 |
| `build_school_replacements(df)`, `build_non_people(df)` | `MOVE` ← the school-dict and non-people parts of `build_name_replacement_artifacts`. | FR-DOM-04 |
| `class NameTableError` | | FR-DOM-04c |

### `transforms/name_conflicts.py`: `ADD` (NEW)

Pure function over `dim_results` joined to `dim_races` (`race_date`, `race_name`, `series`, `division`, `venue`). Rows with `is_unidentified_athlete` are dropped first. Every rule runs within `gender`.

| Function | Notes | Req |
|---|---|---|
| `RULES = ("same_sheet", "exclusive_leagues", "u16_vs_u18_u20")`, `KEY_TYPES = ("name_clean", "athlete_id")` | | FR-DOM-04d |
| `same_sheet_conflicts(df, key)` | Groups by (`race_id`, `gender`, key) with more than one row. `conflict_group` = hash of rule, key type, `race_id`, gender and key value. | FR-DOM-04d (a) |
| `exclusive_league_conflicts(df, key, leagues)` | Keeps rows whose `series` is in `leagues`; groups by (`race_season`, `gender`, key) with more than one distinct `series`. | FR-DOM-04d (b) |
| `age_set(df, *, ec_series, ec_age_groups, u16_champs_series) -> Series` | Labels each row `U16` (an Eastern Cup race whose `division` maps to U16, or any race whose `series` is `u16_champs_series`), `U18/U20` (an Eastern Cup race whose `division` maps to U18/U20), or null. Eastern Cup divisions that map to neither are ignored and counted in a warning. | FR-DOM-04d (c) |
| `age_set_conflicts(df, key)` | Groups by (`race_season`, `gender`, key) where both `U16` and `U18/U20` appear. | FR-DOM-04d (c) |
| `find_name_conflicts(df_dim_results, df_dim_races, *, exclusive_leagues, ec_series, ec_age_groups, u16_champs_series) -> DataFrame` | Runs every rule for both key types and concatenates the results, one row per (conflict group, result). `is_resolved`: for key type `name_clean`, true when the same rule finds no conflict on `athlete_id` for those rows; for key type `athlete_id`, always false. Output columns as listed in FR-DOM-04d. Sorted by `is_resolved`, rule, season, name. | FR-DOM-04d |

### `transforms/dim_races.py`: `MOVE+FIX`

| Function | Status | Fix | Req |
|---|---|---|---|
| `build_dim_races(df_prep_all_races, *, sample: bool, seed: int \| None = None)` | `FIX` ← legacy `build_dim_races(data, warehouse_env)` | Takes the already-combined DataFrame instead of `data`. `sample` replaces `warehouse_env == "dev"`. Add `seed` so sampled runs can be reproduced. | FR-DOM-01, -02, FR-ENV-07, FR-DEV-08 |
| `sample_races(df, seed)` | `ADD` | Pulled out of `build_dim_races` so `DimRaces` can sample after sheet resolution. | FR-DOM-02, FR-SRC-05 |

### `transforms/results_ingest.py`: `MOVE+FIX`

From `legacy/build_stg_results_and_dim_races.py`.

| Function | Status | Notes | Req |
|---|---|---|---|
| `STANDARD_COL_MAP`, `STD_COLS` | `MOVE` | | FR-DOM-03 |
| `first_matching_col_expr(cols, candidates)` | `FIX` ← `_first_matching_col_expr` | Quote with `naming.quote_ident`. | NFR-03 |
| `standardize_select_sql(actual_cols, *, race_id, gender, source_file_id, prep_results_url, source_table) -> (sql, params)` | `FIX` ← `standardize_raw_table_in_duckdb` | Returns a parameterized `SELECT` instead of running `CREATE OR REPLACE` with values inserted into the string (D17). The model does the insert. | NFR-03 |
| `build_needed_results_list(df_dim_races)` | `FIX` | Also returns the file IDs and URLs now stored on `dim_races`. | FR-DOM-03 |
| `resolve_results_sheets(df_races, df_drive) -> (resolved, warnings)` | `FIX` ← `resolve_sheet_names_to_ids` + `validate_drive_lookup_against_races` + `test_one_to_one_race_gender_to_sheet` | Uses the `sheet_name, file_id` columns from `DriveSource`. Raises for FR-SRC-03 and -06. Returns warnings for FR-SRC-04 and -05 instead of printing them. | FR-SRC-03…-06 |
| `add_results_files_to_dim_races(df_dim_races, resolved)` | `MOVE+FIX` ← `add_results_urls_to_dim_races` | Keep `prep_results_url_b/_g`; add `prep_results_file_id_b/_g`. | FR-DOM-01 |
| `SheetTask` | `MOVE` | Frozen dataclass. | |
| `_reset_duckdb_file`, `_init_duckdb_file`, `union_standardized_tables_to_df`, `process_one_sheet_into_duckdb`, `build_stg_results_and_dim_races`, `DriveService` | `DELETE` | Replaced by the warehouse, `sources/gviz.py`, `DimRaces` and `StgResults.run`. | |

### `transforms/dim_results_registrations.py`: `MOVE+FIX`

From `legacy/build_dim_results_and_dim_registrations.py`.

| Function | Status | Notes | Req |
|---|---|---|---|
| `timestr_to_min`, `float_time_to_str` | `MOVE` | | FR-DOM-04 |
| `build_name_replacement_artifacts(...)` | `MOVE`, then `DELETE` in Phase 7 | Replaced by `transforms/names.py`. | FR-DOM-04 |
| `build_dim_results(...)` | `FIX`, then **NEW** `FIX` | Phase 5: remove the commented-out `put_warehouse_table` and any `print`. Phase 7: keep the cleaned, title-cased name as a new `name_clean` column, which is no longer dropped. Replace `.replace(athlete_name_replacements)` with `names.apply_name_lookup(name_col="name_clean", season_col="race_season")`. Set `disambiguation = ""` for non-person rows. Keep `disambiguation` in the output. Take the Eastern Cup series code from the `ec_series` argument. | FR-DOM-04, -04a, -04b, -04d, -06 |
| `build_dim_registrations(...)` | `MOVE`, then **NEW** `FIX` | Phase 7: replace both `.replace(athlete_name_replacements)` calls (eligible and ghost rows) with `apply_name_lookup` on the built full name and `race_season`; keep `disambiguation` in the output. | FR-DOM-05, -06 |

### `transforms/dim_athletes.py`: `MOVE+FIX`

| Function | Status | Notes | Req |
|---|---|---|---|
| `build_dim_athletes(df_dim_registrations)` | `MOVE`, then **NEW** `FIX` → `build_dim_athletes(df_dim_results, df_dim_registrations)` | Phase 7: start from the distinct (`athlete_id`, `athlete_name`, `disambiguation`, `gender`) in identified `dim_results` rows and non-ghost `dim_registrations` rows. Left-join the existing "most recent non-ghost registration" attributes and the `was_registered_eligible_<season>` pivot (filled with false). `birth_year` and `graduation_year` become nullable `Int64`. Exactly one row per `athlete_id`; raise if one ID maps to two names (a hash collision or data error). | FR-DOM-07 |

### `transforms/penalty_windows.py`: `MOVE+FIX`

| Function | Status | Notes | Req |
|---|---|---|---|
| `race_dates_in_windows`, `calculate_penalty_windows` | `MOVE` | | FR-DOM-08 |
| `build_ext_races_by_penalty_window(...)` | `FIX` | Replace `warnings.warn` with returned warnings, which the model logs. Keep `staleness_months=3`. | FR-DOM-08, FR-OBS-04 |

### `transforms/penalties.py`: `MOVE+FIX`

| Function | Status | Notes | Req |
|---|---|---|---|
| `build_df_results_by_penalty_window_prep`, `build_df_ext_athlete_penalties_prep`, `calculate_penalties_2025` | `MOVE` | Keep the logic. Make `tqdm` progress bars optional (`progress: bool = False`) so Prefect logs stay clean. | FR-DOM-09, -10 |
| `standardize_penalties_first_date_cohort(..., method="scale")` | `MOVE`, then **NEW** `FIX` | Phase 7: add `method: Literal["scale","shift"]`. `"scale"` is today's `target_mean / robust_mean` factor (default, used by `_25`/`_26`). `"shift"` adds `target_mean − robust_mean` instead. | FR-DOM-09, -09a |
| `fit_fixed_effects_2026(df, ...) -> (df_coef, df_diag)` | **NEW** `ADD` ← the per-group fit loop of `calculate_penalties_2026` | Returns the raw athlete coefficients per (season, date, gender, athlete), before `exp`, plus the outlier diagnostics. | FR-DOM-09a, -10 |
| `calculate_penalties_2026` | `MOVE`, then **NEW** `FIX` | Phase 7: becomes `fit_fixed_effects_2026` → `exp` → `scale_to_target_mean` → standardize (scale). Output must equal the Phase 5 output exactly (test). | FR-DOM-09 |
| `LOG_BASE_27 = 1.001`, `SCALE_27 = 1 / math.log(LOG_BASE_27)` (≈ 1000.5), `TARGET_MEAN_27 = 500.0` | **NEW** `ADD` | Module constants, also passed as defaults to the functions below. | FR-DOM-09a |
| `center_and_scale(arr, *, target_mean, scale, robust_pctiles)` | **NEW** `ADD` | `target_mean + scale × (arr − interquartile mean of arr)`. | FR-DOM-09a |
| `calculate_penalties_2027(df_coef, *, target, target_mean=TARGET_MEAN_27, scale=SCALE_27, robust_pctiles=(0.25, 0.75))` | **NEW** `ADD` | Per (season, date, gender) group: `center_and_scale(coef)` → `athlete_penalty_27`. Then `standardize_penalties_first_date_cohort(method="shift", target_mean=500)` → `standardized_athlete_penalty_27`. Aligned to `target` like the others. | FR-DOM-09a |
| `build_penalties(...)` | `MOVE`, then **NEW** `FIX` | Phase 7: fit once, then derive `_26` and `_27` from the same coefficients; assign both. Sort order stays on `standardized_athlete_penalty_26`. | FR-DOM-09, -09a, -15 |

---

## 9. `tests/`

| Path | Status | Contents | Req |
|---|---|---|---|
| `tests/conftest.py` | `ADD` | Fixtures: `settings_memory` (`load_settings("dev", local_duckdb_path=":memory:", log_dir=tmp_path)`), `warehouse` (`DuckDBWarehouse` on `:memory:`), `fake_sources`. | FR-QA-01 |
| `tests/fixtures/` | `ADD` | Small Parquet and CSV files for every source tab and the expected outputs. Built once from a real stg run with personal data reduced to a minimum. | FR-QA-02 |
| `tests/unit/test_settings.py` | `ADD` | Load order, env mismatch, secret masking, schema, database and target derivation, input-setting defaults, `sample_races` independent of env. | FR-ENV-02, -05, -07, -08, -09, -11 |
| `tests/unit/test_naming.py` | `ADD` | Sanitizing, quoting hostile names, round-trip of `parse_physical_name`. | NFR-03, FR-WH-06 |
| `tests/unit/test_selector.py` | `ADD` | Port every case the current `ModelSelector` handles, plus `tag:`, `exclude` and table names. | FR-RUN-02, -03 |
| `tests/unit/test_registry_planner.py` | `ADD` | Duplicate producers, missing inputs, cycles, multi-output edges, depending on one output of a multi-output model, fixed order, input-source classification. | FR-DEV-05, -07, FR-RUN-04…-07 |
| `tests/unit/test_render.py` | `ADD` | `ref()` substitution and the unknown/unused ref errors. | FR-DEV-02 |
| `tests/unit/test_run_log.py` | `ADD` | `run.json` written on success, on model failure and on an exception escaping `run`; `list_runs` ordering and `incomplete` runs; summary-line format. | FR-OBS-02…-04, -07 |
| `tests/integration/test_warehouse.py` | `ADD` | Read and write, chunked writes, CTAS, exists, rename, comments, promotion including a failure injected partway, publication rows written in the same transaction. | FR-SAFE-01, -02, FR-OBS-01 |
| `tests/integration/test_metadata.py` | `ADD` | Lock acquire, refuse, force, stale takeover, release in `finally`. | FR-RUN-11 |
| `tests/integration/test_lifecycle.py` | `ADD` | List, drop and prune archives (including `dry_run`); a run never prunes; rolling back a table; rolling back a run that created a new table; cleaning leftovers. | FR-SAFE-05…-08 |
| `tests/integration/test_orchestrator.py` | `ADD` | Toy graph: selective runs read primaries, failure skips descendants and promotes nothing, a multi-output model with one bad output fails as a whole, run log filled in, concurrent-run guard. | FR-RUN-*, FR-SAFE-*, FR-DEV-05, FR-OBS-02, -03 |
| `tests/transforms/test_*.py` | `ADD` | One per `transforms/` module, using recorded fixtures. `test_ids.py` pins hash values. | FR-QA-02, FR-DOM-06 |
| `tests/transforms/test_names.py` | `ADD` (NEW) | Season-specific mapping, the same raw name mapping to different athletes in different seasons, unmatched names, conflicting keys, ambiguity warning. | FR-DOM-04a…-04c, FR-QA-06 |
| `tests/transforms/test_penalties_27.py` | `ADD` (NEW) | Hand-worked examples for `center_and_scale` (coefficients `ln(1.001)` apart come out exactly 1 unit apart, centered on 500) and the shift standardization; `_25` and `_26` values identical to the Phase 6 golden output. | FR-DOM-09a, FR-QA-06 |
| `tests/sql/test_ext_results_27.py` | `ADD` (NEW) | A small race with 6 penalized finishers, hand-worked `race_penalty_27` and `race_points_27` (a finisher 1% slower than the race-penalty time with compression 1 scores about 9.95 more points); a race with 2 penalized finishers is unscorable; a non-positive value triggers the FR-DOM-09b warning. | FR-DOM-09b, -11a, FR-QA-06 |
| `tests/transforms/test_name_conflicts.py` | `ADD` (NEW) | For each rule: a flagged case, a case resolved by a disambiguation (`is_resolved = true`), and a case where a spelling row merges two people (found by `athlete_id`); a U16 Championships race paired with a U18/U20 Eastern Cup race; the same name in U16 one season and U18 the next is not a conflict; unmapped divisions ignored; different genders never conflict. | FR-DOM-04d, FR-QA-06 |
| `tests/e2e/test_pipeline_offline.py` | `ADD` | `Orchestrator(default_registry()).run()` with `FakeSources` on `:memory:`. Every table is built and promoted. | FR-QA-01 |
| `tests/live/test_google_sources.py` | `ADD` | `@pytest.mark.live`: list `prep_results*` in the folder and read every configured prep sheet. | FR-QA-03 |

---

## 10. Files removed in v2

| File | Removed in | Reason |
|---|---|---|
| `src/mrl_pipeline/utils.py` | Phase 1 | Replaced by `settings.py`. `fetch_environ` and `.env` loading duplicate pydantic-settings. |
| `io/__init__.py` | Phase 4 | The package is gone. |
| `io/warehouse_service.py` | Phase 2 | Its interface only existed to make Sheets and DuckDB interchangeable (FR-WH-01). |
| `io/warehouse_table.py` | Phase 2 | Its state machine moves to `RunContext`. It also contains defect D4. |
| `io/table_version_ref.py` | Phase 2 | Replaced by `naming.py`. |
| `io/utils.py` | Phase 2 | Moved to `naming.py`. |
| `io/duckdb_service.py` | Phase 2 | Replaced by `warehouse/duckdb_warehouse.py`. |
| `io/database_connectors.py` | Phase 1 | Moved to `warehouse/connectors.py`. |
| `io/drive_service.py` | Phase 4 | Moved to `sources/drive.py`. |
| `io/google_sheets_warehouse_manager/sheet_service.py` | Phase 4 | Prep-reading methods moved to `sources/sheets.py`; warehouse methods dropped. |
| `io/google_sheets_warehouse_manager/schema_gsheet.py` | Phase 4 | Only used for Sheets warehouse tables. Replaced by `COMMENT ON` (FR-OBS-01). |
| `io/google_sheets_warehouse_manager/resilient_gspread_client.py` | Phase 4 | Moved to `sources/resilient_gspread.py`. |
| `warehouse/warehouse_backend.py` | Phase 2 | Split into `refs.py` and `duckdb_warehouse.py`. |
| `models/warehouse_operation.py`, `models/infra_result_files.py` | Phase 5 | Replaced by `orchestration/model.py` and `models/sources.py`. (`models/__init__.py` and `models/stg_results.py` are rewritten in place.) |
| `orchestration/transformation.py`, `orchestration/orchestration.py`, `orchestration/model_selector.py`, `orchestration/flows.py` | Phase 3 / 5 | Split into the new `orchestration/` modules. |
| `legacy/` (all 13 files) | Phase 8 | Ported to `transforms/` and `models/`. Kept until v2 matches v1 (FR-QA-04). |

---

## 11. Known defects in the current code

| # | Defect | Location | Fixed by |
|---|---|---|---|
| D1 | `models/pipeline_model.py` was deleted in `93cc07b`, but `models/__init__.py`, `infra_result_files.py` and `stg_results.py` still import `PipelineModel` from it. | `models/` | Phase 0 temporary fix; rewritten in Phase 5 |
| D2 | `model_selector.py` imports `PipelineModel` from `warehouse_operation`, which defines only `WarehouseOperation`. | `orchestration/model_selector.py` | Phase 0; `registry.py` |
| D3 | `warehouse_operation.py` imports `make_table_ref` from `duckdb_service.py`, which doesn't define it. | `models/warehouse_operation.py` | Phase 0; file deleted |
| D4 | `WarehouseTable.__init__` calls `TableVersionRef(table_name=…)`, but the parameter is `base_name`. It also reads `self.base_name` (an abstract property) before subclasses can set it up. | `io/warehouse_table.py` | File deleted |
| D5 | Names are quoted twice. `TableVersionRef.names` are already quoted, `DuckDBWarehouseService` quotes them again, and `_table_exists` compares quoted names against unquoted `information_schema` values, so it always returns false and promotion always raises "Temporary table missing". | `io/table_version_ref.py`, `io/duckdb_service.py` | `naming.py`, `DuckDBWarehouse.exists` |
| D6 | `DuckDBWarehouseService.__init__` requires keyword-only `schema`, `env`, `local_duckdb_path`, `schema_name` and `table_suffix` with no defaults, and ignores most of them. `connector` defaults to `None`, so `_connection()` fails. | `io/duckdb_service.py` | `DuckDBWarehouse` |
| D7 | The MotherDuck token is set after connecting to `md:`. | `io/database_connectors.py` | `warehouse/connectors.py` |
| D8 | `scikit-learn`, `numpy` and `python-dotenv` are imported but not declared; `pytest` and `typing` are listed as runtime dependencies. | `pyproject.toml` | Phase 0 |
| D9 | `requires-python >=3.9`, but the code uses `X \| None` in pydantic models. | `pyproject.toml` | Phase 0 |
| D10 | `Settings` never loads `config/*.env`. The default `schema_name="mrl_dev"` doesn't match the files. | `settings.py` | `load_settings` |
| D11 | Duplicates: two `WarehouseService` classes, two `TableSummary` classes, two `DriveService` classes (with different output columns), and three `TableVersionRef` variants. | `io/`, `warehouse/`, `legacy/` | Phases 2 and 4 |
| D12 | `flows.py` discovers models and builds Prefect tasks when the module is imported. | `orchestration/flows.py` | `flow.py` |
| D13 | `orchestration/orchestration.py` imports `Transformation` from the package `__init__` that is importing it. This only works because of import order. | `orchestration/` | Phase 3 |
| D14 | `SqlTransformation.run` calls `warehouse.render_query(...)` and `create_table_as_select`, and neither is defined on any class. | `orchestration/transformation.py` | `model.py`, `DuckDBWarehouse.render` |
| D15 | `io/drive_service.get_gsheets_by_prefix` puts the sheet title in a column named `race_id`, and leaves `modified_at` as a string. | `io/drive_service.py` | `sources/drive.py` |
| D16 | `SheetService` asks for read-write Drive and Sheets scopes. | `sheet_service.py` | `sources/credentials.py` |
| D17 | `models/stg_results.py` and legacy `standardize_raw_table_in_duckdb` insert sheet-derived values directly into the SQL string. | both | `transforms/results_ingest.standardize_select_sql` |
| D18 | Prep sheets are opened with `gc.open(title)`, which searches every file the service account can see and picks one if titles repeat. | `sheet_service.get_prep_table`, legacy `DataConnector` | `sources.open_prep_sheet` (FR-SRC-12) |

---

## 12. Requirement-to-code traceability

| Requirement group | Implemented in |
|---|---|
| FR-DOM-01, -02 | `models/sources.PrepAllRaces`, `models/dims.DimRaces`, `transforms/dim_races`, `transforms/results_ingest.add_results_files_to_dim_races` |
| FR-DOM-03 | `models/stg_results.StgResults`, `transforms/results_ingest`, `sources/gviz` |
| FR-DOM-04, -05 | `models/dims.DimResultsAndRegistrations`, `transforms/dim_results_registrations` |
| FR-DOM-04a…-04c (NEW) | `transforms/names`, `models/sources.PrepPeopleAndSpellings` |
| FR-DOM-04d (NEW) | `transforms/name_conflicts`, `models/qa.NameConflicts`, `dim_results.name_clean` |
| FR-DOM-06 | `transforms/ids` |
| FR-DOM-07 | `models/dims.DimAthletes`, `transforms/dim_athletes` |
| FR-DOM-08 | `models/penalties.ExtRacesByPenaltyWindow`, `transforms/penalty_windows` |
| FR-DOM-09, -10 | `models/penalties.Penalties`, `transforms/penalties` |
| FR-DOM-09a, -09b (NEW) | `transforms/penalties.fit_fixed_effects_2026`, `center_and_scale`, `calculate_penalties_2027`; `Penalties.warning_checks`, `ExtResults.warning_checks` |
| FR-DOM-11…-15, -11a (NEW) | `models/ext.*`, `models/sql/*.sql` |
| FR-SRC-01 | `sources/credentials.READONLY_SCOPES`, all of `sources/` |
| FR-SRC-02, -03 | `sources/drive.DriveSource`, `find_duplicate_titles`, `models/sources.InfraResultFiles` |
| FR-SRC-04…-06 | `transforms/results_ingest.resolve_results_sheets`, `models/dims.DimRaces` |
| FR-SRC-07 | `models/stg_results.StgResults.run`, `sources/gviz.SourceFetchError` |
| FR-SRC-08, -09, -12 | `sources/sheets.SheetsSource`, `sources.open_prep_sheet`, `DriveSource.find_sheet_by_title`, `models/sources.*` |
| FR-SRC-10 | `sources/resilient_gspread` |
| FR-SRC-11 | `ModelContext.note_source`, `run_log.RunLogWriter.record_source`, `infra_result_files` archives |
| FR-ENV-01…-11 | `settings.py` (`Settings`, `load_settings`, `ConfigError`), `config/`, `config.example/`, `warehouse/connectors._prepare` |
| FR-WH-01…-06 | `warehouse/connectors.py`, `warehouse/duckdb_warehouse.py`, `naming.py` |
| FR-RUN-01…-10, -12 | `orchestration/selector.py`, `planner.py`, `orchestrator.py`, `flow.py`, `cli.py` |
| FR-RUN-11 | `warehouse/metadata.RunLock` |
| FR-SAFE-01…-04, -09 | `orchestration/run_context.py`, `DuckDBWarehouse.promote`, `orchestrator.validate_outputs`, `naming.temp_name` |
| FR-SAFE-05…-08 | `warehouse/lifecycle.py`, `warehouse/metadata.record_publication`, `cli.cmd_archives`, `cmd_rollback`, `cmd_clean` |
| FR-OBS-01 | `DuckDBWarehouse.comment`, `Model.column_descriptions`, `Orchestrator.run_model` |
| FR-OBS-02…-04, -07 | `orchestration/run_log.py`, `orchestrator.RunResult` / `ModelResult`, `flow.run_model_task`, `cli.cmd_runs` |
| FR-OBS-05, -06 | `cli.cmd_list`, `cli.cmd_describe`, `lifecycle.list_archives`, `metadata.last_publication` |
| FR-DEV-01…-08 | `orchestration/model.py`, `registry.py`, `models/__init__.MODELS`, `transforms/` |
| FR-QA-01…-03, -06 | `tests/`, `sources/fake.FakeSources`, `pyproject.toml` pytest config |
| FR-QA-04, -05 | `cli.cmd_compare`, Phase 6 |
| NFR-01 | `StgResults` thread pool, `DuckDBWarehouse` single connection with cursors |
| NFR-02 | `pyproject.toml`, `uv.lock` |
| NFR-03 | `naming.quote_ident`, `sanitize_identifier`, `standardize_select_sql`, read-only scopes, `Settings.masked_dict` |
| NFR-04 | `__init__.py` changes, `flow.py` with no import-time work, `models/__init__` holding plain class instances |
| NFR-05 | `settings.CONFIG_DIR` and `log_dir` overrides; no absolute paths |
| NFR-06 | The removals in §10; `ruff` |

---

## 13. Build order

Each phase leaves the repository importable, with passing tests. Phases 1–3 need no Google access. The legacy rules are ported and proven equal to v1 (Phases 5–6) **before** any NEW rule is added (Phase 7), so differences found in Phase 6 can only come from the port (FR-QA-04).

| Phase | Work | Files | Done when |
|---|---|---|---|
| **0: Make it import** (½ day) | Temporary fixes for D1–D3 so the current branch imports again (alias `PipelineModel = WarehouseOperation`, remove the `make_table_ref` call). Fix D8 and D9. Add a smoke test that imports every module. | `models/*`, `pyproject.toml`, `tests/test_imports.py` | `uv run pytest` passes |
| **1: Config and connectors** (1 day) | `Settings` / `load_settings` with every FR-ENV-11 setting, update the env files, `config.example/`, move and fix the connectors (D7, D10). Delete `utils.py`. | `settings.py`, `config*/`, `warehouse/connectors.py` | `test_settings.py` passes; local connect works; manual MotherDuck connect to the project database works |
| **2: Warehouse** (2 days) | `naming.py`, `refs.py`, `duckdb_warehouse.py`, `lifecycle.py`, `metadata.py`. **Start with the R1 spike.** Delete `io/warehouse_*`, `io/table_version_ref.py`, `io/utils.py`, `io/duckdb_service.py`, `warehouse/warehouse_backend.py`. | `naming.py`, `warehouse/*` | `test_naming`, `test_warehouse`, `test_metadata`, `test_lifecycle` pass, including the rollback test with an injected failure |
| **3: Orchestration** (2–3 days) | `model.py`, `registry.py`, `selector.py`, `planner.py`, `run_context.py`, `run_log.py`, `orchestrator.py`. Fixes D13, D14. | `orchestration/*` | `test_selector`, `test_registry_planner`, `test_render`, `test_run_log`, `test_orchestrator` pass on a toy graph that includes a multi-output model |
| **4: Sources** (1–2 days; can run alongside 2–3) | `sources/*`, `FakeSources`. Answer Q4. Delete `io/drive_service.py` and `io/google_sheets_warehouse_manager/`. Fixes D15, D16, D18. | `sources/*` | The live test lists result sheets in the folder and reads every configured prep sheet |
| **5: Port the models** (4–6 days) | In graph order: source models → `DimRaces` → `StgResults` → `DimResultsAndRegistrations` → `DimAthletes` → `ExtRacesByPenaltyWindow` → `Penalties` → SQL models, each with its `transforms/` module and fixture test, **with v1 logic only**. Then `flow.py` and `cli.py`. Delete the old `models/` and `orchestration/` files. Fixes D12, D17. | `transforms/*`, `models/*`, `flow.py`, `cli.py` | `mrl run --env dev` succeeds on a local file; the offline e2e test passes in CI |
| **6: Check against v1** (2–4 days plus fixes) | Freeze the inputs (record Drive `modifiedTime`s), run v1 once, run v2 in `stg`, and `mrl compare` every table. Fix or accept each difference. Save the v2 outputs as golden fixtures. | `cli.cmd_compare`, `tests/fixtures/golden/` | Every table matches or each difference is explicitly accepted (FR-QA-04) |
| **7: NEW rules** (4–6 days) | Resolve Q4 and Q7 first. (a) Name matching: `transforms/names.py`. Add empty `race_season` and `disambiguation` columns to the name tab; existing rows then apply to every season unchanged. Update `build_dim_results` (including `name_clean`) and `build_dim_registrations`. (b) `calc_athlete_id` with disambiguation. (c) `dim_athletes` at `athlete_id` grain. (d) `_27` penalties (`fit_fixed_effects_2026`, `center_and_scale`, `calculate_penalties_2027`). (e) `_27` in the three SQL models. (f) `NameConflicts` and `transforms/name_conflicts.py`; review the first report with the data maintainer. | `transforms/*`, `models/dims.py`, `models/qa.py`, `models/sql/*` | The NEW tests pass; `_25`/`_26` columns still equal the Phase 6 golden output except where explained by the name and ID changes |
| **8: Cutover** (1 day) | Run prod on MotherDuck; archive the v1 Sheets warehouse folders (manual Drive task, R4 in the spec); delete `legacy/` and `cmd_compare`; update `README.md`. | — | A scheduled prod run succeeds; no Sheets-warehouse code remains |

---

## 14. Risks

| # | Risk | Mitigation |
|---|---|---|
| R1 | Renaming tables inside a transaction is restricted, or behaves differently on MotherDuck than locally. | Spike at the start of Phase 2. Fallback: build in a staging schema `"{schema}__staging_{run_id}"`, then swap tables in a short transaction. |
| R2 | v1 results depended on Sheets converting every value to text. | `stg_*` and `prep_*` tables are read as text (`all_varchar`, `get_all_records` then string conversion). Type conversion happens only in `transforms/`. |
| R3 | The two current versions of `stg_results` produce different rows. | The legacy version is the reference (§7, `StgResults`). Fixture-test it before combining. |
| R4 | A local DuckDB file allows only one writing process, so Prefect and an interactive session can block each other. | `cli` catches the lock error and explains it; use MotherDuck for shared work. |
| R5 | Threads loading `httpfs` at the same time fail with "already registered". | Port the legacy `load_httpfs_serialized` approach into `connectors._try_load_httpfs`, and load it once per connection before starting threads. |
| R6 | The gviz CSV endpoint depends on result sheets being shared by link. | Document it. A fallback is `SheetsSource.read_worksheet`, which is slower but uses authenticated access. |
| R7 | The `duckdb` pin falls behind what MotherDuck supports. | Check in Phase 1; upgrade on purpose, using the Phase 6 comparison as a check. |
| R8 | A field wider than historical ranges (slowest more than about 2× the fastest) pushes `_27` penalties or points to zero or below. | FR-DOM-09b warnings name the rows; the operator decides whether to exclude the race. |
| R9 | The new `athlete_id` formula and name matching change keys, so v2 can't be compared with v1 after Phase 7. | Phase 6 runs before Phase 7, and its outputs are kept as golden fixtures. |
| R10 | Run logs written inside a Prefect worker container disappear with the container. | Mount `log_dir` on a persistent volume for prod workers; Prefect keeps the console log too. |
| R11 | The first name-conflict report may be long, because conflicts from past seasons have never been checked. | It never blocks publishing. Resolved conflicts stay visible with `is_resolved = true`, so the list can be worked through over time. |
| R12 | The Eastern Cup age-group and league rules depend on how `series` and `division` are coded in the race sheet (spec Q7). | Codes are settings, not literals; unmapped divisions are counted in a warning, so a coding change shows up instead of silently disabling rule (c). |
