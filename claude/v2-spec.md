# MRL Pipeline v2: Functional Specification

*Status: draft 3, 2026-11-17. Inferred from the `feat/legacy` branch (`93cc07b` plus uncommitted work), revised with the owner's review.*
*Companion document: [v2-project-structure.md](v2-project-structure.md) maps every requirement here to the files and classes that implement it.*

This document lists **what v2 must do for the people who use it**. It doesn't describe how the code is organized; that is in the companion document. Every requirement has an ID (`FR-…` or `NFR-…`). The project-structure document refers to these IDs, so each requirement can be traced to code.

Each requirement's **Source** says where it came from:

- **legacy**: behavior the existing scripts in `src/mrl_pipeline/legacy/` already have, which v2 must keep.
- **v2-draft**: intent visible in the unfinished v2 code (`io/`, `warehouse/`, `orchestration/`, `models/`).
- **owner**: required or confirmed by the project owner in review.
- **decision**: a choice this spec makes where nothing else settles it. These can be changed.

**New behavior** (not in v1) is marked **NEW**. Everything not marked NEW must reproduce v1 exactly.

---

## Contents

1. [Purpose and scope](#1-purpose-and-scope)
2. [Users](#2-users)
3. [Glossary](#3-glossary)
4. [Ranking outputs](#4-ranking-outputs-fr-dom)
5. [Ingesting sources](#5-ingesting-sources-fr-src)
6. [Environments and configuration](#6-environments-and-configuration-fr-env)
7. [The warehouse](#7-the-warehouse-fr-wh)
8. [Running the pipeline](#8-running-the-pipeline-fr-run)
9. [Safe publishing, rollback and archives](#9-safe-publishing-rollback-and-archives-fr-safe)
10. [Documentation and observability](#10-documentation-and-observability-fr-obs)
11. [Extending the pipeline](#11-extending-the-pipeline-fr-dev)
12. [Quality assurance](#12-quality-assurance-fr-qa)
13. [Non-functional requirements](#13-non-functional-requirements-nfr)
14. [Non-goals](#14-non-goals)
15. [Resolved questions](#15-resolved-questions)
16. [Open questions](#16-open-questions)

---

## 1. Purpose and scope

The Mass Ranking List (MRL) pipeline turns race results from Massachusetts points-eligible Nordic ski races into athlete penalties, race points, season point totals and qualifying places for the U16 and Eastern High School championships.

v2 replaces the infrastructure around the ranking logic and makes a small number of deliberate ranking changes:

- **Warehouse:** Google Sheets is replaced by **DuckDB**, as a local file or on **MotherDuck**. Google Sheets is **no longer a warehouse backend in any form**. Google Drive and Sheets stay only as **read-only sources** of hand-maintained inputs.
- **Execution:** handwritten scripts are replaced by a **declared graph of models**, run by Prefect or a CLI, that can rebuild any part of the pipeline.
- **Safety:** non-atomic Sheet renames are replaced by **all-or-nothing runs** with archives and rollback.
- **Configuration:** credentials passed as arguments are replaced by **environment profiles** (`dev`, `stg`, `prod`), secret files, and named settings for every input.
- **Ranking changes (NEW):** season-aware athlete name matching with disambiguation, a report of likely name conflicts, `dim_athletes` covering every athlete, and a third penalty and points method (`_27`, on a base-1.001 log scale centered on 500) alongside `_25` and `_26`.

Downstream consumers of v1 outputs are out of scope for this rewrite: v2 may change table shapes and IDs without a compatibility layer (resolved question R1).

---

## 2. Users

| User | Who | What they need |
|---|---|---|
| **Operator** | The person who runs the pipeline during the season (today, the project maintainer) | Run everything or part of it in the right environment, see what happened, recover from bad runs. |
| **Developer** | Someone changing ranking logic or adding a table | Add or change one model without touching anything else; test locally with no Google access. |
| **Data consumer** | Ranking committee, coaches, anyone querying results | Correct, documented, consistent tables in one place, never half-updated. |
| **Data maintainer** | Whoever edits the prep sheets and race result sheets in Google Drive | Clear warnings when a sheet is missing, duplicated or malformed; a way to tell apart athletes who share a name. |

---

## 3. Glossary

| Term | Meaning |
|---|---|
| **Environment** | `dev`, `stg` or `prod`. An isolated copy of every warehouse table, stored as its own schema. |
| **Table** | A named dataset in the warehouse, e.g. `dim_results`. Users always refer to tables by this name. |
| **Model** | One unit of pipeline work. It reads named tables (or Google sources) and writes one or more named tables. The pipeline is a graph of models. |
| **Run** | One execution of all or part of the pipeline in one environment. |
| **Publish / promote** | Making a run's newly built tables the current versions. Either all of them are published or none are. |
| **Archive** | A previous version of a table, kept after a newer one is published. |
| **Prep sheet** | A hand-maintained Google Sheet of reference data (race schedule, registrations, name and school spellings, penalty configuration). |
| **Results sheet** | A Google Sheet whose title starts with the results prefix (default `prep_results`), holding one race's results for one gender. |
| **Canonical name** | The standard spelling of an athlete's name (`athlete_name`). |
| **Disambiguation string** | A short, maintainer-entered value (e.g. a town or school) that tells apart different athletes who share a canonical name and gender. Empty for almost everyone. **NEW** |
| **Cleaned name** (`name_clean`) | A results row's name after whitespace, asterisk and title-case cleanup, before the name table is applied. This is the "raw name" the conflict checks look at. **NEW** |
| **Name conflict** | Evidence that one cleaned name, or one `athlete_id`, covers two different people, e.g. it appears twice on one results sheet. **NEW** |
| **Exclusive leagues** | High-school leagues an athlete can belong to only one of in a season: by default `BERK`, `MBE` and `MBW`. |
| **Vintage `_25` / `_26` / `_27`** | The 2025, 2026 and 2027 penalty and points methods. All three are calculated side by side. `_27` is **NEW**. |
| **MAQ** | MA Nordic Cup qualifier series. It is treated differently when counting a racer's best results. |
| **EHS / U16** | Eastern High School and U16 championship eligibility. |

---

## 4. Ranking outputs (FR-DOM)

The legacy code is the reference for everything not marked NEW, and v2 must not change those formulas. The descriptions summarize what the code does so outputs can be checked.

### 4.1 Races and staging

| ID | Requirement | Source |
|---|---|---|
| **FR-DOM-01** | **Race schedule.** Build `dim_races` from every worksheet of the races prep sheet (setting `races_sheet`, default `prep_all_races`), one worksheet per season. Each race has an ID, date, season, name, series, technique, discipline, format, division, venue, state, PDF result links, and the titles of its boys' and girls' results sheets. It also carries each results sheet's Drive file ID and URL (`prep_results_url_b`, `prep_results_url_g`), for lineage. | legacy `build_dim_races`, `add_results_urls_to_dim_races`; owner (URLs stay on `dim_races`) |
| **FR-DOM-02** | **Race sampling.** When the `sample_races` setting is true, `dim_races` keeps one randomly chosen race per (season, series), so test runs are fast. The setting defaults to false and is **independent of the environment**: no code checks the environment name to decide. An optional `sample_seed` makes the sample repeatable. | legacy `build_dim_races` (`if warehouse_env == "dev"`); owner (separate setting) |
| **FR-DOM-03** | **Raw results staging.** `stg_results` holds one row per line of every results sheet, with these columns kept as text: `name_raw`, `time_raw`, `usss_id`, `fis_id`, `school_raw`, `team_raw`, `state_raw`, plus `race_id`, `gender`, `source_file_id`, `prep_results_url`, `created_at`. Columns are matched by known header variants (e.g. `Name` or `Skier Name`, `Race Time` or `Elapsed`, `USSS#` or `Member #`). A column that can't be matched is null. | legacy `STANDARD_COL_MAP`, `standardize_raw_table_in_duckdb`; `models/stg_results.py` |

### 4.2 Results, registrations and athletes

| ID | Requirement | Source |
|---|---|---|
| **FR-DOM-04** | **Result cleaning.** `dim_results` cleans athlete names: removes non-breaking spaces, extra whitespace and asterisks, and converts to title case except for Eastern Cup results. The result is kept as `name_clean` (**NEW** column, used by FR-DOM-04d). It then resolves each cleaned name to a canonical name and disambiguation string (FR-DOM-04a), maps known non-person competitor names to `Unidentified Athlete`, applies school spelling fixes, parses times to minutes (`time_float`), and assigns finish places, leaving out non-finishers. | legacy `build_dim_results`; owner (name matching) |
| **FR-DOM-04a** | **Season-aware name matching. NEW.** The name-spelling table maps **(`name_raw`, `race_season`) → (`athlete_name`, `disambiguation`)**. It is many-to-one: several raw spellings in a season can map to one athlete. The same `name_raw` may map to a different athlete in a different season. A row with a **blank `race_season` applies to every season**; a row for a specific season takes precedence over the blank-season row for the same `name_raw`. So existing rows carry over unchanged, and a season-specific row is added only when a conflict comes up (FR-DOM-04d). A cleaned name with no matching row maps to itself with an empty disambiguation. The same lookup is applied to registrations, using the registration's season (FR-DOM-05). | owner |
| **FR-DOM-04b** | **Disambiguation values. NEW.** `disambiguation` is an empty string unless two or more different athletes share a canonical name and gender, in which case the maintainer enters a different value for each of them (any text, e.g. `Concord` or `2011`). The pipeline never invents values. `dim_results`, `dim_registrations` and `dim_athletes` carry the `disambiguation` column. | owner |
| **FR-DOM-04c** | **Name-table checks. NEW.** Loading the name table fails the run if a (`name_raw`, `race_season`) key appears twice with different targets (a blank season counts as its own key), or if a non-blank `race_season` is not a whole number. It warns when a canonical name and gender is used both with and without a disambiguation in the same season, because that usually means one row was missed. | decision; owner (blank season allowed) |
| **FR-DOM-04d** | **Name-conflict report. NEW.** `qa_name_conflicts` lists results rows that suggest one name covers two different people, so the maintainer knows where to add a disambiguation. All rules are applied within one gender and leave out `Unidentified Athlete` rows. The rules: <br>**(a) Same sheet:** a name appears more than once on one results sheet (same `race_id` and gender). This is either a data error or two people with the same name. <br>**(b) Exclusive leagues:** within one season, a name appears in races from more than one of the mutually exclusive high-school leagues (setting `exclusive_league_series`, default `BERK`, `MBE`, `MBW`). <br>**(c) U16 vs U18/U20:** within one season, a name appears in both the **U16 set** (any U16 Eastern Cup race or any U16 Championships race) and the **U18/U20 set** (any U18/U20 Eastern Cup race). The two sets of racers never overlap, so this is always two different people. The check is per season because an athlete moves from U16 to U18 between seasons. Which `division` values count as U16 or U18/U20 is set by `ec_age_groups`, and which `series` is the U16 Championships by `u16_champs_series` (Q7). <br>Each rule is checked twice: by `name_clean` (the raw name) and by `athlete_id` (the resolved identity). A conflict found by `name_clean` whose rows already map to different `athlete_id`s is listed with `is_resolved = true`, as a record of past fixes. A conflict found by `athlete_id` is always `is_resolved = false`: either the name hasn't been disambiguated yet, or a spelling row has merged two people. Each row has the rule, key type, season, gender, `name_clean`, `athlete_id`, `athlete_name`, `disambiguation`, `result_id`, `race_id`, race name, date, series, division, a `conflict_group` ID shared by the rows of one conflict, and `is_resolved`. Unresolved conflicts are logged as warnings with a count per rule. They never block publishing. | owner |
| **FR-DOM-05** | **Registrations.** `dim_registrations` keeps registrations in the eligible categories (U16/EHS Qualifier and Late U16/EHS Mass Nordic Cup, boys and girls, seeded or unseeded). It flags exact-match `GHOST` registrations, normalizes clubs, gender (`m`/`f` → `b`/`g`), NENSA and USSS numbers, school (including "Other"), and grade (from numbers or words like "sophomore"), and sets EHS and U16 eligibility. Names go through FR-DOM-04a. | legacy `build_dim_registrations` |
| **FR-DOM-06** | **Identifiers.** `athlete_id` is the first 12 hex characters of SHA-256 of `gender,athlete_name,disambiguation` (**NEW**: disambiguation is added to the hash, so v2 IDs differ from v1 IDs). `result_id` is the same hash over `athlete_id,race_id`; `registration_id` over `athlete_id,race_season`. IDs are stable from run to run for the same inputs. | legacy `calc_*_id`; owner |
| **FR-DOM-07** | **Athletes. NEW grain.** `dim_athletes` has exactly one row per `athlete_id` that appears in `dim_results` or in a non-ghost `dim_registrations` row. Identified-athlete rows only: `Unidentified Athlete` results are left out. Each row has `athlete_name`, `disambiguation` and `gender`. Registration attributes (school, club, town, state, NENSA and USSS numbers, birth year, graduation year = season + 12 − grade) come from the athlete's most recent non-ghost registration and are null for athletes who never registered. It also has a `was_registered_eligible_<season>` flag for each season, false when not registered. | legacy `build_dim_athletes`; owner |

### 4.3 Penalties

| ID | Requirement | Source |
|---|---|---|
| **FR-DOM-08** | **Penalty windows.** `ext_races_by_penalty_window` lists, for each penalty-list publication date, the races that count toward it, based on each series' contribution window. It warns when a series has no window defined, and when a race is more than 3 months newer than the latest publication date. | legacy `build_ext_races_by_penalty_window` |
| **FR-DOM-09** | **Athlete penalties, 2025 and 2026.** `ext_athlete_penalties` has one row per (season, publication date, gender, athlete). The 2025 penalty is a regression of log time relative to the winner. The 2026 penalty is a fixed-effects model on log time (race intercept + athlete effect), with slow outlier races removed and a refit. For both, the raw penalty is `exp(coefficient)` scaled so the interquartile mean is 200, and the standardized penalty is scaled again so the cohort from the season's first publication date has an interquartile mean of 200. | legacy `build_penalties` |
| **FR-DOM-09a** | **Athlete penalties, 2027. NEW.** `_27` takes the 2026 calculation's fitted log-time coefficients (same model, same outlier refit) and, instead of exponentiating them, **mean-centers and scales** them as if they were logarithms in **base 1.001**, so one unit is a 0.1% time difference. With `k = 1 / ln(1.001) ≈ 1000.5`: <br>• `athlete_penalty_27 = 500 + k × (coefficient − interquartile mean of the coefficients)`, within each (season, publication date, gender) fit. <br>• `standardized_athlete_penalty_27 = athlete_penalty_27 + (500 − interquartile mean of the first-date cohort's athlete_penalty_27 on that date)`. <br>The 2027 target mean is **500** (2025 and 2026 stay at 200). The interquartile range is the 25th–75th percentiles, as for the other vintages. | owner |
| **FR-DOM-09b** | **2027 values stay positive. NEW.** `_27` penalties and points can in theory go negative, but won't while the slowest skiers take less than 2× as long as the fastest (log₁.₀₀₁ 2 ≈ 693, so the field spans about ±347 around 500). A run logs a warning, naming the rows, if any `standardized_athlete_penalty_27` or `race_points_27` is zero or below. It does not block publishing. | owner; decision (warn only) |
| **FR-DOM-10** | **Penalty diagnostics.** `ext_results_by_penalty_window` has one row per result per window, with the 2026 model's outlier flags and residuals, for checking and debugging. These diagnostics also apply to 2027 because it shares the fit. | legacy `build_penalties` |

### 4.4 Points and qualifying

| ID | Requirement | Source |
|---|---|---|
| **FR-DOM-11** | **Race penalties and points, 2025 and 2026.** `ext_results` has one row per result. Each result uses the most recent penalty list on or before the race date. The race penalty is the geometric mean of the top penalized finishers' standardized penalties (2025: places 1–5; 2026: places 1–6) and needs at least 3 penalized finishers; the race-penalty time is the geometric mean of those finishers' times. Race points = race penalty × (time / race-penalty time) ^ compression. Compression is 1.0 for 2025; for 2026 it comes from each series' DI or SP factor. Points are rounded to 2 decimals. Ranks are given per athlete-season and per athlete-season-technique. | legacy `build_ext_results.sql` |
| **FR-DOM-11a** | **Race penalties and points, 2027. NEW.** `race_points_27 = race_penalty_27 + compression_27 × log₁.₀₀₁(time / race_penalty_time_27)`, where log₁.₀₀₁(x) = ln(x) / ln(1.001), the same units as the 2027 penalties. `race_penalty_27` is the **arithmetic** mean of the top-6 penalized finishers' `standardized_athlete_penalty_27` (the log-scale equivalent of the 2026 geometric mean), with at least 3 penalized finishers required. `race_penalty_time_27` is the geometric mean of those finishers' times. `compression_27` is the same per-series DI/SP factor as 2026. Points are rounded to 2 decimals, and ranks are given as for the other vintages. | owner |
| **FR-DOM-12** | **Reason for no points.** For seasons 2026 and later, every result that doesn't earn points gets one reason: race not scored, no finish time, racer not eligible, race unscorable, or other (`reason_for_no_points`, based on `_26`). **NEW:** `reason_for_no_points_27` and `ma_points_2027_str` do the same for seasons 2027 and later, based on `_27`. | legacy `build_ext_results.sql`; decision (`_27` columns) |
| **FR-DOM-13** | **Points-contributing results.** `ext_points_contributing_results` picks, for each registration and technique, up to three best non-MAQ races that beat the racer's MAQ points (if they have MAQ points), plus the MAQ race, separately for each vintage. Each slot has a description. | legacy `build_ext_points_contributing_results.sql` |
| **FR-DOM-14** | **Season points and qualifying places.** `ext_registrations` adds weighted SK and CL technique points, total points, MAQ-finisher flags, and EHS and U16 qualifying places (ranked by total points within season, gender and eligibility), for each vintage. | legacy `build_ext_registrations.sql` |
| **FR-DOM-15** | **Three vintages side by side.** Every penalty and points output carries `_25`, `_26` and `_27` columns in parallel, computed with the same slot, rank and comparison logic. No output drops a vintage. `_25` and `_26` results are unchanged by adding `_27`. | legacy; owner |

---

## 5. Ingesting sources (FR-SRC)

| ID | Requirement | Source |
|---|---|---|
| **FR-SRC-01** | **Read-only Google access.** The pipeline reads Google Drive and Sheets with a service account using **read-only** scopes. It never creates, renames, edits or deletes a Drive file. | decision (scope change) |
| **FR-SRC-02** | **Finding results sheets.** The pipeline finds every spreadsheet whose title starts with the results prefix (setting `results_sheet_prefix`, default `prep_results`) **inside the configured Drive folder and its subfolders only** (setting `google_drive_source_folder_id`, required for any run that reads sources). For each one it records the file ID, title and last-modified time. | `io/drive_service.py`, legacy `DriveService`; owner (folder only) |
| **FR-SRC-03** | **Duplicate titles stop the run.** If two Drive files share a results-sheet title, ingestion fails and lists the duplicates. | legacy `validate_drive_lookup_against_races` |
| **FR-SRC-04** | **Missing sheets are warnings.** If a race in `dim_races` names a results sheet that isn't in the folder, it is logged as a warning (race, gender, title) and skipped. | legacy `test_one_to_one_race_gender_to_sheet` |
| **FR-SRC-05** | **Orphan sheets are warnings.** Results sheets in the folder that no race refers to are logged as warnings, with a count and a sample of titles. Orphans are judged against the full race list, before sampling (FR-DOM-02). | legacy `validate_drive_lookup_against_races`; decision (before sampling) |
| **FR-SRC-06** | **One sheet per race and gender.** If a (race, gender) pair resolves to more than one sheet, ingestion fails. | legacy `test_one_to_one_race_gender_to_sheet` |
| **FR-SRC-07** | **A single bad sheet doesn't stop the run.** A results sheet that can't be fetched or parsed is logged with race, gender, title and error, then skipped. The run ends with a count of failures. | legacy `process_one_sheet_into_duckdb` |
| **FR-SRC-08** | **Prep sheets.** Each prep sheet is found by its configured title (FR-ENV-11) and copied into warehouse tables, kept as text: <br>• races sheet → `prep_all_races` (all worksheets, `race_season` from the worksheet title) <br>• people sheet (default `prep_people_and_spellings`, several tabs) → `prep_names_equivalent_spellings` (from its name-spelling tab) and `prep_non_people` (from its non-person tab) <br>• schools sheet (default `prep_schools`) → `prep_schools_equivalent_spellings` <br>• penalty sheets → `prep_penalty_publication_dates`, `prep_penalty_contribution_windows`, `prep_penalty_contributions` | legacy function inputs; owner (sheet names) |
| **FR-SRC-09** | **Registrations.** `stg_registrations` is read through the Google Sheets API from the registrations sheet (setting `registrations_sheet`, default `prep_qualifier_registrations`), which has **one tab per race season**. Every tab is read, `race_season` is taken from the tab title, and all columns are kept as text. A tab whose title isn't a season year fails the run. | owner |
| **FR-SRC-10** | **Handling Google errors.** Temporary Google API errors (HTTP 429, 500, 503) are retried with exponential backoff before failing. | `resilient_gspread_client.py` |
| **FR-SRC-11** | **Record of sources read.** For each run, the Drive file IDs, titles and modified times that were read can be found afterwards: in that run's log file (FR-OBS-02), and in the published `infra_result_files` table and its archives. | decision |
| **FR-SRC-12** | **Prep sheets are found unambiguously.** A prep sheet title that matches no file, or more than one file, in the configured folder fails the run and names the title. | decision |

---

## 6. Environments and configuration (FR-ENV)

| ID | Requirement | Source |
|---|---|---|
| **FR-ENV-01** | **Three environments.** `dev`, `stg` and `prod` exist, and every run targets exactly one. | `config/*.env`, `settings.py` |
| **FR-ENV-02** | **Choosing the environment.** The operator picks the environment with one value: `--env` on the CLI, the `env` parameter of the Prefect flow, or the `MRL_ENV` process variable. The default is `dev`. | decision (`settings.py` doesn't load env files today) |
| **FR-ENV-03** | **Environments are isolated.** Each environment is its own schema in the project database. A run in one environment can't read or write another environment's tables. | v2-draft; owner |
| **FR-ENV-04** | **Same table names everywhere.** A table has the same name in every environment (`dim_results`, not `dim_results_dev`), so queries and SQL work unchanged across environments. | decision |
| **FR-ENV-05** | **Per-environment profiles.** Each environment's non-secret settings live in one file (`config/<env>.env`). Process variables prefixed `MRL_` override them. | `settings.py`, `config/*.env` |
| **FR-ENV-06** | **Secrets in files.** The MotherDuck token and the Google service-account key are read from files in `config/secrets/`, never from command-line arguments or committed files. | `settings.py` (`secrets_dir`) |
| **FR-ENV-07** | **Behavior differences are explicit.** Anything that can behave differently between runs (e.g. race sampling, FR-DOM-02) is a named setting, never a hidden check of the environment name. | owner |
| **FR-ENV-08** | **Mistakes caught at startup.** If the requested environment doesn't match the loaded profile, a required secret is missing for the chosen target, or a run that reads sources has no Drive folder configured, the run fails before doing any work and says why. | decision |
| **FR-ENV-09** | **Settings logged.** Each run logs its resolved settings once, with secrets masked. | decision |
| **FR-ENV-10** | **Template config.** A committed example config directory documents every setting and every secret file name. | decision |
| **FR-ENV-11** | **Every input is configurable. NEW.** Every input location is a named setting with a default, so no sheet title or folder is written into code: | owner |

FR-ENV-11 settings:

| Setting (`MRL_…`) | Default | Meaning |
|---|---|---|
| `PROJECT_NAME` | `mrl` | Database name: the MotherDuck database, or the local file `db/<project_name>.duckdb`. |
| `GOOGLE_DRIVE_SOURCE_FOLDER_ID` | *(none; required to read sources)* | The Drive folder searched for results and prep sheets. |
| `RESULTS_SHEET_PREFIX` | `prep_results` | Title prefix of results sheets. |
| `RACES_SHEET` | `prep_all_races` | Race schedule, one tab per season. |
| `REGISTRATIONS_SHEET` | `prep_qualifier_registrations` | Registrations, one tab per season. |
| `PEOPLE_SHEET` | `prep_people_and_spellings` | Name spellings and non-person names. |
| `PEOPLE_NAME_SPELLINGS_TAB` | *(Q4)* | Tab of the people sheet holding the name table. |
| `PEOPLE_NON_PERSONS_TAB` | *(Q4)* | Tab of the people sheet holding non-person names. |
| `SCHOOLS_SHEET` | `prep_schools` | School spellings. |
| `PENALTY_PUBLICATION_DATES_SHEET` | `prep_penalty_publication_dates` | Penalty list dates. |
| `PENALTY_CONTRIBUTION_WINDOWS_SHEET` | `prep_penalty_contribution_windows` | Series contribution windows. |
| `PENALTY_CONTRIBUTIONS_SHEET` | `prep_penalty_contributions` | Series scoring and compression factors. |
| `EXCLUSIVE_LEAGUE_SERIES` | `["BERK","MBE","MBW"]` | FR-DOM-04d rule (b). |
| `EC_SERIES` | `EC` | The `series` value for Eastern Cup races (the code already uses `"EC"`). |
| `EC_AGE_GROUPS` | *(Q7)* | Maps Eastern Cup `division` values to `U16` or `U18/U20`, for FR-DOM-04d rule (c). |
| `U16_CHAMPS_SERIES` | *(Q7)* | The `series` value for U16 Championships races; every such race is in the U16 set, whatever its division. |
| `SAMPLE_RACES` | `false` | FR-DOM-02. |
| `SAMPLE_SEED` | *(none)* | FR-DOM-02. |
| `LOG_DIR` | `logs/` | FR-OBS-02. |

---

## 7. The warehouse (FR-WH)

| ID | Requirement | Source |
|---|---|---|
| **FR-WH-01** | **DuckDB only.** Every warehouse table is stored in DuckDB. No warehouse table is written to or read from Google Sheets. | decision (scope change) |
| **FR-WH-02** | **Local or MotherDuck.** The operator can use a local DuckDB file or a MotherDuck database. There is **one database per project** (named by `project_name`) and **one schema per environment**. If a MotherDuck token is configured, MotherDuck is the default; otherwise local. An explicit setting overrides either default. | `database_connectors.py`; owner (database and schema layout) |
| **FR-WH-03** | **Same results on both.** For the same inputs and settings, local and MotherDuck runs produce the same tables. | decision |
| **FR-WH-04** | **Queryable with any DuckDB client.** Data consumers can query current tables with the DuckDB CLI, the MotherDuck UI, or any DuckDB client using `<env>.<table>`. | v2-draft |
| **FR-WH-05** | **Types kept.** Tables keep proper column types (dates, integers, floats, booleans). Raw `stg_*` and `prep_*` tables keep source values as text. | decision; legacy (`all_varchar` reads) |
| **FR-WH-06** | **Temporary tables hidden by name.** Temporary, archive and pipeline-bookkeeping tables use reserved name prefixes, so they're easy to filter out of table listings. | v2-draft (`TableVersionRef`) |

---

## 8. Running the pipeline (FR-RUN)

| ID | Requirement | Source |
|---|---|---|
| **FR-RUN-01** | **Full run.** One command builds every table in dependency order. | `orchestration/flows.py` |
| **FR-RUN-02** | **Selective run.** The operator can rebuild a subset using selectors: a model or table name; `+name` (with everything upstream); `name+` (with everything downstream); `tag:<tag>`; space-separated for union; comma-separated for intersection. Naming a table selects the model that produces it. | `orchestration/model_selector.py`; decision (tags) |
| **FR-RUN-03** | **Exclusions.** `--exclude <selector>` removes models from the selection. | decision |
| **FR-RUN-04** | **Unselected inputs.** In a selective run, inputs that aren't being rebuilt are read from their current published versions. Inputs rebuilt earlier in the same run are read from that run's new versions. | v2-draft (`RunContext.resolve_input`) |
| **FR-RUN-05** | **Plan before running.** The operator can see the ordered list of models a selection would run, and where each input would be read from, without running anything. | v2-draft (`Orchestrator.plan`) |
| **FR-RUN-06** | **Missing inputs caught early.** If a selective run needs a published table that doesn't exist yet, the run fails before any model runs and names the missing tables. | decision |
| **FR-RUN-07** | **Fixed order.** Running the same selection twice gives the same model order. | `model_selector.py` (`lexicographical_topological_sort`) |
| **FR-RUN-08** | **Prefect.** The pipeline runs as a Prefect flow, with one Prefect task per model, so it can be scheduled and monitored there. | `orchestration/flows.py` |
| **FR-RUN-09** | **CLI.** The pipeline can also be run from an `mrl` command, with or without Prefect. | owner |
| **FR-RUN-10** | **Building without publishing.** The operator can run models and leave the results unpublished (`--no-promote`) to inspect them first. | owner |
| **FR-RUN-11** | **One run at a time.** Starting a second run in an environment that already has a run in progress is refused, unless forced. This holds across machines when using MotherDuck. | owner |
| **FR-RUN-12** | **Failure stops downstream models.** If a model fails, the models that depend on it are skipped. Independent branches still run, but nothing is published (FR-SAFE-02). | owner |

---

## 9. Safe publishing, rollback and archives (FR-SAFE)

Archive management is in scope for this phase (resolved question R5).

| ID | Requirement | Source |
|---|---|---|
| **FR-SAFE-01** | **All or nothing.** A run publishes every table it built, or none. Consumers never see a mix of old and new tables from one run. | v2-draft (`DuckDBWarehouseService.promote_temp_tables` transaction) |
| **FR-SAFE-02** | **A failed run changes nothing.** If any model fails, no table is published, and every published table stays as it was. | v2-draft |
| **FR-SAFE-03** | **Checks before publishing.** Before publishing, each table built in the run must exist, contain every documented column, and have at least one row unless its model marks it as allowed to be empty. Models can define extra checks, such as unique keys. Any failure blocks publishing. | owner |
| **FR-SAFE-04** | **Archives.** Publishing a table keeps its previous version as an archive tagged with the run's timestamp. Every table from one run shares that timestamp. | `TableVersionRef` (`archive`) |
| **FR-SAFE-05** | **No automatic archive deletion.** Archives are kept until the operator removes them. The operator can list a table's archives, drop one archive, or prune each table down to its newest N archives, each with one command. Nothing deletes archives during a run. | owner |
| **FR-SAFE-06** | **Rolling back one table.** The operator can restore one table to its previous archive, or to a specific run's archive, in one command. The version being replaced is itself archived. | decision |
| **FR-SAFE-07** | **Rolling back a run.** The operator can undo everything one run published, in one command, as a single transaction. Tables the run created for the first time are dropped. | owner |
| **FR-SAFE-08** | **Leftovers kept for debugging.** Temporary tables from a failed run are left in place so they can be inspected, and one command removes leftovers from runs that are no longer active. | owner |
| **FR-SAFE-09** | **Runs don't collide.** Temporary tables from different runs never share names. | owner |

---

## 10. Documentation and observability (FR-OBS)

| ID | Requirement | Source |
|---|---|---|
| **FR-OBS-01** | **Table and column descriptions in the database.** Each published table carries its description and its columns' descriptions as database comments, viewable in any DuckDB or MotherDuck client. This replaces the schema tab that v1 added to every Sheet. | owner; `models/*` (`column_descriptions`) |
| **FR-OBS-02** | **Run log file.** Every run writes a **local log file per run** under `<log_dir>/<env>/<run_id>/`. It records the run ID, environment, start and end times, status, selection, whether it published, code version, target (local or MotherDuck), masked settings, the sources read (FR-SRC-11) and any error. The file is written even when the run fails. | owner |
| **FR-OBS-03** | **Model history in the run log.** The same per-run log records, for every model, its output tables, row and column counts, timing, status, and any error or warning summary. | owner |
| **FR-OBS-04** | **Structured logs.** Each model writes one summary line (`model=… status=… rows=… cols=… secs=…`) to the run log and the console (or Prefect). Source and data warnings (FR-SRC-04, -05, -07, FR-DOM-04c, -04d, -08, -09b) are logged at WARNING level. Nothing writes to stdout with `print`. | owner |
| **FR-OBS-05** | **Listing the pipeline.** The operator can list every model and table, with tags, inputs, outputs and the current row count of each published table. | decision |
| **FR-OBS-06** | **Describing a table.** The operator can see a table's columns, descriptions, which run last published it, and its archives. | decision |
| **FR-OBS-07** | **Finding past runs.** The operator can list recent runs for an environment from the local run logs, with status and duration. | decision |

---

## 11. Extending the pipeline (FR-DEV)

| ID | Requirement | Source |
|---|---|---|
| **FR-DEV-01** | **Models declare their inputs and outputs.** A model declares the tables it reads and writes by name, and where they come from is determined from those names. A model never builds physical table names itself. | v2-draft (`Transformation`) |
| **FR-DEV-02** | **SQL models.** A model can be a `.sql` file containing one `SELECT`, with inputs written as `{{ ref('table') }}`. A template referencing an undeclared input, or declaring an input it never references, is an error. | v2-draft (`SqlTransformation`); decision (`ref`) |
| **FR-DEV-03** | **Python models.** A model can be a Python function that takes DataFrames and returns one DataFrame, or a dict of DataFrames for models with several outputs. | v2-draft (`PythonTransformation`) |
| **FR-DEV-04** | **Source models.** A model can read from Google sources and write warehouse tables. Only source models may talk to Google. | decision |
| **FR-DEV-05** | **Several outputs per model. (Important.)** A model can produce more than one table, e.g. the penalty model produces both `ext_athlete_penalties` and `ext_results_by_penalty_window`, and the people-sheet model produces two `prep_*` tables. Every part of the framework works per table: each output gets its own temp table, checks, description, row counts, archive and rollback; any output can be named in a selector; a downstream model can depend on just one of the outputs. If any output of a model fails, the whole model fails. | owner |
| **FR-DEV-06** | **Adding a model is local.** Adding a model means writing it and adding it to one registry list. No other file changes. | decision |
| **FR-DEV-07** | **Graph errors caught at startup.** Two models producing the same table, an input no model produces, and cycles are reported when the pipeline loads, with the offending names or cycle path. | `model_selector.py` (unknown deps, cycles) |
| **FR-DEV-08** | **Transforms are pure.** Ranking logic lives in functions with no IO. They can be tested with plain DataFrames. | legacy (already pure apart from `build_dim_races`) |

---

## 12. Quality assurance (FR-QA)

| ID | Requirement | Source |
|---|---|---|
| **FR-QA-01** | **Offline test suite.** The whole pipeline can run end to end in an in-memory database using recorded source data, with no network or credentials. | decision |
| **FR-QA-02** | **Fixture tests per model.** Every ranking model has a test with small recorded inputs and expected outputs. | decision |
| **FR-QA-03** | **Live smoke test.** One optional test reads real Google sources with real credentials. It is skipped by default. | decision |
| **FR-QA-04** | **Port matches v1 before new rules are added.** The legacy logic is ported first, unchanged, and every v2 output table is compared with the matching v1 table for the same inputs: same row count, same keys, same values (with float tolerance and normalized types). Each difference is fixed or explicitly accepted. Only then are the NEW rules (FR-DOM-04a–d, -06, -07, -09a, -09b, -11a, `_27` columns) added, each with its own tests. | goal of the migration; decision (order) |
| **FR-QA-05** | **Comparison tool.** The operator has a command that compares one v2 table with a v1 table and reports differences. It reads v1 Sheets read-only and exists only for the migration. | decision |
| **FR-QA-06** | **New rules are checked against hand-worked examples. NEW.** The 2027 penalty and points calculations, the name matching, and each name-conflict rule (flagged, resolved, and merged-by-mapping cases) each have a test on a small example whose expected values were worked out by hand. Adding `_27` leaves every `_25` and `_26` value unchanged, checked by a test. | decision |

---

## 13. Non-functional requirements (NFR)

| ID | Requirement |
|---|---|
| **NFR-01** | **Speed.** A full prod run on MotherDuck takes no longer than the current v1 run. Results sheets are fetched in parallel. |
| **NFR-02** | **Reproducible installs.** Dependencies are locked (`uv.lock`), the DuckDB version is pinned and kept compatible with MotherDuck, and the minimum Python version reflects the syntax used (3.10+). |
| **NFR-03** | **Security.** Google access is read-only, secrets are never logged or committed, and SQL identifiers built from external text (sheet titles, table names) are always quoted. |
| **NFR-04** | **No side effects at import.** Importing any module makes no network or database connections and does no Prefect setup. |
| **NFR-05** | **Portability.** Runs on macOS and Linux and inside a Prefect worker container, without depending on a local Dropbox path. Run logs go to a configurable directory. |
| **NFR-06** | **Maintainability.** One implementation per concept: one settings object, one warehouse class, one Drive client. `ruff` (rules `I`, `F`, `E`) passes. |

---

## 14. Non-goals

- Changing the 2025 or 2026 ranking formulas, penalty models or eligibility rules. Ranking changes are limited to the items marked NEW.
- Keeping v1 tables, columns or IDs compatible for downstream consumers (R1).
- Writing any output to Google Sheets.
- Warehouse engines other than DuckDB and MotherDuck.
- Automatic deletion of archives (FR-SAFE-05).
- Incremental or partial-table builds. Every model rebuilds its whole tables.
- Running more than one run at once in the same environment (FR-RUN-11).
- A web UI. Users interact through the CLI, Prefect, DuckDB clients and the run log files.

---

## 15. Resolved questions

Answers from the owner's review of draft 1. The "R" numbers replace the draft-1 "Q" numbers.

| # | Draft-1 question | Answer | Reflected in |
|---|---|---|---|
| R1 | Does anyone read the v1 Sheets warehouse tables directly? | Ignore downstream effects; assume nothing breaks downstream. | §1, §14, FR-DOM-06 |
| R2 | Where does registration data come from? | Google Sheets API read of the configured registrations sheet (default `prep_qualifier_registrations`), one tab per season. | FR-SRC-09, FR-ENV-11 |
| R3 | One MotherDuck database with a schema per environment? | One database per project (configurable project name), one schema per environment. | FR-WH-02, FR-ENV-03, -11 |
| R4 | What happens to the v1 Sheets warehouse folders? | They will be archived at cutover. | Cutover (structure doc, Phase 8) |
| R5 | Archive retention? | Archive management is in scope; no automatic deletion for now. | FR-SAFE-05 |
| R6 | Limit results-sheet discovery to one folder? | Yes, the configured Drive folder. | FR-SRC-02 |
| R7 | Keep results-sheet URLs on `dim_races`? | Yes, for observability and lineage. | FR-DOM-01 |
| R8 | Prep-sheet titles? | Configurable; defaults `prep_people_and_spellings` (several tabs, including non-persons) and `prep_schools`. | FR-SRC-08, FR-ENV-11 |

Answers from the owner's review of draft 2:

| # | Draft-2 question | Answer | Reflected in |
|---|---|---|---|
| R9 | Scale of 2027 values? | Scale as if logs were base 1.001 (one unit = 0.1%), applied after mean-centering; target mean 500. Negative values aren't wanted, but won't occur while the slowest times stay under 2× the fastest. | FR-DOM-09a, -09b, -11a |
| R10 | 2027 race penalty: arithmetic mean of top 6, minimum 3, geometric-mean time, 2026 compression? | Yes. | FR-DOM-11a |
| R11 | 2027 reuses the 2026 fit? | Yes: take the 2026 calculation, then mean-center and scale. | FR-DOM-09a |
| R12 | Add gender or school to the name key? | No: school isn't reliable enough to key on. Instead, report likely conflicts (same sheet, exclusive leagues, Eastern Cup age groups). Gender is already part of `athlete_id`. | FR-DOM-04d |
| R13 | Blank-season rows apply to every season? | Yes, so names are adjusted only when a conflict comes up. | FR-DOM-04a, -04c |
| R14 | How to group Eastern Cup races into events? | Not needed. Racers in any U16 Eastern Cup or U16 Championships race and racers in any U18/U20 Eastern Cup race are disjoint sets, so a name in both is two people. | FR-DOM-04d (c) |

---

## 16. Open questions

| # | Question | Affects |
|---|---|---|
| Q4 | **People sheet layout** (*on hold*). What are the tab names in `prep_people_and_spellings` for name spellings and non-persons, and what column headers should the name table use? This spec assumes `name_raw`, `race_season`, `athlete_name`, `disambiguation`. | FR-SRC-08, FR-DOM-04a, FR-ENV-11 |
| Q7 | **League and age-group codes.** This spec assumes the high-school league of a race is its `series` value (`BERK`, `MBE`, `MBW`), and that Eastern Cup age groups can be read from `dim_races.division`. What `division` values do Eastern Cup U16 and U18/U20 races use, and what `series` value marks U16 Championships races? | FR-DOM-04d, FR-ENV-11 |
