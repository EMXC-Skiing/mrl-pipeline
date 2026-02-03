from __future__ import annotations

import threading
import os
import json
import uuid
from dataclasses import dataclass
from typing import Optional, Tuple, List

import pandas as pd
import duckdb
from duckdb import IOException
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed

from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials


# =============================================================================
# Google Drive metadata lookup
# =============================================================================
class DriveService:
    def __init__(self, auth_json_str: str):
        """
        Initialize a Google Drive client using a service account JSON string.

        Parameters
        ----------
        auth_json_str:
            A JSON-encoded service account credential payload.
        """
        self.credentials = Credentials.from_service_account_info(
            json.loads(auth_json_str)
        ).with_scopes(["https://www.googleapis.com/auth/drive.metadata.readonly"])

        self.service = build("drive", "v3", credentials=self.credentials)

    def get_gsheets_by_prefix(self, prefix: str = "prep_results") -> pd.DataFrame:
        """
        List Google Sheets in Drive matching a naming pattern and return metadata.

        The Drive query selects spreadsheets whose name contains the prefix, and then
        the result set is filtered again to only names that start with the prefix.

        Returns a DataFrame with:
          - sheet_name
          - source_file_id
          - modified_at (UTC timestamp)
        """
        files = []
        page_token = None
        query = (
            "mimeType='application/vnd.google-apps.spreadsheet' "
            f"and name contains '{prefix}'"
        )

        while True:
            response = (
                self.service.files()
                .list(
                    q=query,
                    includeItemsFromAllDrives=True,
                    supportsAllDrives=True,
                    fields="nextPageToken, files(name, id, modifiedTime)",
                    pageSize=1000,
                    pageToken=page_token,
                    orderBy="name",
                )
                .execute()
            )

            files.extend(response.get("files", []))
            page_token = response.get("nextPageToken")
            if not page_token:
                break

        df = pd.DataFrame(
            [f for f in files if f.get("name", "").startswith(prefix)]
        ).rename(
            columns={
                "name": "sheet_name",
                "id": "source_file_id",
                "modifiedTime": "modified_at",
            }
        )

        if df.empty:
            return df.assign(modified_at=pd.NaT)

        df["modified_at"] = pd.to_datetime(df["modified_at"], errors="coerce", utc=True)
        df = df.sort_values(
            by=["sheet_name", "modified_at"], ascending=[True, False]
        ).reset_index(drop=True)
        return df


# =============================================================================
# Results standardization logic (performed inside DuckDB)
# =============================================================================
STANDARD_COL_MAP = {
    "name_raw": ["Name", "Skier Name"],
    "time_raw": ["Race Time", "Elapsed"],
    "usss_id": ["USSS#", "Member #"],
    "fis_id": ["FIS#"],
    "school_raw": ["School"],
    "team_raw": ["Team"],
    "state_raw": ["State"],
}

STD_COLS = list(STANDARD_COL_MAP.keys())


def _first_matching_col_expr(actual_cols: List[str], candidates: List[str]) -> str:
    """
    Build a DuckDB SQL expression selecting the first candidate column that exists.

    Returns a VARCHAR expression:
      - CAST("Candidate" AS VARCHAR) if found
      - CAST(NULL AS VARCHAR) if not found
    """
    for c in candidates:
        if c in actual_cols:
            return f'CAST("{c}" AS VARCHAR)'
    return "CAST(NULL AS VARCHAR)"


def standardize_raw_table_in_duckdb(
    con: duckdb.DuckDBPyConnection,
    raw_table: str,
    out_table: str,
    race_id: str,
    gender: str,
    source_file_id: str,
    prep_results_url: str,
) -> None:
    """
    Create a standardized results table inside DuckDB from a raw ingested table.

    Output includes standardized columns plus lineage fields and created_at.
    """
    actual_cols = con.execute(f"PRAGMA table_info('{raw_table}')").df()["name"].tolist()

    select_exprs = []
    for std_col, candidates in STANDARD_COL_MAP.items():
        select_exprs.append(
            f"{_first_matching_col_expr(actual_cols, candidates)} AS {std_col}"
        )

    select_exprs.extend(
        [
            f"'{race_id}'::VARCHAR AS race_id",
            f"'{gender}'::VARCHAR AS gender",
            f"'{source_file_id}'::VARCHAR AS source_file_id",
            f"'{prep_results_url}'::VARCHAR AS prep_results_url",
            "current_timestamp AS created_at",
        ]
    )

    con.execute(f"""
        CREATE OR REPLACE TABLE "{out_table}" AS
        SELECT
            {", ".join(select_exprs)}
        FROM "{raw_table}";
    """)


# =============================================================================
# Race scaffolding and sheet-name resolution
# =============================================================================
def build_dim_races(data, warehouse_env: str) -> pd.DataFrame:
    """
    Build a dim-like races DataFrame from prep tables.

    Assumed data contract:
      - data.iter_prep_tables('prep_all_races') yields (race_season, df) pairs.

    In 'dev' env, a small sample is taken per (race_season, series).
    """
    df_races = pd.concat(
        [
            df.assign(race_season=race_season)
            for race_season, df in data.iter_prep_tables("prep_all_races")
        ],
        ignore_index=True,
    )

    if warehouse_env == "dev":
        df_races = (
            df_races.groupby(["race_season", "series"], group_keys=False)
            .apply(lambda df: df.sample(1))
            .reset_index(drop=True)
        )

    df_dim_races = pd.DataFrame(
        {
            "race_id": df_races["race_id"].astype("string"),
            "race_date": pd.to_datetime(
                df_races["date"], errors="coerce"
            ).dt.normalize(),
            "race_season": df_races["race_season"].astype("int64"),
            "race_name": df_races["name"],
            "series": df_races["series"],
            "technique": df_races["technique"],
            "discipline": df_races["discipline"],
            "race_format": df_races["format"],
            "division": df_races["division"],
            "venue": df_races["venue"],
            "state": df_races["state"],
            "results_pdf_url_b": df_races.get("boys_result_sheet_url"),
            "results_pdf_url_g": df_races.get("girls_result_sheet_url"),
            # These are Google Sheet *titles* referenced by the race table.
            "prep_results_table_b": df_races.get("boys_results_name"),
            "prep_results_table_g": df_races.get("girls_results_name"),
        }
    )

    return df_dim_races


def build_needed_results_list(df_dim_races: pd.DataFrame) -> pd.DataFrame:
    """
    Expand dim_races into a long-form list of required results sheets.

    Output rows are one per (race_id, gender) for non-empty sheet titles.
    """
    rows = []
    for _, r in df_dim_races.iterrows():
        race_id = str(r["race_id"])
        for g in ["b", "g"]:
            sheet_name = r.get(f"prep_results_table_{g}")
            if pd.notna(sheet_name) and str(sheet_name).strip():
                rows.append(
                    {
                        "race_id": race_id,
                        "gender": g,
                        "sheet_name": str(sheet_name).strip(),
                    }
                )

    return pd.DataFrame(rows)


def resolve_sheet_names_to_ids(
    df_needed: pd.DataFrame, df_lookup: pd.DataFrame
) -> pd.DataFrame:
    """
    Resolve sheet titles to Drive file IDs and derive sheet URLs.

    Adds:
      - source_file_id
      - modified_at
      - prep_results_url (edit URL)
      - gviz_csv_url (CSV export URL)
    """
    df = df_needed.merge(
        df_lookup[["sheet_name", "source_file_id", "modified_at"]],
        on="sheet_name",
        how="left",
        validate="many_to_one",
    )

    df["prep_results_url"] = df["source_file_id"].map(
        lambda x: f"https://docs.google.com/spreadsheets/d/{x}/edit"
        if pd.notna(x)
        else pd.NA
    )
    df["gviz_csv_url"] = df["source_file_id"].map(
        lambda x: f"https://docs.google.com/spreadsheets/d/{x}/gviz/tq?tqx=out:csv"
        if pd.notna(x)
        else pd.NA
    )
    return df


def test_one_to_one_race_gender_to_sheet(df_resolved: pd.DataFrame) -> None:
    """
    Validate that each (race_id, gender) maps to exactly one resolved mapping row.

    Also prints non-fatal warnings for missing Drive file IDs (those tasks are skipped).
    """
    counts = df_resolved.groupby(["race_id", "gender"], as_index=False).size()
    bad = counts[counts["size"] != 1]
    if not bad.empty:
        print(
            "ERROR: race_id+gender does not map to exactly one row in the resolved mapping:"
        )
        print(bad.to_string(index=False))
        raise ValueError(
            "Uniqueness test failed: race_id+gender -> sheet is not one-to-one."
        )

    missing = df_resolved[df_resolved["source_file_id"].isna()]
    if not missing.empty:
        print(
            "WARNING: Some required results sheet names could not be resolved to Drive file IDs (will be skipped):"
        )
        print(missing[["race_id", "gender", "sheet_name"]].to_string(index=False))


def validate_drive_lookup_against_races(
    df_drive_lookup: pd.DataFrame,
    df_needed: pd.DataFrame,
    *,
    prefix: str = "prep_results",
) -> None:
    """
    Validate consistency between Drive metadata search results and required sheet titles.

    Enforces:
      (A) No sheet_name appears with multiple different Drive file IDs. (hard error)
      (B) Any sheets found by metadata that are not referenced in df_needed are reported
          as warnings (not hard errors).
    """
    df_drive = df_drive_lookup.copy()
    df_drive = df_drive[
        df_drive["sheet_name"].astype(str).str.startswith(prefix)
    ].copy()

    # (A) Duplicate title means multiple distinct Drive file IDs for the same sheet_name.
    dup = (
        df_drive.groupby("sheet_name", as_index=False)
        .agg(n_ids=("source_file_id", "nunique"))
        .query("n_ids > 1")
    )
    if not dup.empty:
        detail = (
            df_drive[df_drive["sheet_name"].isin(dup["sheet_name"])]
            .groupby("sheet_name")["source_file_id"]
            .apply(lambda s: sorted(set(map(str, s))))
            .reset_index(name="source_file_ids")
        )
        print(
            "ERROR: Drive metadata search returned multiple different sheets with the same title."
        )
        print(detail.to_string(index=False))
        raise ValueError(
            "Metadata search duplicate-title failure: sheet_name is not unique in Drive results."
        )

    # (B) Extra sheets in Drive metadata are treated as hygiene warnings (not fatal).
    needed_names = set(df_needed["sheet_name"].astype(str))
    drive_names = set(df_drive["sheet_name"].astype(str))

    extra_in_drive = sorted(drive_names - needed_names)
    if extra_in_drive:
        sample = extra_in_drive[:50]
        print(
            "WARNING: Drive metadata search found prep_results* sheets not referenced in the race list.\n"
            "These may be orphaned or manually-created sheets."
        )
        print(f"Count extra in Drive: {len(extra_in_drive)}")
        print("Sample sheet_name values:")
        for n in sample:
            print(f"  - {n}")


# =============================================================================
# Parallel DuckDB ingestion: per-sheet raw ingest -> standardized table -> union
# =============================================================================
# One global lock to serialize extension LOAD across all threads.
_HTTPFS_LOAD_LOCK = threading.Lock()


def _is_httpfs_double_init_error(e: Exception) -> bool:
    msg = str(e).lower()
    return "already registered secret type" in msg and "s3" in msg


def load_httpfs_serialized(con: duckdb.DuckDBPyConnection) -> None:
    """
    Ensure httpfs is loaded for this connection, serializing the LOAD across threads.

    Notes:
      - Uses `LOAD httpfs;` inside a global lock to prevent concurrent init.
      - Calls `INSTALL httpfs;` as well (safe to repeat).
      - If DuckDB throws the known double-init error, treat it as success.
    """
    with _HTTPFS_LOAD_LOCK:
        try:
            # LOAD is what triggers init; serialize to avoid race.
            con.execute("LOAD httpfs;")
        except Exception as e:
            if not _is_httpfs_double_init_error(e):
                raise
            # If we hit the known "already registered" error, another thread likely
            # initialized it; proceed.


@dataclass(frozen=True)
class SheetTask:
    """
    Immutable task description for ingesting and standardizing one results sheet.
    """

    race_id: str
    gender: str
    sheet_name: str
    source_file_id: str
    prep_results_url: str
    gviz_csv_url: str


def _reset_duckdb_file(db_path: str) -> None:
    try:
        os.remove(db_path)
    except FileNotFoundError:
        pass


def _init_duckdb_file(db_path: str) -> None:
    """
    Initialize a DuckDB database file and ensure httpfs is installed and loadable.
    Turn OFF DuckDB's auto-loading of httpfs as this causes race conditions when multi-threaded.
    """
    con = duckdb.connect(db_path)
    try:
        con.execute("INSTALL httpfs;")
    finally:
        con.close()


def process_one_sheet_into_duckdb(
    db_path: str, task: SheetTask
) -> Tuple[bool, Optional[str]]:
    """
    Ingest and standardize a single sheet into DuckDB.

    Returns:
      (ok, standardized_table_name)
    """
    con = duckdb.connect(db_path)

    try:
        # Ensure httpfs is loaded in serial

        suffix = uuid.uuid4().hex[:12]
        raw_table = f"raw_{suffix}"
        std_table = f"std_{suffix}"

        try:
            load_httpfs_serialized(con)
            con.execute(f"""
                CREATE OR REPLACE TABLE "{raw_table}" AS
                SELECT *
                FROM read_csv(
                    '{task.gviz_csv_url}',
                    auto_type_candidates = ['VARCHAR'],
                    header = true
                );
            """)
        except IOException as e:
            print("WARNING: Failed to fetch results sheet via gviz CSV (skipping).")
            print(f"  race_id={task.race_id} gender={task.gender}")
            print(f"  sheet_name={task.sheet_name}")
            print(f"  gviz_csv_url={task.gviz_csv_url}")
            print(f"  duckdb.IOException: {e}")
            return False, None

        standardize_raw_table_in_duckdb(
            con=con,
            raw_table=raw_table,
            out_table=std_table,
            race_id=task.race_id,
            gender=task.gender,
            source_file_id=task.source_file_id,
            prep_results_url=task.prep_results_url,
        )

        return True, std_table

    except Exception as e:
        print("WARNING: Unexpected error processing results sheet (skipping).")
        print(f"  race_id={task.race_id} gender={task.gender}")
        print(f"  sheet_name={task.sheet_name}")
        print(f"  gviz_csv_url={task.gviz_csv_url}")
        print(f"  error={type(e).__name__}: {e}")
        return False, None

    finally:
        con.close()


def union_standardized_tables_to_df(
    db_path: str, table_names: List[str]
) -> pd.DataFrame:
    """
    Union all per-sheet standardized tables into a single DuckDB table (stg_results),
    then return it as a pandas DataFrame.
    """
    con = duckdb.connect(db_path)
    try:
        if not table_names:
            cols = STD_COLS + [
                "race_id",
                "gender",
                "source_file_id",
                "prep_results_url",
                "created_at",
            ]
            return pd.DataFrame(columns=cols)

        union_sql = " UNION ALL ".join([f'SELECT * FROM "{t}"' for t in table_names])
        con.execute(f"CREATE OR REPLACE TABLE stg_results AS {union_sql};")
        return con.execute("SELECT * FROM stg_results").df()
    finally:
        con.close()


def add_results_urls_to_dim_races(
    df_dim_races: pd.DataFrame, df_resolved: pd.DataFrame
) -> pd.DataFrame:
    """
    Add per-race Google Sheets URLs to dim_races, pivoted into separate columns
    for boys/girls (prep_results_url_b and prep_results_url_g).
    """
    df_urls = (
        df_resolved[["race_id", "gender", "prep_results_url"]]
        .dropna(subset=["prep_results_url"])
        .pivot_table(
            index="race_id",
            columns="gender",
            values="prep_results_url",
            aggfunc="first",
        )
    )

    df_urls = df_urls.rename(
        columns={c: f"prep_results_url_{c}" for c in df_urls.columns}
    ).reset_index()

    return df_dim_races.merge(df_urls, how="left", on="race_id")


# =============================================================================
# Orchestration
# =============================================================================
def run_results_hybrid_pipeline(
    *,
    data,
    google_hex_credentials: str,
    warehouse_env: str = "prod",
    prefix: str = "prep_results",
    duckdb_path: str = "temp_results.duckdb",
    max_workers: Optional[int] = None,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Orchestrate results ingestion and standardization across many Google Sheets.

    Key behaviors:
      - Duplicate Drive titles (same sheet_name -> multiple file IDs) are a hard error.
      - Orphaned Drive sheets (found in metadata, not referenced by race list) are warnings.
      - Missing sheet resolutions (race references a title not found in Drive) are warnings and skipped.
    """

    _reset_duckdb_file(duckdb_path)
    _init_duckdb_file(duckdb_path)

    df_dim_races = build_dim_races(data, warehouse_env)

    drive = DriveService(google_hex_credentials)
    df_drive_lookup = drive.get_gsheets_by_prefix(prefix=prefix)

    df_needed = build_needed_results_list(df_dim_races)

    # Validate uniqueness and report (non-fatal) hygiene issues.
    validate_drive_lookup_against_races(df_drive_lookup, df_needed, prefix=prefix)

    df_resolved = resolve_sheet_names_to_ids(df_needed, df_drive_lookup)

    test_one_to_one_race_gender_to_sheet(df_resolved)

    df_tasks = df_resolved.dropna(subset=["source_file_id"]).reset_index(drop=True)
    tasks = [
        SheetTask(
            race_id=str(r["race_id"]),
            gender=str(r["gender"]),
            sheet_name=str(r["sheet_name"]),
            source_file_id=str(r["source_file_id"]),
            prep_results_url=str(r["prep_results_url"]),
            gviz_csv_url=str(r["gviz_csv_url"]),
        )
        for _, r in df_tasks.iterrows()
    ]

    std_tables: List[str] = []
    failures = 0

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(process_one_sheet_into_duckdb, duckdb_path, task)
            for task in tasks
        ]

        with tqdm(
            total=len(futures),
            desc="Reading+standardizing results sheets",
            unit="sheet",
        ) as pbar:
            for fut in as_completed(futures):
                ok, tbl = fut.result()
                if ok and tbl:
                    std_tables.append(tbl)
                else:
                    failures += 1
                pbar.update(1)

    if failures:
        print(
            f"INFO: Finished with {failures} failure(s). Successful sheets: {len(std_tables)}."
        )

    stg_results_df = union_standardized_tables_to_df(duckdb_path, std_tables)

    # data.put_warehouse_table("stg_results", stg_results_df)

    df_dim_races_out = add_results_urls_to_dim_races(df_dim_races, df_resolved)
    # data.put_warehouse_table("dim_races", df_dim_races_out)

    return stg_results_df, df_dim_races_out
