import re
import pandas as pd
from hashlib import sha256

# Helpers


def calc_athlete_id(df: pd.DataFrame) -> pd.Series:
    # athlete id is a hash of the skier's name and gender; it is the granular entity for which points are calculated
    # we calculate it as a SHA-256 hash of gender and athlete name, truncated to 12 characters
    return df.apply(
        lambda row: sha256(
            f"{row['gender']},{row['athlete_name']}".encode()
        ).hexdigest()[:12],
        axis=1,
    )


def calc_result_id(df: pd.DataFrame) -> pd.Series:
    return df.apply(
        lambda row: sha256(
            f"{row['athlete_id']},{row['race_id']}".encode()
        ).hexdigest()[:12],
        axis=1,
    )


def calc_registration_id(df: pd.DataFrame) -> pd.Series:
    return df.apply(
        lambda row: sha256(
            f"{row['athlete_id']},{row['race_season']}".encode()
        ).hexdigest()[:12],
        axis=1,
    )


def timestr_to_min(s):
    if not isinstance(s, str):
        return s
    s = s.replace("'", "")
    units = [1.0 / 60, 1, 60]
    try:
        return sum([float(x) * units[i] for i, x in enumerate(s.split(":")[::-1])])
    except ValueError:
        return s


def float_time_to_str(t_min):
    if pd.isna(t_min):
        return ""
    minutes = int(t_min)
    seconds = (t_min - minutes) * 60
    return f"{minutes:02}:{seconds:05.2f}"


# Build token-replacement artifacts


def build_name_replacement_artifacts(
    df_names_equivalent_spellings: pd.DataFrame,
    df_schools_equivalent_spellings: pd.DataFrame,
    df_non_people: pd.DataFrame,
) -> tuple[dict, dict, list]:
    """
    Returns:
      athlete_name_replacements: dict[variant_name -> canonical_name]
      school_name_replacements:  dict[variant_name -> canonical_name]
      list_non_people:           list[str] of non-person competitor names
    """

    # Create dictionary for athlete name standardization
    athlete_name_replacements = {
        row.iloc[1]: row.iloc[0] for _, row in df_names_equivalent_spellings.iterrows()
    }

    # Create dictionary for school name standardization
    school_name_replacements = {
        row.iloc[1]: row.iloc[0]
        for _, row in df_schools_equivalent_spellings.iterrows()
    }

    list_non_people = list(df_non_people["Non-Person Competitor Name"])

    return athlete_name_replacements, school_name_replacements, list_non_people


# Main transform: takes fetched tables + artifacts, returns dim_results


def build_dim_results(
    df_stg_results: pd.DataFrame,
    df_dim_races: pd.DataFrame,
    *,
    athlete_name_replacements: dict,
    school_name_replacements: dict,
    list_non_people: list,
) -> pd.DataFrame:
    """
    Returns a df_dim_results suitable for data.put_warehouse_table('dim_results', ...).
    """

    df_dim_results = df_stg_results.copy()

    # join key race information from race table
    df_dim_results = df_dim_results.merge(
        df_dim_races[["race_id", "race_date", "race_season", "series"]],
        how="left",
        on="race_id",
    )

    # replace the \xa0 character with space from the name column of df_results
    df_dim_results["name_raw"] = df_dim_results["name_raw"].str.replace(
        "\xa0", " ", regex=True
    )

    # remove excess whitespace from name column of df_results
    df_dim_results["name_raw"] = df_dim_results["name_raw"].str.strip()

    # use regex replace to remove more than one space between first and last name
    df_dim_results["name_raw"] = df_dim_results["name_raw"].str.replace(
        " +", " ", regex=True
    )

    # strip asterisks from names
    df_dim_results["name_raw"] = df_dim_results["name_raw"].str.replace(
        "*", "", regex=False
    )

    # convert names to title case unless they come from Eastern Cup results
    df_dim_results["name_raw"] = df_dim_results.apply(
        lambda r: r["name_raw"] if r["series"] == "EC" else r["name_raw"].title(),
        axis=1,
    )

    # clean up names according to provided spelling equivalents table
    df_dim_results["athlete_name"] = df_dim_results["name_raw"].replace(
        athlete_name_replacements
    )

    # standardize identification of unidentified athletes (eg "Unknown Skier")
    df_dim_results["is_unidentified_athlete"] = df_dim_results["athlete_name"].isin(
        list_non_people
    )
    df_dim_results["athlete_name"] = df_dim_results["athlete_name"].replace(
        list_non_people, "Unidentified Athlete"
    )

    # strip whitespace from times
    df_dim_results["time_raw"] = df_dim_results["time_raw"].str.strip()

    # Convert 'time_float' column to numeric, coercing invalid strings to NaN
    df_dim_results["time_float"] = pd.to_numeric(
        df_dim_results["time_raw"].apply(timestr_to_min), errors="coerce"
    )

    # Apply the formatting function to the 'time_float' column
    df_dim_results["time"] = df_dim_results["time_float"].apply(float_time_to_str)

    # add finish place column - will ignore NaN finishes
    race_groups = df_dim_results.groupby(["race_id", "gender"])
    for ix in race_groups.indices.values():
        df_dim_results.loc[ix, "finish_place"] = (
            df_dim_results.loc[ix, "time_float"]
            .rank(method="min", na_option="keep")
            .astype("Int64")
        )

    # add identifiers (USSS/FIS, team, school, state)
    df_dim_results["school"] = (
        df_dim_results["school_raw"].str.title().replace(school_name_replacements)
    )
    df_dim_results["team"] = df_dim_results[
        "team_raw"
    ]  # TODO: standardize team names via lookup table
    df_dim_results["state"] = df_dim_results[
        "state_raw"
    ]  # TODO: pull in USSS data to get state from USSS number

    # calculate athlete and result unique ids
    df_dim_results["athlete_id"] = calc_athlete_id(df_dim_results)
    df_dim_results["result_id"] = calc_result_id(df_dim_results)

    leading_columns = [
        "result_id",
        "race_id",
        "athlete_id",
        "race_date",
        "race_season",
        "gender",
        "finish_place",
        "athlete_name",
        "time",
        "time_float",
    ]
    drop_columns = [
        "time_raw",
        "name_raw",
        "series",
        "school_raw",
        "team_raw",
        "state_raw",
    ]

    # reorder columns
    df_dim_results = df_dim_results[
        leading_columns
        + [
            c
            for c in df_dim_results.columns
            if c not in (leading_columns + drop_columns)
        ]
    ]

    return df_dim_results


def build_dim_registrations(
    df_stg_registrations: pd.DataFrame,
    *,
    athlete_name_replacements: dict,
    school_name_replacements: dict,
) -> pd.DataFrame:
    """
    Transform-only function (no warehouse IO). Assumes helper fns exist:
      - calc_athlete_id(df) -> Series
      - calc_registration_id(df) -> Series

    Inputs:
      df_stg_registrations: already-fetched + concatenated staging registrations, with race_season present
      athlete_name_replacements, school_name_replacements: artifacts from precursor function

    Returns:
      df_dim_registrations
    """

    df = df_stg_registrations.copy().convert_dtypes()

    # drop rows that are not related to EHS/U16 race registration
    eligible_categories = [
        "U16/EHS Qualifer - Boys",
        "U16/EHS Qualifier - Girls",
        "Late U16/EHS Mass Nordic Cup - Boys",
        "Late U16/EHS Mass Nordic Cup - Girls",
    ]
    df = df.loc[
        df["Category Entered / Merchandise Ordered"]
        .astype("string")
        .str.strip()
        .isin(eligible_categories)
    ]

    # define replacements and standardize inputs
    club_name_replacements = {
        "I am not currently a member of a listed team or club": pd.NA,
        "My Team / Club is not listed.": pd.NA,
        "": pd.NA,
        "Eastern Mass Nordic, inc (EMXC)": "EMXC",
    }
    membership_number_replacements = {"NO LICENSE": "", "INTERNATIONAL": ""}
    gender_replacements = {"m": "b", "f": "g"}

    # helpers local to this transform
    def pdnum(x):
        return pd.to_numeric(x, errors="coerce")

    def fn_school_or_other(row):
        school = row["School"]
        other = row.get("SchoolOth")
        val = school if school != "Other" else other
        return val.title() if isinstance(val, str) else val

    def fn_full_names(row):
        first = row.get("First Name")
        last = row.get("Last Name")
        full = f"{first} {last}"
        return full.title() if isinstance(full, str) else full

    def process_grade(s):
        # Step 1: integer present
        for grade in range(7, 13):
            if str(grade) in str(s):
                return grade

        # Step 2: keyword map
        grade_keywords = {
            "freshman": 9,
            "firstyear": 9,
            "sophomore": 10,
            "junior": 11,
            "senior": 12,
        }
        s_cleaned = re.sub(r"[^a-z0-9]", "", str(s).lower())
        for keyword, grade in grade_keywords.items():
            if keyword in s_cleaned:
                return grade

        return pd.NA

    # Construct final DataFrame with standardized and transformed columns
    df_dim_registrations = pd.DataFrame(
        {
            "athlete_name": (
                df.apply(fn_full_names, axis=1)
                .astype("string")
                .replace(athlete_name_replacements)
            ),
            "race_season": pdnum(df["race_season"]).astype("int64"),
            "city_town": df["City"].astype("string").str.title(),
            "state": df["State"].astype("string").str.upper(),
            "school": (
                df.apply(fn_school_or_other, axis=1)
                .replace(school_name_replacements)
                .astype("string")
            ),
            "club": df["Club"].replace(club_name_replacements).astype("string"),
            "gender": df["Gender"]
            .astype("string")
            .str.lower()
            .replace(gender_replacements),
            "nensa_number": (
                pdnum(
                    df["NENSA License"].replace(membership_number_replacements)
                ).astype("Int64")
            ),
            "usss_number": (
                pdnum(
                    df["USSS Member #"].replace(membership_number_replacements)
                ).astype("Int64")
            ),
            "birth_year": pdnum(df["Birth Year"]).astype("int64"),
            "grade": pdnum(df["Grade"].apply(process_grade)).astype("Int64"),
        }
    ).sort_values(by=["athlete_name", "race_season"], ascending=[True, False])

    # calculate ids
    df_dim_registrations["athlete_id"] = calc_athlete_id(df_dim_registrations)
    df_dim_registrations["registration_id"] = calc_registration_id(df_dim_registrations)

    # US Ski & Snowboard age is the athlete's age at the end of December of a racing season
    df_dim_registrations["usss_age"] = (
        df_dim_registrations["race_season"] - 1
    ) - df_dim_registrations["birth_year"]

    # add U16s and EHS qualification eligibility
    df_dim_registrations["is_ehs_eligible"] = df_dim_registrations["grade"].isin(
        [9, 10, 11, 12]
    )
    df_dim_registrations["is_u16c_eligible"] = df_dim_registrations["usss_age"].isin(
        [14, 15]
    )

    # filter to only registrations eligible for U16s or EHS
    df_dim_registrations = df_dim_registrations.loc[
        df_dim_registrations["is_ehs_eligible"]
        | df_dim_registrations["is_u16c_eligible"]
    ]

    leading_columns = [
        "registration_id",
        "race_season",
        "athlete_id",
        "athlete_name",
        "gender",
        "school",
        "club",
        "city_town",
        "state",
    ]
    df_dim_registrations = df_dim_registrations[
        leading_columns
        + [c for c in df_dim_registrations.columns if c not in leading_columns]
    ]

    return df_dim_registrations
