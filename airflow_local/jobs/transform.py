import pandas as pd
from typing import List, Dict, Any

NATURAL_KEY = [
    "flight_date", "flight_number", "airline_name", "departure_airport", "arrival_airport"
]

ALL_COLS = [
    "flight_date", "flight_status",
    "departure_airport", "departure_timezone",
    "arrival_airport",   "arrival_timezone", "arrival_terminal",
    "airline_name", "flight_number", "ingested_at",
]

def transform(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if not rows:
        return []

    df = pd.DataFrame(rows)

    for c in ALL_COLS:
        if c not in df.columns:
            df[c] = None

    str_cols = [
        "flight_status","departure_airport","arrival_airport",
        "airline_name","flight_number","departure_timezone",
        "arrival_timezone","arrival_terminal"
    ]
    for c in str_cols:
        df[c] = (
            df[c].astype(str)
                .str.strip()
                .str.replace(r"\s+", " ", regex=True)
        )
        df.loc[df[c].isin(["", "None", "nan", "NaN", "NaT"]), c] = None

    df["flight_date"] = pd.to_datetime(df["flight_date"], errors="coerce").dt.date.astype("string")
    df.loc[df["flight_date"].isin(["<NA>", "NaT", "None", "nan"]), "flight_date"] = None

    ing = pd.to_datetime(df["ingested_at"], errors="coerce", utc=True)
    df["ingested_at"] = ing.dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    df.loc[df["ingested_at"].isna(), "ingested_at"] = None

    req = (
        df["departure_airport"].notna() &
        df["arrival_airport"].notna() &
        df["flight_number"].notna() &
        df["airline_name"].notna() &
        df["flight_date"].notna()
    )
    df = df[req]

    before = len(df)
    sort_key = pd.to_datetime(df["ingested_at"], errors="coerce", utc=True)
    df = (
        df.assign(_sort=sort_key)
          .sort_values("_sort")
          .drop_duplicates(subset=NATURAL_KEY, keep="last")
          .drop(columns=["_sort"])
    )
    after = len(df)
    print(f"Transform: {before} -> {after} tras dedup")

    df = df.where(pd.notnull(df), None)
    return df[ALL_COLS].to_dict(orient="records")
