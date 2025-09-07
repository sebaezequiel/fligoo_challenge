from typing import List, Dict, Tuple
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import MetaData, Table, Column, Integer, Text, Date, TIMESTAMP
from .db import get_engine

NATURAL_KEY = ("flight_date","flight_number","airline_name","departure_airport","arrival_airport")

def _dedup_batch(rows: List[Dict]) -> List[Dict]:
    uniq = {}
    for r in rows:
        key: Tuple = tuple(r.get(k) for k in NATURAL_KEY)
        uniq[key] = r
    return list(uniq.values())

def upsert_rows(rows: List[Dict]) -> int:
    if not rows:
        return 0

    rows = _dedup_batch(rows)

    engine = get_engine()
    md = MetaData()
    testdata = Table(
        "testdata", md,
        Column("id", Integer, primary_key=True),
        Column("flight_date", Date),
        Column("flight_status", Text),
        Column("departure_airport", Text),
        Column("departure_timezone", Text),
        Column("arrival_airport", Text),
        Column("arrival_timezone", Text),
        Column("arrival_terminal", Text),
        Column("airline_name", Text),
        Column("flight_number", Text),
        Column("ingested_at", TIMESTAMP(timezone=False)),
        schema=None
    )

    stmt = insert(testdata).values(rows)
    update_cols = {
        "flight_status": stmt.excluded.flight_status,
        "departure_timezone": stmt.excluded.departure_timezone,
        "arrival_timezone": stmt.excluded.arrival_timezone,
        "arrival_terminal": stmt.excluded.arrival_terminal,
        "ingested_at": stmt.excluded.ingested_at,
    }

    stmt = stmt.on_conflict_do_update(
        index_elements=list(NATURAL_KEY),
        set_=update_cols
    )

    with engine.begin() as conn:
        res = conn.execute(stmt)
        return res.rowcount or 0
