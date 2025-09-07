import os
import requests
from datetime import datetime, timezone

API_URL = "http://api.aviationstack.com/v1/flights"

def _as_str(x):
    if x is None:
        return None
    return str(x).strip()

def extract(limit=100, status="active"):
    api_key = os.getenv("AVIATIONSTACK_KEY") or os.getenv("API_KEY")
    if not api_key:
        raise ValueError(";issing API Key")

    params = {"access_key": api_key, "flight_status": status, "limit": limit}
    r = requests.get(API_URL, params=params, timeout=30)
    r.raise_for_status()
    payload = r.json()
    data = payload.get("data", []) or []

    now_iso = datetime.now(timezone.utc).isoformat()
    rows = []
    for it in data:
        dep = it.get("departure") or {}
        arr = it.get("arrival") or {}
        al  = it.get("airline") or {}
        fl  = it.get("flight") or {}

        row = {
            "flight_date": _as_str(it.get("flight_date")),
            "flight_status": _as_str(it.get("flight_status")),
            "departure_airport": _as_str(dep.get("airport")),
            "departure_timezone": _as_str(dep.get("timezone")),
            "arrival_airport": _as_str(arr.get("airport")),
            "arrival_timezone": _as_str(arr.get("timezone")),
            "arrival_terminal": _as_str(arr.get("terminal")),
            "airline_name": _as_str(al.get("name")),
            "flight_number": _as_str(fl.get("number")),
            "ingested_at": now_iso,
        }
        rows.append(row)
    return rows 
