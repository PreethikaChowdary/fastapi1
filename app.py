# app.py
from fastapi import FastAPI, Request, Response, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
from typing import Optional, Tuple
from datetime import datetime
import threading
import os

CSV_PATH = os.getenv("SENSOR_CSV_PATH", "./data/q-fastapi-timeseries-cache.csv")

app = FastAPI(title="Sensor Stats with Cache")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)

_df = None
_cache = {}
_cache_lock = threading.Lock()

def _parse_date(s: Optional[str]) -> Optional[pd.Timestamp]:
    if s is None:
        return None
    try:
        return pd.to_datetime(s)
    except Exception:
        raise ValueError(f"Invalid date format: {s}")

def _make_cache_key(location, sensor, start_date, end_date) -> Tuple:
    return (None if location is None else str(location).strip().lower(),
            None if sensor is None else str(sensor).strip().lower(),
            None if start_date is None else str(start_date),
            None if end_date is None else str(end_date))

def _compute_stats_from_df(df: pd.DataFrame) -> dict:
    cnt = int(df.shape[0])
    if cnt == 0:
        return {"count": 0, "avg": None, "min": None, "max": None}
    avg = float(df["value"].mean())
    mn = float(df["value"].min())
    mx = float(df["value"].max())
    return {"count": cnt, "avg": avg, "min": mn, "max": mx}

@app.on_event("startup")
def load_data():
    global _df
    if not os.path.exists(CSV_PATH):
        raise RuntimeError(f"CSV not found at {CSV_PATH}. Place the CSV in the repo or set SENSOR_CSV_PATH env var.")
    _df = pd.read_csv(CSV_PATH)
    _df.columns = [c.strip() for c in _df.columns]
    required = {"timestamp","location","sensor","value"}
    if not required.issubset(set(_df.columns)):
        raise RuntimeError("CSV must contain columns: timestamp, location, sensor, value")
    _df["timestamp"] = pd.to_datetime(_df["timestamp"], errors="coerce")
    _df["value"] = pd.to_numeric(_df["value"], errors="coerce")
    _df = _df.dropna(subset=["timestamp","value"])
    _df["location"] = _df["location"].astype(str).str.strip().str.lower()
    _df["sensor"] = _df["sensor"].astype(str).str.strip().str.lower()

@app.get("/stats")
def stats(
    request: Request,
    response: Response,
    location: Optional[str] = None,
    sensor: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
):
    global _df, _cache
    if _df is None:
        raise HTTPException(status_code=500, detail="Data not loaded")
    try:
        sd = _parse_date(start_date)
        ed = _parse_date(end_date)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    key = _make_cache_key(location, sensor, sd.isoformat() if sd is not None else None, ed.isoformat() if ed is not None else None)

    with _cache_lock:
        if key in _cache:
            response.headers["X-Cache"] = "HIT"
            return {"stats": _cache[key]}

    df = _df
    if location is not None:
        df = df[df["location"] == location.strip().lower()]
    if sensor is not None:
        df = df[df["sensor"] == sensor.strip().lower()]
    if sd is not None:
        df = df[df["timestamp"] >= sd]
    if ed is not None:
        df = df[df["timestamp"] <= ed]

    stats_obj = _compute_stats_from_df(df)

    with _cache_lock:
        _cache[key] = stats_obj

    response.headers["X-Cache"] = "MISS"
    return {"stats": stats_obj}
