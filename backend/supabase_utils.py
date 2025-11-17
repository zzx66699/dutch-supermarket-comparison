import os
import math
from typing import List, Dict, Any

import pandas as pd
import numpy as np
from supabase import create_client

import time
import httpx



def get_supabase():
    """Create and return a Supabase client using env vars."""
    url = os.environ["SUPABASE_URL"]
    key = os.environ["SUPABASE_SERVICE_KEY"]
    return create_client(url, key)


# ---------- helper to make values JSON-safe ----------

def sanitize_value(v: Any) -> Any:
    import datetime

    # floats: kill NaN / inf
    if isinstance(v, (float, np.floating)):
        if math.isnan(v) or math.isinf(v):
            return None
        return float(v)

    # numpy int -> python int
    if isinstance(v, (int, np.integer)):
        return int(v)

    # numpy bool -> python bool
    if isinstance(v, (np.bool_,)):
        return bool(v)

    # pandas timestamps / dates -> string
    if isinstance(v, (pd.Timestamp, datetime.date, datetime.datetime)):
        return v.isoformat()

    # None stays None (becomes null in JSON)
    if v is None:
        return None

    return v


def sanitize_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return [{k: sanitize_value(v) for k, v in row.items()} for row in rows]


def upsert_rows(table_name: str, rows: List[Dict[str, Any]], batch_size: int = 100):
    """
    upsert = update existing row if the primary key (or unique key) matches, otherwise insert a new row.
    - upsert one row at a time
    - if one row fails → log it and continue with the next row
    - no retries, no crashes
    """
    if not rows:
        print("[upsert_rows] No rows to upsert.")
        return

    supabase = get_supabase()
    safe_rows = sanitize_rows(rows)

    total = len(safe_rows)

    for idx, row in enumerate(safe_rows, start=1):
        url = row.get("url")
        try:
            supabase.table(table_name).upsert(row).execute()
            print(f"[upsert_rows] OK {idx}/{total} url={url}")
        except Exception as e:
            print(f"[upsert_rows] ❌ Skip {idx}/{total} url={url} due to error: {e}")

    print("[upsert_rows] Done.")

