import os
import pandas as pd
from supabase import create_client
import numpy as np
import json
import math

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_SERVICE_KEY = os.environ["SUPABASE_SERVICE_KEY"]
supabase = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)

df = pd.read_csv("backend/dirk_data.csv")

# 2. Replace all +inf / -inf → NaN globally
df = df.replace([np.inf, -np.inf], np.nan)

# 3. Convert to list-of-dicts
rows = df.to_dict(orient="records")

# 4. Brutal sanitizer on the *dicts*, not the DataFrame
import datetime

def sanitize_value(v):
    # Numpy floats → Python float, but kill NaN/inf
    if isinstance(v, (float, np.floating)):
        if math.isnan(v) or math.isinf(v):
            return None
        return float(v)

    # Numpy ints → Python int
    if isinstance(v, (int, np.integer)):
        return int(v)

    # Numpy bools → Python bool
    if isinstance(v, (np.bool_,)):
        return bool(v)

    # Pandas / datetime objects → ISO strings
    if isinstance(v, (pd.Timestamp, datetime.date, datetime.datetime)):
        return v.isoformat()

    # Leave None as None (becomes null in JSON)
    if v is None:
        return None

    # Everything else (str, normal bool, etc.) → unchanged
    return v


def sanitize_row(row: dict) -> dict:
    return {k: sanitize_value(v) for k, v in row.items()}


rows = [sanitize_row(r) for r in rows]

# 5. Double-check there are no bad floats left
def has_bad_values(obj):
    if isinstance(obj, (float, np.floating)):
        return math.isnan(obj) or math.isinf(obj)
    if isinstance(obj, dict):
        return any(has_bad_values(v) for v in obj.values())
    if isinstance(obj, list):
        return any(has_bad_values(v) for v in obj)
    return False

print("Any bad floats in rows?", any(has_bad_values(r) for r in rows))

# Optional: try JSON-dumping ONE row to be sure
try:
    json.dumps(rows[0])
    print("Sample row is JSON safe ✅")
except Exception as e:
    print("Sample row JSON error:", e)

# 6. Upsert into Supabase
BATCH_SIZE = 100
for i in range(0, len(rows), BATCH_SIZE):
    batch = rows[i:i+BATCH_SIZE]
    res = supabase.table("dirk_data").upsert(batch).execute()
    print(f"Batch upsert OK: {len(batch)} rows")

