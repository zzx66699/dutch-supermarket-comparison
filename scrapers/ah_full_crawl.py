import numpy as np
import pandas as pd

from ah_core import (
    fetch_all_ah_products,
    translate_cached,
    parse_unit,

)

from supabase_utils import upsert_rows 

if __name__ == "__main__":

    # 1. Fetch all the products
    df = pd.DataFrame(fetch_all_ah_products(page_size=60, max_taxonomies=None)) 
    print("rows:", len(df))

    # 2. Translate product_name_du → product_name_en
    df["product_name_en"] = df["product_name_du"].apply(translate_cached)

    # 3. Parse unit strings → unit_qty, unit_type_en
    df[["unit_qty", "unit_type_en"]] = df["unit_du"].apply(
        lambda x: pd.Series(parse_unit(x))
    )

    # 3. Replace ±inf with NaN at DataFrame level (just in case)
    df = df.replace([np.inf, -np.inf], np.nan)

    # 4. Convert to list-of-dicts
    rows = df.to_dict(orient="records")

    # 5. Upsert
    print(f"[ah_full_crawl] Uploading {len(rows)} rows to Supabase...")
    upsert_rows("ah",rows)

