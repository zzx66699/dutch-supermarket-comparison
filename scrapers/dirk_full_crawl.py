"""
Full pipeline for dirk_full_crawl.py:

1. Discover all product URLs (starting from category pages)
2. Parse each product page into a raw product dict
3. Add the supermarket column
4. Translate product names to English
5. Parse unit strings into (unit_qty, unit_type)
6. Replace ±inf with NaN at DataFrame level
7. Sanitize data so it's JSON-safe
8. Upsert

Run this as a one-off or manual script:
    python backend/dirk_full_crawl.py
"""

import numpy as np
import pandas as pd
from supabase_utils import upsert_rows

# When import, Python will load & execute the entire file dirk_core.py first.
from dirk_core import (
    fetch_all_dirk_products,
    crawl_urls,
    extract_product_id_from_url,
    translate_cached
)

from dotenv import load_dotenv
load_dotenv()

if __name__ == "__main__":

    # 1. Crawl all product URLs
    urls = crawl_urls()
    print(f"[dirk_full_crawl] Found {len(urls)} product URLs")

    # 2. Get a dataframe of id and url
    rows = []
    for url in urls:
        pid = extract_product_id_from_url(url)
        rows.append({
            "url": url,
            "sku": pid,
        })

    df_url = pd.DataFrame(rows)
    print(f"[dirk_full_crawl] df_url: {len(df_url)} rows")

    # 3. Get product information by GraphQL
    details = fetch_all_dirk_products()
    df_details = pd.DataFrame(details)
    print(f"[dirk_full_crawl] df_details: {len(df_details)} rows")

    # 4. Combine
    df = df_details.merge(
        df_url,
        how="left",
        on="sku"
    )

    # 5. Translate product_name_du → product_name_en
    df["product_name_en"] = df["product_name_du"].apply(translate_cached)

    # 6. Replace ±inf with NaN at DataFrame level (just in case)
    df = df.replace([np.inf, -np.inf], np.nan)

    # 7. Convert to list-of-dicts
    rows = df.to_dict(orient="records")

    # 8. Upsert
    print(f"[dirk_full_crawl] Uploading {len(rows)} rows to Supabase...")
    upsert_rows("dirk",rows)

