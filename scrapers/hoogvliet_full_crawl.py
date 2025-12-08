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

# When import, Python will load & execute the entire file dirk_core.py first.
from hoogvliet_core import (
    fetch_all_products_with_prices,
    parse_product_page,
    translate_cached,
    parse_unit,

)

from supabase_utils import upsert_rows 

if __name__ == "__main__":
    # 1. Using API to fetch the details of all the products
    products = fetch_all_products_with_prices()
    print("\nTotal products with prices:", len(products)) 
    df = pd.DataFrame(products)

    # 2. Parsing from HTML to get the promotion period. (Only for the products that are on sales)
    df_promoted = df[df["regular_price"] != df["current_price"]]
    urls = df_promoted["url"].tolist()

    product_valid_period = []
    for url in urls:
        valid_period = parse_product_page(url)
        product_valid_period.append(valid_period)
        print(len(product_valid_period))

    df_product_valid_period = pd.DataFrame(product_valid_period)

    # 3. Merge the dataframes to get the whole information.
    df = df.merge(
    df_product_valid_period,
    how="left",
    on="url"
    )

    # 4. Translate product_name_du → product_name_en
    df["product_name_en"] = df["product_name_du"].apply(translate_cached)

    # 5. Parse unit strings → unit_qty, unit_type_en
    df[["unit_qty", "unit_type_en"]] = df["unit_du"].apply(
        lambda x: pd.Series(parse_unit(x))
    )

    # 6. Replace ±inf with NaN at DataFrame level (just in case)
    df = df.replace([np.inf, -np.inf], np.nan)

    # 7. Convert to list-of-dicts
    rows = df.to_dict(orient="records")

    # 8. Upsert
    print(f"[hoogvliet_full_crawl] Uploading {len(rows)} rows to Supabase...")
    upsert_rows("hoogvliet",rows)


