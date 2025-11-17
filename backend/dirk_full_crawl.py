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
from dirk_core import (
    SUPERMARKET,
    CATEGORY_URLS,
    crawl_urls,
    parse_product_page,
    translate_cached,
    parse_unit,

)

from supabase_utils import upsert_rows 

if __name__ == "__main__":

    # 1. Crawl all product URLs
    urls = crawl_urls(CATEGORY_URLS)
    print(f"[dirk_full_crawl] Found {len(urls)} product URLs")

    # 2. Parse each product page
    products = []
    for url in urls:
        product = parse_product_page(url)
        # we will add None in the list if soup is None or price_large_tag is None. None will become empty row in df, so we should remove them.
        if product:
            products.append(product)
            
    df = pd.DataFrame(products)
    print(f"[dirk_full_crawl] Parsed products: {len(df)} rows")

    # 3. Add supermarket column
    df["supermarket"] = SUPERMARKET

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
    print(f"[dirk_full_crawl] Uploading {len(rows)} rows to Supabase...")
    upsert_rows("dirk_data",rows)

