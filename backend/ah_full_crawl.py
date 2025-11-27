import numpy as np
import pandas as pd

# When import, Python will load & execute the entire file dirk_core.py first.
from ah_core import (
    SUPERMARKET,
    CATEGORY_URLS,
    crawl_urls,
    get_soup,
    get_current_bonus_period,
    parse_ah_product_page,
    translate_cached,
    parse_unit,

)

from supabase_utils import upsert_rows 

if __name__ == "__main__":

    # 1. Crawl all product URLs
    urls = crawl_urls(CATEGORY_URLS)
    print(f"[dirk_full_crawl] Found {len(urls)} product URLs")

    # 2. Parse each product page
    bonus_period = get_current_bonus_period() # Get the bonus period
    print(f"[ah_full_crawl] bonus_period = {bonus_period}")

    products = []
    for url in urls:
        product = parse_ah_product_page(url, bonus_period = bonus_period)
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

