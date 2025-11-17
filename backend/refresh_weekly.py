from dirk_core import (
    SUPERMARKET,
    CATEGORY_URLS,
    translate_cached,
    crawl_urls,
    parse_unit,
    parse_product_page,
    fetch_price_snapshot,
    normalize_date,
    normalize_price,
)

from supabase_utils import get_supabase, upsert_rows

import pandas as pd
import numpy as np

def main():
    # Crawl all category pages again -> new_urls
    new_urls = set(crawl_urls(CATEGORY_URLS))
    
    # Get the existing data
    supabase = get_supabase()
    resp = (supabase.table("dirk_data")
            .select("url, current_price, regular_price, valid_from, valid_to, availability")
            .execute()
    )
    if getattr(resp, "error", None):
        print("❌ Error reading dirk_data:", resp.error)
        return
    rows_db = resp.data or []
    print(f"[refresh_weekly] Loaded {len(rows_db)} rows from dirk_data.")


    # Map url -> row for easy lookup
    # db_by_url =
    # {
    #   "A": {"url": "A", "price": 1.0},
    #   "B": {"url": "B", "price": 2.0},
    # }
    db_by_url = {}
    for row in rows_db:
        url = row.get("url")
        if url:
            db_by_url[url] = row

    # Get the set of old_urls
    old_urls = set(db_by_url.keys())
    
    # Get the 3 groups of urls in set
    missing_urls = old_urls - new_urls 
    joint_urls = old_urls & new_urls
    add_urls = new_urls - old_urls

    updates = []
    invalid_count = 0
    unchanged_count = 0
    changed_count = 0


    # -------------------------------------------------
    # A) URLs that disappeared from category pages
    #    -> availability = False
    # -------------------------------------------------
    for url in missing_urls:
        row = db_by_url.get(url, {})
        old_avail = row.get("availability", True)

        # Only update if it’s not already False
        if old_avail is not False:
            updates.append({
                "url": url,
                "availability": False,
            })
            changed_count += 1
        else:
            unchanged_count += 1
    

    # -------------------------------------------------
    # B) URLs that still exist on category pages (joint)
    #    -> re-crawl and compare fields
    # -------------------------------------------------

    for idx, url in enumerate(joint_urls, start = 1):
        # row is a dic of all the data
        # row = {
        # "url":
        # "current_price":,
        # "regular_price":,
        # "valid_from":,
        # "valid_to":,
        # "availability":,
        # }
        row = db_by_url[url]

        old_curr = normalize_price(row.get("current_price"))
        old_reg = normalize_price(row.get("regular_price"))
        old_vf = normalize_date(row.get("valid_from"))
        old_vt = normalize_date(row.get("valid_to"))
        old_avail = row.get("availability", True)

        snap = fetch_price_snapshot(url)

        # -------------------------------
        # Case 1: URL invalid / product gone
        # -------------------------------
        if not snap:
            invalid_count += 1

            # Only update if it's not already False
            if old_avail is not False:
                updates.append({
                    "url": url,
                    "availability": False,
                })
                changed_count += 1
            else:
                unchanged_count += 1

            continue

        # -------------------------------
        # Case 2: URL valid — compare fields
        # -------------------------------
        new_curr = normalize_price(snap["current_price"])
        new_reg = normalize_price(snap["regular_price"])
        new_vf = normalize_date(snap["valid_from"])
        new_vt = normalize_date(snap["valid_to"])

        same_curr = (old_curr == new_curr)
        same_reg = (old_reg == new_reg)
        same_vf = (old_vf == new_vf)
        same_vt = (old_vt == new_vt)
        same_avail = (old_avail is True)  

        if same_curr and same_reg and same_vf and same_vt and same_avail:
            # No change needed
            unchanged_count += 1
        else:
            # Something changed → upsert only the fields we care about
            update_row = {
                "url": url,
                "current_price": snap["current_price"],
                "regular_price": snap["regular_price"],
                "valid_from": snap["valid_from"],
                "valid_to": snap["valid_to"],
                "availability": True,
            }
            updates.append(update_row)
            changed_count += 1
        

    # -------------------------------------------------
    # C) Apply updates for Case 1 and Case 2
    # -------------------------------------------------  
   
    if updates:
        print(f"[refresh_daily] Upserting {len(updates)} changed rows to Supabase...")
        upsert_rows("dirk_data", updates)
    else:
        print("[refresh_daily] No updates to existing rows.")

    print(
        f"[refresh_daily] Stats: invalid={invalid_count}, "
        f"changed={changed_count}, unchanged={unchanged_count}"
    )


    # -------------------------------------------------
    # D) New URLs (add_urls) -> full crawl and insert
    # -------------------------------------------------
    products = []
    for url in add_urls:
        product = parse_product_page(url)
        if not product:
            continue

        product.setdefault("url", url)
        products.append(product)
   
    if not products:
        print("[refresh_daily] No new URLs to insert.")
        return

    df = pd.DataFrame(products)

    df["supermarket"] = SUPERMARKET
    df["product_name_en"] = df["product_name_du"].apply(translate_cached)
    df[["unit_qty", "unit_type_en"]] = df["unit_du"].apply(
        lambda x: pd.Series(parse_unit(x))
    )

    df = df.replace([np.inf, -np.inf], np.nan)

    rows = df.to_dict(orient="records")

    print(f"[dirk_full_crawl] Uploading {len(rows)} rows to Supabase...")
    upsert_rows("dirk_data",rows)


if __name__ == "__main__":
    main()