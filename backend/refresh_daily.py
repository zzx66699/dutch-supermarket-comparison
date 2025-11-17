"""
Refresh A (daily, Mon–Sat):
For each existing URL in dirk_data:
- Fetch current_price, regular_price, valid_from, valid_to
- If URL invalid => set availability = false
- Else:
    - If (curr, reg, vf, vt) unchanged => skip
    - Else => update those fields (and availability = true)
"""


from supabase_utils import get_supabase, upsert_rows

from dirk_core import fetch_price_snapshot, normalize_date, normalize_price


def main():
    supabase = get_supabase()

    # Fetch the columns
    resp = supabase.table("dirk_data").select("url, current_price, regular_price, valid_from, valid_to, availability").execute()

    if getattr(resp, "error", None):
        print("❌ Error reading dirk_data:", resp.error)
        return

    rows_db = resp.data or []
    print(f"[refresh_daily] Loaded {len(rows_db)} rows from dirk_data.")

    updates = []
    invalid_count = 0
    unchanged_count = 0
    changed_count = 0

    for idx, row in enumerate(rows_db, start=1):
        url = row["url"]

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
        same_avail = (old_avail is True)  # if it's valid now, we want availability = True

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


        print(f"[refresh_daily] Unchanged rows: {unchanged_count}")
        print(f"[refresh_daily] Invalid URLs: {invalid_count}")
        print(f"[refresh_daily] Rows with changes: {changed_count}")

        if updates:
            upsert_rows("dirk_data", updates)
            print(f"[refresh_daily] Upsert {len(updates)} updated rows to Supabase. ")
        else:
            print("[refresh_daily] Nothing to update.")


if __name__ == "__main__":
    main()
