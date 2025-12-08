from __future__ import annotations

import re
import time
from datetime import date

import pandas as pd
import requests
from deep_translator import GoogleTranslator
from datetime import date, datetime

from supabase_utils import get_supabase, upsert_rows
from typing import List, Dict, Any

import time
import requests
import pandas as pd
from datetime import datetime, date
from typing import Dict, Any, List, Set

# ---------------------------------------------------------------------------
# Translation
# ---------------------------------------------------------------------------

translation_cache = {}

def translate_cached(text):
    """
    Translate a Dutch product name to English using GoogleTranslator, with an in-memory cache.

    Returns None if text is None or translation fails.
    """
    if not text:
        return None
    
    if text in translation_cache:
        return translation_cache[text]

    try:
        en = GoogleTranslator(source='nl', target='en').translate(text)
        translation_cache[text] = en
        return en
    except Exception as e:
        print(f"[translate_product_names] Translation failed for: {text} | Reason: {e}")
        return None
    

# ---------------------------------------------------------------------------
# Unit parsing
# ---------------------------------------------------------------------------
def handle_normalized(unit_text): 
    """
    Converts the normalized format of unit (e g. 205 g, 290kg) into (unit_qty, unit_type),
    with unit_type ∈ {"kg", "l", "piece"}.
    """
    m = re.match(r"^\s*(\d+(?:\.\d+)?)\s*([a-zA-Z]+)", unit_text)
    if not m:
        print("[WARN] cannot parse:", unit_text)
        return None, None
    
    unit_qty = float(m.group(1))   
    unit_type = m.group(2)

    if unit_type in ("g","gram", "gr"): # "500 gr"," 154 gram"
        return unit_qty / 1000.0, "kg"
    if unit_type in ("kg", "kilo"):
        return unit_qty, "kg"
    if unit_type == "ml":
        return unit_qty / 1000.0, "l"
    if unit_type == "cl":
        return unit_qty / 100.0, "l"
    if unit_type == "l":
        return unit_qty, "l"    
    
    return unit_qty, "piece"


def parse_unit(unit_text: str):
    """
    Converts messy Dutch unit strings into (unit_qty, unit_type)
        - Converts messy unit into normalized unit first, so the function "handle_normalized" can handle it.
    unit_type ∈ {"kg", "l", "piece"} or (None, None) if unknown.
    """
    if pd.isna(unit_text):
        return None, None

    s = unit_text.strip().lower()
    s = s.replace(",", ".")
    s = s.replace("×", "x")
    s = s.replace("stuks", "stuk")
    s = s.replace("st.", "stuk")
    s = s.replace("-"," ")           # "5-pack" -> "5 pack"
    s = s.replace("ca. ", "")        # "ca. 115 g" -> "115 g"
    s = s.replace("ca ", "")        # "ca 444 g" -> "444 g"
    s = s.replace("los per ", "")    # "loose per 500 g" -> "500 g"

    # "2-3 pers | 20 min" -> "2-3 pers"
    if "|" in s:
        s = s.split("|", 1)[0].strip()
    if re.search(r"\bpers(?:oon|onen)?\b", s):
        return 1, "piece"
    
    s = re.sub(r"^\s*per\s+", "", s) # "per 500 g" -> "g", "per stuk" -> "stuk"
    s = s.split("(")[0].strip()      # Extract everything before the first left parenthese "1 kg (ca. 5 stuk)"

    # "stuk" -> "1 stuk"
    if not any(i.isdigit() for i in s): 
        s = "1 " + s

    # "6 x 250 g" -> "1500 g"
    m = re.match(r"(\d+)\s*x\s*(\d+(?:\.\d+)?)\s*([a-zA-Z]+)", s)
    if m:
        count = float(m.group(1))
        size = float(m.group(2))
        unit_type = m.group(3).split()[0] # eg. "6 x 250 g appel" -> drop "appel"
        unit_qty = size * count
        s = str(unit_qty) + unit_type

    # "4 + 2 stuks" -> "6 stuks"
    m = re.match(r"(\d+(?:\.\d+)?)\s*\+\s*(\d+(?:\.\d+)?)\s*([a-zA-Z]+)", s)
    if m:
        unit_qty = float(m.group(1)) + float(m.group(2))
        unit_type = m.group(3).split()[0]
        s = str(unit_qty) + unit_type

    # "2-3 pers|20 min" -> "1 stuck"
    m = re.match(r"(\d+(?:\.\d+)?)\s*(\d+(?:\.\d+)?)\s*([a-zA-Z]+)", s)
    
    return(handle_normalized(s))   


# ---------------------------------------------------------------------------
# Normalize the price and date for refresh
# ---------------------------------------------------------------------------
def normalize_price(v):
    """
    Normalize price to float or None for comparison.
    - In scrapper: current_price = f"0.{price_large_tag.get_text(strip=True)}". This is a string.
    - In supabse: current_price is stored as float8
    - To compare them, we need to normalize into float. 
    """
    if v is None:
        return None
    return float(v) 


def normalize_date(v):
    """Convert date/datetime to ISO string for comparison; keep None as None.
    - In scrapper: date(2025, 11, 4)
    - In supabse: "2025-11-04"
    """
    if v is None:
        return None
    if isinstance(v, (date, datetime)):
        return v.isoformat()
    return str(v)


# ---------------------------------------------------------------------------
# Fetch products via API
# ---------------------------------------------------------------------------
BASE_URL = "https://api.ah.nl"

BASE_HEADERS = {
    "User-Agent": "Appie/8.63 Android/12-API31",
    "X-Application": "AHWEBSHOP",
    "Content-Type": "application/json; charset=UTF-8",
}

def auth_headers(access_token: str) -> Dict[str, str]:
    h = BASE_HEADERS.copy()
    h["Authorization"] = f"Bearer {access_token}"
    return h

def get_access_token() -> str:
    url = f"{BASE_URL}/mobile-auth/v1/auth/token/anonymous"
    resp = requests.post(url, headers=BASE_HEADERS, json={"clientId": "appie"}, timeout=10)
    # data = {
    #     "access_token": "USERID_ACCESSTOKEN",
    #     "refresh_token": "REFRESHTOKEN",
    #     "expires_in": 7199
    # }
    resp.raise_for_status()
    data = resp.json()
    return data["access_token"]


def get_root_categories(access_token: str) -> List[Dict[str, Any]]:
    """
    Top-level categories, e.g. 'Aardappel, groente, fruit', 'Vlees', etc.
    It may return: 
    - {"category":list[dict]}
        {
            "categories": [
                { "id": 6401, "name": "Groente en fruit", ... },
                { "id": 3200, "name": "Frisdrank", ... }
            ]
        }
    - list[dict]
        [
            { "id": 6401, "name": "Groente en fruit" },
            { "id": 3200, "name": "Frisdrank" }
        ]

    """
    url = f"{BASE_URL}/mobile-services/v1/product-shelves/categories"
    resp = requests.get(url, headers=auth_headers(access_token), timeout=10)
    resp.raise_for_status()
    data = resp.json()
    if isinstance(data, dict) and "categories" in data:
        return data["categories"]
    return data


def get_subcategories(access_token: str, category_id: int) -> List[Any]:
    """
    Direct sub-categories for a given category id.
    It may return: 
    - list[int]  list[str]
    - list[dict]
    - { "subCategories": [...] }
    """
    url = f"{BASE_URL}/mobile-services/v1/product-shelves/categories/{category_id}/sub-categories"
    resp = requests.get(url, headers=auth_headers(access_token), timeout=10)

    if resp.status_code in (204, 404):
        return []

    resp.raise_for_status()
    data = resp.json()

    if isinstance(data, dict):
        for key in ("subCategories", "categories", "children"):
            if key in data and isinstance(data[key], list):
                return data[key]
        
        return []

    if isinstance(data, list):
        return data

    return []


def collect_all_taxonomy_ids(access_token: str) -> Set[int]:
    """
    Traverse the category tree via /categories and /categories/{id}/sub-categories,
    return a set of all category ids (taxonomyIds).

    - root: list[dict] 
    - cat can be: list[str] / list[int] / list[dict]
    """
    # roots = [
    #     {"id":1618, "name": "Zuivel"},
    #     {"id":21217, "name":"Dranken"},
    #     ...
    # ]
    roots = get_root_categories(access_token)

    queue: List[int] = []
    
    for cat in roots:
        cid = None
        if isinstance(cat, dict):
            for key in ("id", "categoryId", "taxonomyId"):
                if key in cat and isinstance(cat[key], int):
                    cid = cat[key]
                    break
        elif isinstance(cat, int):
            cid = cat
        elif isinstance(cat, str) and cat.isdigit():
            cid = int(cat)

        if cid is not None:
            queue.append(cid)

    seen: Set[int] = set()

    # queue = [6401, 21217, 1618, ...]
    print(f"[AH] root categories: {len(queue)}")

    while queue:
        cid = queue.pop()
        if cid in seen:
            continue
        seen.add(cid)

        try:
            subs = get_subcategories(access_token, cid)
        except Exception as e:
            print(f"[AH] warning: failed to fetch sub-categories for {cid}: {e}")
            continue

        for sub in subs:
            sid = None

            if isinstance(sub, dict):
                for key in ("id", "categoryId", "taxonomyId"):
                    val = sub.get(key)
                    if isinstance(val, int):
                        sid = val
                        break
                    if isinstance(val, str) and val.isdigit():
                        sid = int(val)
                        break
            elif isinstance(sub, int):
                sid = sub
            elif isinstance(sub, str) and sub.isdigit():
                sid = int(sub)

            if sid is not None and sid not in seen:
                queue.append(sid)

        
        time.sleep(0.05)
    
    # seen = {
    #     861, 868, 877, 881, 884, 890, 892, 894, ...)
    # }
    print(f"[AH] collected taxonomy ids: {len(seen)}")
    return seen


def search_products_by_taxonomy(
    access_token: str,
    taxonomy_id: int,
    page: int = 0,
    size: int = 100,
) -> Dict[str, Any]:
    """
    Search products within a specific taxonomy (category) id.
    """
    url = f"{BASE_URL}/mobile-services/product/search/v2"
    params = {
        "sortOn": "RELEVANCE",
        "page": page,
        "size": size,
        "taxonomyId": taxonomy_id,
        "adType": "TAXONOMY",
        "availableOnline": "true",
        "orderable": "any",
    }
    resp = requests.get(url, headers=auth_headers(access_token), params=params, timeout=10)

    if resp.status_code == 400:
        print(f"[AH search taxonomy] 400 for taxonomyId={taxonomy_id}, page={page}")
        return {}

    resp.raise_for_status()
    return resp.json()

def fetch_all_products_via_taxonomies(
    access_token: str,
    page_size: int = 100,
    max_taxonomies: int | None = None,
) -> List[Dict[str, Any]]:
    """
    Enumerate *all* products by walking all taxonomyIds (categories + subcategories).
    """
    taxonomy_ids = sorted(collect_all_taxonomy_ids(access_token))
    if max_taxonomies is not None:
        taxonomy_ids = taxonomy_ids[:max_taxonomies]

    all_products_by_id: Dict[int, Dict[str, Any]] = {}

    for idx, tid in enumerate(taxonomy_ids, start=1):
        print(f"\n[AH taxonomy] ({idx}/{len(taxonomy_ids)}) taxonomyId={tid}")
        page = 0

        while True:
            data = search_products_by_taxonomy(
                access_token, taxonomy_id=tid, page=page, size=page_size
            )
            if not data:
                break

            page_info = data.get("page") or {}
            total_pages = page_info.get("totalPages", page + 1)

            products = data.get("products") or []
            if not products:
                break

            for p in products:
                wid = p.get("webshopId")
                if wid is not None and wid not in all_products_by_id:
                    all_products_by_id[wid] = p

            collected = len(all_products_by_id)
            print(
                f"  [AH taxonomy] tid={tid} page {page+1}/{total_pages}, "
                f"products on this page={len(products)}, unique collected={collected}"
            )

            page += 1
            if page >= total_pages:
                break

            time.sleep(0.03)  

    print(f"\n[AH] total unique products collected via taxonomy: {len(all_products_by_id)}")
    return list(all_products_by_id.values())


def map_product_to_row(p: Dict[str, Any]) -> Dict[str, Any]:
    """
    Map raw AH product JSON -> one row in DataFrame.
    """
    wid = p.get("webshopId")
    url = f"https://www.ah.nl/producten/product/wi{wid}" if wid is not None else None

    title_raw = p.get("title") or ""
    brand = p.get("brand") or ""
    # title_raw contains brand name. e.g. "AH Latex handschoenen one size". remove the brand name from the title.
    product_name_du = title_raw.strip()
    if brand:
        b = brand.strip()
        if product_name_du.lower().startswith(b.lower() + " "):
            product_name_du = product_name_du[len(b) + 1:].strip()

    unit_du = p.get("salesUnitSize")

    unit_qty = None
    unit_type_en = None
    if unit_du:
        unit_qty, unit_type_en = parse_unit(unit_du)


    regular_price = p.get("priceBeforeBonus")
    current_price = p.get("currentPrice", regular_price)

    bonus_start = p.get("bonusStartDate")
    bonus_end = p.get("bonusEndDate")

    valid_from = None
    valid_to = None
    if bonus_start:
        valid_from = bonus_start  
    if bonus_end:
        valid_to = bonus_end
    
    

    return {
        "sku": wid,
        "url": url,
        "product_name_du": product_name_du,
        "unit_du": unit_du,
        "unit_type_en": unit_type_en,
        "unit_qty": unit_qty,
        "regular_price": regular_price,
        "current_price": current_price,
        "valid_from": valid_from,
        "valid_to": valid_to,
        "brand": brand,
    }


def fetch_all_ah_products(
    page_size: int = 100,
    max_taxonomies: int | None = None,
):
    token = get_access_token()
    products = fetch_all_products_via_taxonomies(
        token,
        page_size=page_size,
        max_taxonomies=max_taxonomies,
    )
    rows = [map_product_to_row(p) for p in products]
    return rows


# ---------------------------------------------------------------------------
# Daily refresh for AH
# ---------------------------------------------------------------------------
def refresh_ah_daily():
    """
    1. Fetch all existing AH products from Supabase -> old_by_sku
    2. Fetch all fresh AH products via API -> new_by_sku
    3. missing_skus = old_skus - new_skus
         -> availability = False
    4. joint_skus   = old_skus ∩ new_skus
         -> if price/promo changed -> update
    5. add_skus     = new_skus - old_skus
         -> insert new products with full info (url, names, unit, brand, prices, etc.)
    """
    # -------------------------------------------------------------------
    # 1. Fetch existing from Supabase
    # -------------------------------------------------------------------
    supabase = get_supabase()
    resp = supabase.table("ah").select(
        "sku, url, product_name_du, product_name_en, unit_du, unit_qty, unit_type_en, "
        "regular_price, current_price, valid_from, valid_to, brand, availability"
    ).execute()
    old_rows = resp.data or []
    old_by_sku: Dict[str, Dict[str, Any]] = {
        str(r["sku"]): r for r in old_rows if r.get("sku") is not None
    }
    old_skus = set(old_by_sku.keys())
    print(f"[AH daily] Found {len(old_skus)} existing AH products in DB.")

    # -------------------------------------------------------------------
    # 2. Fetch fresh AH products via API
    # -------------------------------------------------------------------
    fresh_products = fetch_all_ah_products()
    new_by_sku: Dict[str, Dict[str, Any]] = {
        str(p["sku"]): p for p in fresh_products if p.get("sku") is not None
    }
    new_skus = set(new_by_sku.keys())
    print(f"[AH daily] Fetched {len(new_skus)} fresh AH products from API.")

    # -------------------------------------------------------------------
    # 3. Set comparisons
    # -------------------------------------------------------------------
    missing_skus = old_skus - new_skus
    joint_skus = old_skus & new_skus
    add_skus = new_skus - old_skus

    print(f"[AH daily] missing_skus: {len(missing_skus)}")
    print(f"[AH daily] joint_skus:   {len(joint_skus)}")
    print(f"[AH daily] add_skus:     {len(add_skus)}")

    rows_to_upsert: List[Dict[str, Any]] = []

    # -------------------------------------------------------------------
    # 4.1) missing_skus: mark as unavailable
    # -------------------------------------------------------------------
    for sku in missing_skus:
        rows_to_upsert.append(
            {
                "sku": sku,
                "availability": False,
            }
        )

    # -------------------------------------------------------------------
    # 4.2) joint_skus: compare price / promo, update if changed
    # -------------------------------------------------------------------
    for sku in joint_skus:
        old = old_by_sku[sku]
        new = new_by_sku[sku]

        old_cp = normalize_price(old.get("current_price"))
        old_rp = normalize_price(old.get("regular_price"))
        old_vf = normalize_date(old.get("valid_from"))
        old_vt = normalize_date(old.get("valid_to"))

        new_cp = normalize_price(new.get("current_price"))
        new_rp = normalize_price(new.get("regular_price"))
        new_vf = normalize_date(new.get("valid_from"))
        new_vt = normalize_date(new.get("valid_to"))

        if (
            new_cp == old_cp
            and new_rp == old_rp
            and new_vf == old_vf
            and new_vt == old_vt
            and old.get("availability") is True
        ):
            continue

        row = {
            "sku": sku,
            "regular_price": new.get("regular_price"),
            "current_price": new.get("current_price"),
            "valid_from": new.get("valid_from"),
            "valid_to": new.get("valid_to"),
            "availability": True,
        }
        rows_to_upsert.append(row)

    # -------------------------------------------------------------------
    # 4.3) add_skus: insert brand-new products
    # -------------------------------------------------------------------
    for sku in add_skus:
        new = new_by_sku[sku]

        product_name_du = new.get("product_name_du")
        product_name_en = translate_cached(product_name_du) if product_name_du else None

        rows_to_upsert.append(
            {
                "sku": sku,
                "url": new.get("url"),
                "product_name_du": product_name_du,
                "product_name_en": product_name_en,
                "brand": new.get("brand"),
                "unit_du": new.get("unit_du"),
                "unit_qty": new.get("unit_qty"),
                "unit_type_en": new.get("unit_type_en"),
                "regular_price": new.get("regular_price"),
                "current_price": new.get("current_price"),
                "valid_from": new.get("valid_from"),
                "valid_to": new.get("valid_to"),
                "availability": True,
            }
        )

    if not rows_to_upsert:
        print("[AH daily] nothing to upsert.")
        return

    print(f"[AH daily] upserting {len(rows_to_upsert)} rows to Supabase...")

    upsert_rows("ah", rows_to_upsert, conflict_col="sku")
    print("[AH daily] Done.")
