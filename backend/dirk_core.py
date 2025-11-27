from __future__ import annotations

import re
import time
from collections import deque
from datetime import date

import pandas as pd
import requests
from bs4 import BeautifulSoup
from deep_translator import GoogleTranslator
from urllib.parse import urljoin, urlparse
from datetime import date, datetime
import xml.etree.ElementTree as ET

from supabase_utils import get_supabase, sanitize_rows, upsert_rows
from typing import List, Dict, Any

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
# Fetch product info using GraphQL
# ---------------------------------------------------------------------------
DIRK_GRAPHQL_URL = "https://web-dirk-gateway.detailresult.nl/graphql"

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json",
    "Content-Type": "application/json",
}

# Check the webgroup_ids manually from the dirk website, the max is 146
DIRK_WEBGROUP_IDS = list(range(1, 147))  # [1, 2, ..., 146]
DEFAULT_STORE_ID = 66  


def fetch_webgroup_raw(web_group_id: int, store_id: int = DEFAULT_STORE_ID) -> list[dict]:
    """
    Using Dirk GraphQL, get all the response from webGroupId
    It returns a list with element like:
      {
        "productId": 21204,
        "normalPrice": 1.99,
        "offerPrice": 0.0,
        "startDate": "...",
        "endDate": "...",
        "productOffer": {...} or null,
        "productInformation": {
            "productId": 21204,
            "headerText": "...",
            "packaging": "400 g",
            "image": "...",
            "department": "...",
            "webgroup": "...",
            "brand": "...",
            ...
        }
      }
    """
    query = f"""
    query {{
      listWebGroupProducts(webGroupId: {web_group_id}) {{
        productAssortment(storeId: {store_id}) {{
          productId
          normalPrice
          offerPrice
          isSingleUsePlastic
          singleUsePlasticValue
          startDate
          endDate
          productOffer {{
            textPriceSign
            endDate
            startDate
            disclaimerStartDate
            disclaimerEndDate
          }}
          productInformation {{
            productId
            headerText
            subText
            packaging
            image
            department
            webgroup
            brand
          }}
        }}
      }}
    }}
    """.strip()

    payload = {"query": query, "variables": {}}

    resp = requests.post(DIRK_GRAPHQL_URL, headers=HEADERS, json=payload, timeout=15)
    resp.raise_for_status()
    data = resp.json()

    assort = (
        data.get("data", {})
            .get("listWebGroupProducts", {})
            .get("productAssortment", [])
        or []
    )

    items = [p for p in assort if p is not None]
    return items


def fetch_all_dirk_products(
    webgroup_ids: list[int] = DIRK_WEBGROUP_IDS,
    store_id: int = DEFAULT_STORE_ID,
    sleep_sec: float = 0.2,
) -> list[dict]:
    """
    Scan all the webGroupId, remove deplicates based on productId.
    """
    all_by_id: dict[int, dict] = {}

    for gid in webgroup_ids:
        print(f"\n=== Fetching webGroupId {gid} ===")
        try:
            items = fetch_webgroup_raw(gid, store_id=store_id)
        except Exception as e:
            print(f"  !! error on gid={gid}: {e}")
            continue

        print(f"  {len(items)} products in this group")

        for it in items:
            pid = it.get("productId")
            if pid is None:
                continue
            all_by_id[pid] = it  

        time.sleep(sleep_sec)  

    print(f"\n[INFO] unique products collected: {len(all_by_id)}")


    products: list[dict] = []
    for pid, raw in all_by_id.items():
        info = raw.get("productInformation") or {}
        product_name_du = info.get("headerText")
        offer = raw.get("productOffer") or {}

        normal_price = raw.get("normalPrice")
        offer_price = raw.get("offerPrice")
        # Dirk GraphQL: offerPrice = 0 → means NO OFFER
        if offer_price in (0, 0.0, None):
            offer_price = normal_price
        unit_du=  info.get("packaging")

        promo_start = offer.get("startDate") or raw.get("startDate")
        promo_end = offer.get("endDate") or raw.get("endDate")

        unit_type_en = None
        if unit_du:
            unit_qty, unit_type_en = parse_unit(unit_du)

        products.append(
            {
                "sku": pid,
                "product_name_du": product_name_du,
                "brand": info.get("brand"),
                "unit_du": unit_du,
                "unit_qty": unit_qty,
                "unit_type_en": unit_type_en,
                "regular_price": normal_price,
                "current_price": offer_price,
                "valid_from": promo_start,
                "valid_to": promo_end,
                # "department": info.get("department"),
                # "webgroup": info.get("webgroup"),
                # "image_path": info.get("image"),
            }
        )

    return products


# ---------------------------------------------------------------------------
# Fetch product urls using sitemap
# ---------------------------------------------------------------------------
SITEMAP_URL = "https://www.dirk.nl/products-sitemap.xml"


# Reuse a single session for all requests
session = requests.Session()

# ---------------------------------------------------------------------------
# HTTP / HTML helpers
# ---------------------------------------------------------------------------

def get_soup(url, timeout=10):
    """
    Fetch a URL, return a BeautifulSoup object or return None on error / non-200.
    """
    try:
        response = session.get(url, headers=HEADERS, timeout=timeout)
        if response.status_code != 200:
            return None
        return BeautifulSoup(response.text, "html.parser")
    except Exception:
        return None


def crawl_urls():
    """
    Extract all the food-related urls from the sitemap
    """
    try:
        resp = requests.get(SITEMAP_URL, headers=HEADERS, timeout=30)
        if resp.status_code != 200:
            print("Failed to download sitemap:", resp.status_code)
            return []

        # Dirk uses standard sitemap namespace
        NS = {"ns": "http://www.sitemaps.org/schemas/sitemap/0.9"}
        root = ET.fromstring(resp.text)

        all_urls = []
        for url_tag in root.findall("ns:url", NS):
            loc = url_tag.find("ns:loc", NS)
            if loc is not None and loc.text:
                all_urls.append(loc.text.strip())

        product_urls = [url for url in all_urls]

        return sorted(product_urls)

    except Exception:
        print("Error parsing sitemap:")
        return []


# ---------------------------------------------------------------------------
# Extract product id from product url
# ---------------------------------------------------------------------------
import re
from urllib.parse import urlparse

def extract_product_id_from_url(url: str) -> int | None:
    """
    Given a Dirk product URL, extract productId from the last path segment.
    Examples (hypothetical):
      https://www.dirk.nl/boodschappen/aardappelen/zoete-aardappelfriet-21204  -> 21204
      https://www.dirk.nl/product/1-de-beste-aardappelen-3-kg-103911          -> 103911

    Returns int productId or None if not found.
    """
    try:
        path = urlparse(url).path  # '/boodschappen/.../zoete-aardappelfriet-21204'
        last_seg = path.rstrip("/").split("/")[-1]
        parts = last_seg.split("-")

        # find last all-digit chunk
        for token in reversed(parts):
            if token.isdigit():
                return int(token)
        # fallback: if whole last segment is digits
        if last_seg.isdigit():
            return int(last_seg)
    except Exception:
        pass

    print("[WARN] cannot extract productId from URL:", url)
    return None


# ---------------------------------------------------------------------------
# Daily refresh
# ---------------------------------------------------------------------------
def refresh_dirk_daily():
    """
    Daily refresh for Dirk:

    For all URLs in the DB:
      - Crawl the URL and extract: sku, regular_price, current_price, valid_from, valid_to
      - If invalid: availability -> False 
      - If (curr, reg, vf, vt) unchanged -> skip
      - If changed -> upsert updated fields
    """
    supabase = get_supabase()

    resp = supabase.table("dirk").select(
        "url, sku, regular_price, current_price, valid_from, valid_to, availability"
    ).execute()

    rows = resp.data or []
    if not rows:
        print("[dirk_daily] No existing Dirk products in DB, nothing to refresh.")
        return

    print(f"[dirk_daily] Found {len(rows)} Dirk products to refresh.")
        
    fresh_products = fetch_all_dirk_products()
    print(f"[INFO] Fresh products fetched: {len(fresh_products)}")

    fresh_by_pid: Dict[int, Dict[str, Any]] = {
        p["sku"]: p for p in fresh_products if p.get("sku") is not None
    }

    updates: List[Dict[str, Any]] = []

    for row in rows:
        pid = row.get("sku")
        url = row.get("url")
        if pid is None or url is None:
            continue

        old_cp = normalize_price(row.get("current_price"))
        old_rp = normalize_price(row.get("regular_price"))
        old_vf = normalize_date(row.get("valid_from"))
        old_vt = normalize_date(row.get("valid_to"))

        fresh = fresh_by_pid.get(pid)

        # -------------------------------------------------------------------
        # 1)  can't find productId in GraphQL -> unavailable
        # -------------------------------------------------------------------
        if fresh is None:
            if row.get("availability") is not False:
                updates.append(
                    {
                        "url": url,
                        "availability": False,
                    }
                )
            continue


        new_cp = normalize_price(fresh.get("current_price"))
        new_rp = normalize_price(fresh.get("regular_price"))
        new_vf = normalize_date(fresh.get("valid_from"))
        new_vt = normalize_date(fresh.get("valid_to"))

        # -------------------------------------------------------------------
        # 2) no change, skip
        # -------------------------------------------------------------------
        if (
            new_cp == old_cp
            and new_rp == old_rp
            and new_vf == old_vf
            and new_vt == old_vt
            and row.get("availability") is True
        ):
            continue

        # -------------------------------------------------------------------
        # 3) have change, update
        # -------------------------------------------------------------------
        update_row = { 
            "sku": pid,
            "current_price": fresh.get("current_price"),
            "regular_price": fresh.get("regular_price"),
            "valid_from": fresh.get("valid_from"),
            "valid_to": fresh.get("valid_to"),
            "availability": True,
        }

        updates.append(update_row)

    if not updates:
        print("[INFO] No Dirk rows changed; nothing to update.")
        return

    print(f"[INFO] Upserting {len(updates)} updated Dirk rows to Supabase...")

    supabase.table("dirk").upsert(updates, on_conflict="sku").execute()

    print("[INFO] Dirk daily refresh done.")

     
# ---------------------------------------------------------------------------
# Weekly refresh
# ---------------------------------------------------------------------------
def build_new_dirk_map() -> Dict[str, Dict[str, Any]]:
    """
    sku (== product_id): {}  dict
    """
    products = fetch_all_dirk_products()
    new_by_sku: Dict[str, Dict[str, Any]] = {}

    for p in products:
        pid = p.get("product_id")
        if pid is None:
            continue
        sku = str(pid)  # 假设你在表里 sku 是 text
        new_by_sku[sku] = {
            "sku": sku,
            "product_name_du": p.get("product_name_du"),
            "brand": p.get("brand"),
            "image_path": p.get("image_path"),
            "unit_du": p.get("unit_du"),
            "unit_qty": p.get("unit_qty"),
            "unit_type_en": p.get("unit_type_en"),
            "regular_price": p.get("regular_price"),
            "current_price": p.get("current_price"),
            "valid_from": p.get("valid_from"),
            "valid_to": p.get("valid_to"),
        }

    return new_by_sku


def refresh_dirk_weekly():
    """
    Weekly refresh（用 sku 做 key）：

      1. GraphQL 获取全量商品 → new_by_sku
      2. Supabase 读出当前所有 Dirk 行 → old_by_sku
      3. missing_skus = old_skus - new_skus
            -> availability = False
      4. joint_skus = old_skus ∩ new_skus
            -> 按 daily refresh 逻辑比较 (curr, reg, vf, vt)，有变化再更新
      5. add_skus = new_skus - old_skus
            -> 新商品，直接插入（upsert）
    """
    supabase = get_supabase()

    resp = supabase.table("dirk").select(
        "url, sku, regular_price, current_price, valid_from, valid_to, availability"
    ).execute()

    old_rows = resp.data or []
    if not old_rows:
        print("[dirk_daily] No existing Dirk products in DB, nothing to refresh.")
        return

    old_by_sku: Dict[str, Dict[str, Any]] = {
        str(r["sku"]): r for r in old_rows if r.get("sku") is not None
    }
    old_skus = set(old_by_sku.keys())
    print(f"[DIRK WEEKLY] existing SKUs: {len(old_skus)}")

    print("[DIRK WEEKLY] Fetch all products via GraphQL...")
    new_by_sku = build_new_dirk_map()
    new_skus = set(new_by_sku.keys())
    print(f"[DIRK WEEKLY] new SKUs from GraphQL: {len(new_skus)}")

    missing_skus = old_skus - new_skus
    joint_skus = old_skus & new_skus
    add_skus = new_skus - old_skus

    print(f"[DIRK WEEKLY] missing_skus: {len(missing_skus)}")
    print(f"[DIRK WEEKLY] joint_skus:   {len(joint_skus)}")
    print(f"[DIRK WEEKLY] add_skus:     {len(add_skus)}")

    rows_to_upsert: List[Dict[str, Any]] = []

    # ----------------------------------------------------------------------
    # 1) missing_skus
    # ----------------------------------------------------------------------
    for sku in missing_skus:
        rows_to_upsert.append(
            {
                "sku": sku,
                "availability": False,
                "current_price": None,
                "regular_price": None,
                "valid_from": None,
                "valid_to": None,
            }
        )

    # ----------------------------------------------------------------------
    # 2) joint_skus:  → daily refresh 逻辑
    # ----------------------------------------------------------------------
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

    # ----------------------------------------------------------------------
    # 3) add_skus: insert
    # ----------------------------------------------------------------------
    for sku in add_skus:
        new = new_by_sku[sku]
        row = {
            "sku": sku,
            "product_name_du": new.get("product_name_du"),
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
        rows_to_upsert.append(row)

    if not rows_to_upsert:
        print("[DIRK WEEKLY] nothing to upsert.")
        return

    print(f"[DIRK WEEKLY] upserting {len(rows_to_upsert)} rows to Supabase...")

    supabase.table("dirk").upsert(rows_to_upsert, on_conflict="sku").execute()

    print("[DIRK WEEKLY] done.")
