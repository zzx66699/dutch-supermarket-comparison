from __future__ import annotations
import requests
import pandas as pd


import re
import time
from datetime import date

import pandas as pd
import requests
from bs4 import BeautifulSoup
from deep_translator import GoogleTranslator
from datetime import date, datetime
from typing import List, Dict, Any

from supabase_utils import get_supabase, sanitize_rows




# ---------------------------------------------------------------------------
# Basic constants
# ---------------------------------------------------------------------------


BASE_URL = "https://www.hoogvliet.com/"

MONTHS_NL = {
    "januari": 1,
    "februari": 2,
    "maart": 3,
    "april": 4,
    "mei": 5,
    "juni": 6,
    "juli": 7,
    "augustus": 8,
    "september": 9,
    "oktober": 10,
    "november": 11,
    "december": 12,
}
  

# ---------------------------------------------------------------------------
# parse helper
# ---------------------------------------------------------------------------

def parse_unit_from_attributes(attributes):
    base_unit = None
    ratio = None
    for attr in attributes:
        name = attr.get("name")
        values = attr.get("values") or []
        if not values:
            continue
        if name == "BaseUnit":
            base_unit = values[0]
        elif name == "RatioBasePackingUnit":
            try:
                ratio = float(values[0])
            except ValueError:
                ratio = None
    return base_unit, ratio


def format_unit(base_unit, ratio):
    """
    Turn (base_unit, ratio) into a human-readable unit, e.g. '750 gram', '1 stuk'.
    """
    if base_unit is None and ratio is None:
        return None
    if ratio is None:
        return base_unit
    # ratio is float -> print nicely
    if float(ratio).is_integer():
        ratio_str = str(int(ratio))
    else:
        ratio_str = str(ratio)
    return f"{ratio_str} {base_unit}"


# ---------------------------------------------------------------------------
# Tweakwise API: Fetch the sku of all the products
# ---------------------------------------------------------------------------

# Get the API request URL from network -> fetch/xhr -> filter by tn_ps 
# It returns a list of products on that page
SEARCH_URL = "https://navigator-group1.tweakwise.com/navigation/ed681b01" 


HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "X-Requested-With": "XMLHttpRequest",
}


TOP_CATEGORY_CIDS = [
    "999999-100",     # Aardappelen, groente, fruit
    "999999-200",     # Vlees, vis, vegetarisch
    "999999-300",     # Kaas, vleeswaren, tapas
    "999999-400",     # Verse maaltijden, salades
    "999999-500",     # Zuivel, plantaardig, eieren
    "999999-600",     # Diepvries
    "999999-700",     # Brood
    "999999-800",     # Ontbijtgranen, broodbeleg, tussendoor
    "999999-900",     # Frisdrank, sappen
    "999999-1900",    # Koffie, thee
    "999999-1000",    # Bier, wijn, alcoholvrij
    "999999-1100",    # Chips, zoutjes, noten
    "999999-1200",    # Koek, chocolade, snoep, zelf bakken
    "999999-1300",    # Internationale keuken, pasta, rijst
    "999999-1400",    # Soepen, conserven, sauzen, kruiden
    "999999-1500",    # Huishoud, non-food
    "999999-1800",    # Huisdier
    "999999-1600",    # Gezondheid, cosmetica
    "999999-2000",    # Baby, kind
    "999999-1700",    # Bewuste voeding
    "999999-100225",  # Tijdelijk assortiment
]


def fetch_category_items(tn_cid: str, page_size: int = 16):
    """
    Start from page 1 of a given category (tn_cid).
    Call the hidden API with those query parameters.
    data = r.json() parses the JSON.
    items = data["items"] gives the products on this page.
    """
    all_items = []
    page = 1

    while True:
        params = {
            "tn_q": "",
            "tn_p": page,           
            "tn_ps": page_size,
            "tn_sort": "Relevantie",
            "tn_cid": tn_cid,
            "t": "json",
        }

        r = requests.get(SEARCH_URL, headers=HEADERS, params=params, timeout=10)
        r.raise_for_status()

        # "data:" {
        #   "items": [...],
        #   "properties": {...},
        #   "facets": [...]
        # }
        data = r.json()  

        items = data["items"]
        props = data["properties"]
        nrof_pages = props.get("nrofpages", 1)

        print(f"page {page}: {len(items)} items, total pages = {nrof_pages}")

        if not items:
            break

        # "items": [
        #     {
        #         "itemno": "727444000",
        #         "title": "AH Bolletjes wit 10 stuks",
        #         "price": "1.85",
        #         "url": "/product/727444000/bolletjes-wit-10-stuks",
        #         "attributes": [
        #             {"name": "BaseUnit", "values": ["stuk"]},
        #             {"name": "RatioBasePackingUnit", "values": ["10"]}
        #         ]
        #     },

        #     {
        #         "itemno": "727446000",
        #         "title": "AH Puntjes bruin 8 stuks",
        #         "price": "2.10",
        #         "url": "/product/727446000/puntjes-bruin-8-stuks",
        #         "attributes": [
        #             {"name": "BaseUnit", "values": ["stuk"]},
        #             {"name": "RatioBasePackingUnit", "values": ["8"]}
        #         ]
        #     }
        # ]
        for it in items:
            base_unit, ratio = parse_unit_from_attributes(it.get("attributes", []))
            all_items.append(
                {
                    "sku": it["itemno"],
                    "brand": it.get("brand"),
                    "title": it["title"],
                    "price": it["price"],
                    "url": it["url"],
                    "base_unit": base_unit,
                    "ratio": ratio,
                }
            )

        if page >= nrof_pages:
            break

        page += 1

    return all_items


def fetch_all_skus():
    all_items_by_sku = {}

    for cid in TOP_CATEGORY_CIDS:
        print(f"\n=== Fetching category {cid} ===")
        # "items": [
            # {
            # "sku": it["itemno"],
            # "title": it["title"],
            # "price": it["price"],
            # "url": it["url"],
            # "base_unit": base_unit,
            # "ratio": ratio,
            # },

            # {
            # "sku": it["itemno"],
            # "title": it["title"],
            # "price": it["price"],
            # "url": it["url"],
            # "base_unit": base_unit,
            # "ratio": ratio,
            # },
        # ]
        items = fetch_category_items(cid)
        print(f"  category {cid}: {len(items)} items")
        for it in items:
            # "all_items_by_sku"= [
            #     "727444000": {
            #         "sku": "727444000",
            #         "title": "Bolletjes wit",
            #         "price": "1.85",
            #         "url": "/product/xxx",
            #         "base_unit": "stuk",
            #         "ratio": "10"
            #     },

            #     "235940580": {
            #     }
            # ]
            all_items_by_sku[it["sku"]] = it   # dedupe by sku
        time.sleep(0.2)  

    # list(all_items_by_sku.values()) = [
    #     {"sku": "111", "title": "Milk 1L", "price": "1.25", "url": "/milk", "base_unit": "l", "ratio": "1"},
    #     {"sku": "222", "title": "Bread",   "price": "2.00", "url": "/bread", "base_unit": "stuk", "ratio": "1"},
    #     {"sku": "333", "title": "Chocolate","price": "1.50", "url": "/choco", "base_unit": "stuk", "ratio": "1"}
    # ]
    return list(all_items_by_sku.values())


# ---------------------------------------------------------------------------
# Intershop "products by SKU" API
# ---------------------------------------------------------------------------

# Get the API request URL from network -> fetch/xhr -> productprocessTwProduct
# It returns the details of the product
API_URL = (
    "https://www.hoogvliet.com/INTERSHOP/web/WFS/"
    "org-webshop-Site/nl_NL/-/EUR/ProcessTWProducts-GetTWProductsBySkus"
)

HEADERS_PRODUCTS = {
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "X-Requested-With": "XMLHttpRequest",
    "User-Agent": "Mozilla/5.0",
}


def fetch_products_by_skus(skus):
    """
    Call the Intershop API for a *batch* of SKUs.
    Returns a list/dict of product records.
    """
    params = {
        "products": ",".join(skus)
    }
    resp = requests.post(API_URL, headers=HEADERS_PRODUCTS,
                         params=params, timeout=10)
    resp.raise_for_status()
    data = resp.json()

    # If it's just a list: return data
    # If it's {"products": [...]}, return data["products"]
    if isinstance(data, dict) and "products" in data:
        return data["products"]
    return data


def chunked(iterable, n):
    """Yield successive n-sized chunks from a list."""
    for i in range(len(iterable)):
        if i % n == 0:
            yield iterable[i:i + n]


def build_price_map(all_items, batch_size: int = 80):
    """
    all_items: list from fetch_all_skus()
    Returns: dict[sku] -> {"regular_price": ..., "current_price": ...}
    """
    price_map = {}
    all_skus = [it["sku"] for it in all_items]

    for chunk in chunked(all_skus, batch_size):
        print(f"Fetching prices for batch of {len(chunk)} SKUs...")
        products = fetch_products_by_skus(chunk)

        for p in products:
            sku = p.get("sku") or p.get("itemno")
            if not sku:
                continue

            list_price = p.get("listPrice")
            discounted = p.get("discountedPrice")

            # If there is no discount, discountedPrice is often equal to / or missing;
            # we treat listPrice as both regular + current.
            current = discounted if discounted not in (None, "", 0, "0") else list_price

            price_map[sku] = {
                "regular_price": list_price,
                "current_price": current,
            }

    return price_map



# ---------------------------------------------------------------------------
# Fetch the details for all the skus
# ---------------------------------------------------------------------------
def fetch_all_products_with_prices():
    # 1. Get all products with sku + title + unit info from Tweakwise
    base_items = fetch_all_skus()

    # 2. Get pricing info per sku from Intershop
    price_map = build_price_map(base_items)

    # 3. Merge into final structure
    final_products = []

    for it in base_items:
        sku = it["sku"]
        price_info = price_map.get(sku, {})

        unit_str = format_unit(it.get("base_unit"), it.get("ratio"))

        final_products.append(
            {
                "url": it["url"],
                "sku": sku,
                "brand": it["brand"],
                "product_name_du": it["title"],
                "unit_du": unit_str,
                "regular_price": price_info.get("regular_price"),
                "current_price": price_info.get("current_price"),
            }
        )

    return final_products


# ---------------------------------------------------------------------------
# HTTP / HTML helpers
# ---------------------------------------------------------------------------
session = requests.Session()

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


# ---------------------------------------------------------------------------
# Product page parsing
# ---------------------------------------------------------------------------
def get_text(tag):
    """
    # Extract the text from the BeautifulSoup object and strip extra whitespace
    """ 
    return tag.get_text(strip=True) if tag else None


def parse_product_page(url):
    """
    Parse a Dirk product page and return a dict with raw fields.

    Returns None if the page cannot be parsed (soup is None) or invalid product (no price).
    """
    soup = get_soup(url)
    if soup is None:
        return None
    
    valid_from = valid_to = None
      
    # e.g. "Aanbieding is geldig van 19 november t/m 25 november"
    valid_time_tag = soup.find("h3", class_="pdp-date-range")
    valid_time = get_text(valid_time_tag)
    if valid_time:
        m = re.search(r"van\s+(\d+)\s+([a-zA-Z]+)\s+t/m\s+(\d+)\s+([a-zA-Z]+)", valid_time)
        if m:
            d1, m1, d2, m2 = m.groups()

            year = date.today().year

            valid_from = date(year, MONTHS_NL[m1.lower()], int(d1))
            valid_to   = date(year, MONTHS_NL[m2.lower()], int(d2))    
    
    return {
    "url": url,
    "valid_from": valid_from,
    "valid_to": valid_to,
    }


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
    if unit_type in ("ml","milliliter"):
        return unit_qty / 1000.0, "l"
    if unit_type == "cl":
        return unit_qty / 100.0, "l"
    if unit_type == "l":
        return unit_qty, "l"    
    
    return unit_qty, "stuk"


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


def build_price_map_for_skus(all_skus, batch_size: int = 80):
    """
    build a dict list to use build_price_map
    build_price_map input format: [ {"sku": "123"}, {"sku": "456"}, {"sku": "789"}, ... ]
    data format from supabase: ["123", "456", "789", ...]
    so we build a [ {"sku": "123"}, {"sku": "456"}, {"sku": "789"}, ... ] using dummy_items
    """
    dummy_items = [{"sku": s} for s in all_skus]
    return build_price_map(dummy_items, batch_size=batch_size)


# ---------------------------------------------------------------------------
# Daily refresh
# ---------------------------------------------------------------------------
def refresh_hoogvliet_daily():
    """
    Daily refresh the price and sale period for exsiting URL
      - Get the data from Supabase 
      - Using Intershop API to regular_price / current_price
        - If regular_price is None -> availabilty = False
        - If regular_price and current_price are unchanged -> skip
        - regular_price and current_price change 
            - If regular_price = current_price -> valid_to & valid_from = Null
            - If regular_price != current_pricecrawl, crawling HTML to update valid_from / valid_to
        """
    # -------------------------------------------------------------------
    # 1. Fetch data from supabase 
    # -------------------------------------------------------------------
    supabase = get_supabase()
    resp = supabase.table("hoogvliet").select(
        "url, sku, regular_price, current_price, availability, valid_from, valid_to"
    ).execute()
    # rows = [
    #     {
    #         "url": "https://hoogvliet.com/product/a",
    #         "sku": "98728000",
    #         "regular_price": 1.99,
    #         "current_price": 1.49,
    #         "availability": True,
    #         "valid_from": "2025-02-01",
    #         "valid_to": "2025-02-07",
    #     },
    #     {
    #         "url": "https://hoogvliet.com/product/b",
    #         "sku": "12345000",
    #         "regular_price": 2.49,
    #         "current_price": 2.49,
    #         "availability": True,
    #         "valid_from": None,
    #         "valid_to": None,
    #     },
    # ]
    rows = resp.data or []
    print(f"[INFO] found {len(rows)} existing Hoogvliet products in DB")
    # -------------------------------------------------------------------
    # 2. Fetch the exsiting skus and store them into a list
    # -------------------------------------------------------------------
    # all_skus = ["98728000","12345000" ]
    all_skus = [r["sku"] for r in rows]  
    print(f"[INFO] refreshing prices for {len(all_skus)} SKUs via Intershop API...")


    # -------------------------------------------------------------------
    # 3. Fetch new skus
    # -------------------------------------------------------------------
    fresh_products = fetch_all_skus()
    
    fresh_by_pid: Dict[str, Dict[str, Any]] = {
        str(p.get("sku")): p for p in fresh_products if p.get("sku") is not None
    }

    # -------------------------------------------------------------------
    # 4. Compare the price and make the change
    # -------------------------------------------------------------------

    updates = []

    for row in rows:
        url = row.get("url")
        sku = str(row.get("sku"))

        old_cp = normalize_price(row.get("current_price"))
        old_rp = normalize_price(row.get("regular_price"))
        old_vf = normalize_date(row.get("valid_from"))
        old_vt = normalize_date(row.get("valid_to"))

        fresh = fresh_by_pid.get(sku)
        # fresh_time may be None from html parsing
        fresh_time = parse_product_page(url) or {}
        # -------------------------------------------------------------------
        # 1) product is invalid
        # -------------------------------------------------------------------
        if fresh is None:
            updates.append(
                {
                    "sku": sku,
                    "availability": False,
                }
            )
            continue
        
        # -------------------------------------------------------------------
        # 2) No change, skip
        # -------------------------------------------------------------------
        new_cp = normalize_price(fresh.get("current_price"))
        new_rp = normalize_price(fresh.get("regular_price"))
        new_vf = normalize_date(fresh_time.get("valid_from"))
        new_vt = normalize_date(fresh_time.get("valid_to"))

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
            "sku": sku,
            "current_price": fresh.get("current_price"),
            "regular_price": fresh.get("regular_price"),
            "valid_from": fresh_time.get("valid_from"),
            "valid_to": fresh_time.get("valid_to"),
            "availability": True,
        }

        updates.append(update_row)


    if not updates:
        print("[INFO] nothing changed, skip upsert.")
        return

    print(f"[INFO] Upserting {len(updates)} hoogvliet rows to supabase")

    supabase.table("hoogvliet").upsert(updates, on_conflict="sku").execute()

    print("[INFO] Hoogvliet daily refresh done.")


 

# ---------------------------------------------------------------------------
# Weekly refresh
# ---------------------------------------------------------------------------
def refresh_hoogvliet_weekly():
    """
    - Fetch full snapshot from Tweakwise + Intershop APIs -> new_products[]
    - Load all existing rows from Supabase -> old_rows[]
    - Compare SKU sets:
           missing_skus = old_skus - new_skus   -> mark availability = false
           add_skus     = new_skus - old_skus   -> new products, full insert
           joint_skus   = old_skus ∩ new_skus   -> update price + promotion logic
    """
    supabase = get_supabase()

    # 1) Fetch full new snapshot
    print("[INFO] Fetching full Hoogvliet snapshot via APIs...")
    new_products = fetch_all_products_with_prices()
    print(f"[INFO] Snapshot returned {len(new_products)} products")

    # SKU → product mapping
    new_by_sku = {p["sku"]: p for p in new_products}
    new_skus = set(new_by_sku.keys())

    # 2) Load old rows from DB
    resp = supabase.table("hoogvliet").select(
        "url, sku, product_name_du, unit_du, "
        "regular_price, current_price, availability, "
        "valid_from, valid_to"
    ).execute()

    old_rows = resp.data or []
    print(f"[INFO] Found {len(old_rows)} existing DB rows")

    old_by_sku = {r["sku"]: r for r in old_rows if r.get("sku")}
    old_skus = set(old_by_sku.keys())

    # 3) Set comparisons
    missing_skus = old_skus - new_skus
    add_skus     = new_skus - old_skus
    joint_skus   = old_skus & new_skus

    rows_to_upsert = []

    # 3.1 Missing = down / unavailable
    for sku in missing_skus:
        old = old_by_sku[sku]
        rows_to_upsert.append({
            "url": old["url"],
            "sku": sku,
            "availability": False,
            "regular_price": None,
            "current_price": None,
            "valid_from": None,
            "valid_to": None,
        })

    # 3.2 Joint = update prices + promotion period
    for sku in joint_skus:
        old = old_by_sku[sku]
        new = new_by_sku[sku]

        old_reg = str(old.get("regular_price"))
        old_cur = str(old.get("current_price"))
        new_reg = str(new.get("regular_price"))
        new_cur = str(new.get("current_price"))

        if old_reg == new_reg and old_cur == new_cur:
            continue  # No change, skip

        valid_from = None
        valid_to = None

        # Promotion case
        if new_reg != new_cur:
            full_url = old["url"]
            if full_url.startswith("/"):
                full_url = BASE_URL.rstrip("/") + full_url

            period = parse_product_page(full_url)
            if period:
                valid_from = period["valid_from"]
                valid_to = period["valid_to"]

        rows_to_upsert.append({
            "url": old["url"],
            "sku": sku,
            "availability": True,
            "regular_price": new["regular_price"],
            "current_price": new["current_price"],
            "valid_from": valid_from,
            "valid_to": valid_to,
        })

    # 3.3 Add = new products
    for sku in add_skus:
        p = new_by_sku[sku]

        reg = p.get("regular_price")
        cur = p.get("current_price")

        valid_from = None
        valid_to = None

        if str(reg) != str(cur):
            full_url = p["url"]
            if full_url.startswith("/"):
                full_url = BASE_URL.rstrip("/") + full_url

            period = parse_product_page(full_url)
            if period:
                valid_from = period["valid_from"]
                valid_to = period["valid_to"]
        
        product_name_du = p.get("product_name_du")
        unit_du = p.get("unit_du")

        product_name_en = translate_cached(product_name_du) if product_name_du else None

        unit_qty = None
        unit_type_en = None
        if unit_du:
            unit_qty, unit_type_en = parse_unit(unit_du)

        rows_to_upsert.append(
            {
                "sku": sku,
                "url": p.get("url"),
                "product_name_du": product_name_du,
                "product_name_en": product_name_en,  
                "unit_du": unit_du,
                "unit_qty": unit_qty,                
                "unit_type_en": unit_type_en,        
                "regular_price": reg,
                "current_price": cur,
                "valid_from": valid_from,
                "valid_to": valid_to,
                "availability": True,
            }
        )

    print(f"[INFO] Total rows to upsert: {len(updates)}")

    # 4) Upsert to DB
    if rows_to_upsert:
        safe_rows = sanitize_rows(rows_to_upsert)
        supabase.table("hoogvliet").upsert(safe_rows, on_conflict="sku").execute()

    print("[INFO] Hoogvliet weekly refresh done.")
