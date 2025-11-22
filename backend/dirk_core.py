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

# ---------------------------------------------------------------------------
# Basic constants
# ---------------------------------------------------------------------------

SUPERMARKET = "Dirk"

BASE_URL = "https://www.dirk.nl"

FOOD_PREFIXES = [
    "https://www.dirk.nl/boodschappen/aardappelen-groente-fruit",
    "https://www.dirk.nl/boodschappen/vlees-vis",
    "https://www.dirk.nl/boodschappen/brood-beleg-koek",
    "https://www.dirk.nl/boodschappen/zuivel-kaas",
    "https://www.dirk.nl/boodschappen/dranken-sap-koffie-thee",
    "https://www.dirk.nl/boodschappen/voorraadkast",
    "https://www.dirk.nl/boodschappen/maaltijden-salades-tapas",
    "https://www.dirk.nl/boodschappen/diepvries",
    "https://www.dirk.nl/boodschappen/snacks-snoep",
]


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


SITEMAP_URL = "https://www.dirk.nl/products-sitemap.xml"

# parse the "Geldig van vrijdag 14 november t/m zondag 16 november 2025"
VALID_TIME_RE = re.compile(
    r"Geldig van\s+\w+\s+(\d{1,2})\s+(\w+)\s+t/m\s+\w+\s+(\d{1,2})\s+(\w+)\s+(\d{4})",
    flags=re.IGNORECASE,
)

# HTTP headers: pretend to be a normal browser + add an ethical scraper tag
HEADERS = {
    # The first 3 rows make your request look like it’s coming from a real Chrome browser on macOS. Prevents the website from rejecting your request as “bot traffic.”
    "User-Agent": ( 
        # Mozilla/5.0 : A legacy compatibility token that all modern browsers include. Max OS: operating system
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) " 
        # Identifies the browser engine as WebKit.
        "AppleWebKit/537.36 (KHTML, like Gecko) " 
        # Specifies the browser version as Chrome 122. 
        "Chrome/122.0.0.0 Safari/537.36 " 
        # Custom section — declares this as a “Chrome-compatible scraper” and includes your project URL for transparency. This is considered ethical scraping, because you’re being honest about what’s making the request.
        "(compatible; dirk-scraper; +https://github.com/dirk-price)" 
        )
}

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


# ---------------------------------------------------------------------------
# URL crawler
# ---------------------------------------------------------------------------
def is_food_product_url(url: str) -> bool:
    """
    if the URL starts with the food-prefix, then it is a food URL. 
    """
    if not any(url.startswith(prefix) for prefix in FOOD_PREFIXES):
        return False
    last_seg = url.rstrip("/").split("/")[-1]
    return last_seg.isdigit()


def crawl_urls(_category_urls=None):
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

        # Only keep food-related product URLs
        product_urls = [url for url in all_urls if is_food_product_url(url)]

        return sorted(product_urls)

    except Exception:
        print("Error parsing sitemap:")
        return []


# ---------------------------------------------------------------------------
# Product page parsing
# ---------------------------------------------------------------------------

def text(tag):
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
    
    # Parse the price first because it can quickly skip the invalid product page.
    price_large_tag = soup.select_one(".price-large")
    price_small_tag = soup.select_one(".price-small")

    if not price_large_tag: 
        # No price → invalid product. Example: https://www.dirk.nl/boodschappen/aardappelen-groente-fruit/aardappelen/zoete%20aardappelen/25560, the product or promotion you were looking for is currently unavailable.
        return None
    elif price_small_tag:
        # e.g. "1" + "29" → "1.29"
        current_price = f"{price_large_tag.get_text(strip=True)}.{price_small_tag.get_text(strip=True)}"  
    else: 
        # only cents
        current_price = f"0.{price_large_tag.get_text(strip=True)}"


    h1_tag = soup.find("h1")
    product_name_du = text(h1_tag)

    unit_tag = soup.find("p", class_="subtitle")
    unit_du = text(unit_tag)

    #Find an element "span" nested inside an element with class "regular-price"
    regular_price_tag = soup.select_one(".regular-price span") 
    regular_price = text(regular_price_tag)

    # e.g. "Geldig van woensdag 5 november t/m dinsdag 11 november 2025"
    valid_time_tag = soup.select_one(".offer-runtime")
    valid_time = text(valid_time_tag)
    valid_from = valid_to = None
    if valid_time:
        m = VALID_TIME_RE.search(valid_time)
        if m:
            d1, m1, d2, m2, year = m.groups()
            year = int(year)
            m1 = MONTHS_NL.get(m1.lower())
            m2 = MONTHS_NL.get(m2.lower())
            if m1 and m2:
                valid_from = date(year, m1, int(d1))
                valid_to   = date(year, m2, int(d2))
    
    return {
    "url": url,
    "product_name_du": product_name_du,
    "unit_du": unit_du,
    "regular_price": regular_price,
    "current_price": current_price,
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
# Lightweight interface for refresh
# ---------------------------------------------------------------------------
def fetch_price_snapshot(url: str):
    """
    Thin wrapper for refresh scripts.

    Returns only fields needed for price refresh:
      - current_price
      - regular_price
      - valid_from
      - valid_to

    Returns None if the product page cannot be parsed.
    """
    soup = get_soup(url)
    if soup is None:
        return None
    
    price_large_tag = soup.select_one(".price-large")
    price_small_tag = soup.select_one(".price-small")

    if not price_large_tag: 
        return None
    elif price_small_tag:
        current_price = f"{price_large_tag.get_text(strip=True)}.{price_small_tag.get_text(strip=True)}"  
    else: 
        current_price = f"0.{price_large_tag.get_text(strip=True)}"
    
    regular_price_tag = soup.select_one(".regular-price span") 
    regular_price = text(regular_price_tag)

    valid_time_tag = soup.select_one(".offer-runtime")
    valid_time = text(valid_time_tag)
    valid_from = valid_to = None
    if valid_time:
        m = VALID_TIME_RE.search(valid_time)
        if m:
            d1, m1, d2, m2, year = m.groups()
            year = int(year)
            m1 = MONTHS_NL.get(m1.lower())
            m2 = MONTHS_NL.get(m2.lower())
            if m1 and m2:
                valid_from = date(year, m1, int(d1))
                valid_to   = date(year, m2, int(d2))
    
    return {
    "regular_price": regular_price,
    "current_price": current_price,
    "valid_from": valid_from,
    "valid_to": valid_to,
    }

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