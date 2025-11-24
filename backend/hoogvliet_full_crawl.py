from __future__ import annotations
import requests
import gzip
import xml.etree.ElementTree as ET


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

SUPERMARKET = "Hoogvliet"

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


gz_url = "https://www.hoogvliet.com/sitemap-product-0.xml.gz?SyndicationID=SiteMapXML"



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
        "(compatible; dirk-scraper; +https://github.com/hoogvliet-price)" 
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


def fetch_hoogvliet_urls_from_gz(url: str) -> list[str]:
    """
    Fetch a .xml.gz sitemap directly from the web
    """
    print("Downloading:", url)
    resp = requests.get(url, timeout=30, headers = HEADERS)
    if resp.status_code != 200:
        print("Failed:", resp.status_code)
        return []

    # decompress in memory
    try:
        xml_bytes = gzip.decompress(resp.content)
    except Exception as e:
        print("Gzip error:", e)
        return []

    # parse XML
    try:
        root = ET.fromstring(xml_bytes)
    except Exception as e:
        print("XML parse error:", e)
        return []

    NS = {"ns": "http://www.sitemaps.org/schemas/sitemap/0.9"}

    urls = []
    for url_tag in root.findall("ns:url", NS):
        loc = url_tag.find("ns:loc", NS)
        if loc is not None and loc.text:
            urls.append(loc.text.strip())

    return urls


# urls = fetch_hoogvliet_urls_from_gz(gz_url)

# print("Found:", len(urls))

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
    
    # Parse the price first because it can quickly skip the invalid product page.

    # <div class="price-container product-promo-price demotpd">
    #     <span class="price-euros"><span>1</span><span class="price-seperator">.</span></span>
    #     <span class="price-cents"><sup>29</sup></span>
    # </div>
    promo_div = soup.find("div", class_="product-promo-price")
    if not promo_div:
        return None
    euros_tag = promo_div.find("span", class_="price-euros")
    # <span>
    #   <font dir="auto" style="vertical-align: inherit;">
    #   <font dir="auto" style="vertical-align: inherit;">0 
    #   </font></font>
    # </span>
    euros = get_text(euros_tag) if euros_tag else "0"
    cents = get_text(promo_div.find("span", class_="price-cents"))
    euros = euros.replace(".", "").strip()
    current_price = f"{euros}.{cents}"
    
    # <div class="strikethrough democlass">
    #     <div class="kor-product-sale-price">
    #         <div class="price-baloon">
    #             <span class="kor-product-sale-price-value ws-sale-price">2.79</span>
    #         </div>
    #     </div>
    # </div>
    regular_price_tag = soup.find("span", class_="kor-product-sale-price-value")
    regular_price = get_text(regular_price_tag)  # e.g. "2.79"

    valid_from = valid_to = None

    if regular_price == current_price:
        regular_price = None
        
    else:        
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

    h1_tag = soup.find("h1")
    product_name_du = get_text(h1_tag)

    # <div class="unitPrice">
    # <div class="ratio-base-packing-unit"><span>130 gram</span></div>
    # <div class="price-per-unit">Reguliere prijs per kilo € 29.15</div>
    # </div>
    unit_div = soup.find("div", class_="unitPrice") 
    unit_du = None
    unit_price_du = None
    if unit_div:
        unit_tag = unit_div.find("div", class_="ratio-base-packing-unit")
    unit_du = get_text(unit_tag)        # -> "130 gram"
    

    
    return {
    "url": url,
    "product_name_du": product_name_du,
    "unit_du": unit_du,
    "regular_price": regular_price,
    "current_price": current_price,
    "valid_from": valid_from,
    "valid_to": valid_to,
    }

df = pd.read_csv("hoogvliet_product_urls.csv").loc[1996:]
urls = df["url"].tolist()
products = []
for url in urls:
    product = parse_product_page(url)
    products.append(product)
    print(len(products))

df = pd.DataFrame(products)
df.to_csv("hoogvliet_products_details.csv")

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
# e.g. 'unit_du': '8 \xa0stuk'
df['unit_du'] = df['unit_du'].str.replace("\xa0", " ")

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
    
    promo_div = soup.find("div", class_="product-promo-price")
    if not promo_div:
        return None
    
    euros_tag = promo_div.find("span", class_="price-euros")
    euros = get_text(euros_tag) if euros_tag else "0"
    cents = get_text(promo_div.find("span", class_="price-cents"))
    euros = euros.replace(".", "").strip()
    current_price = f"{euros}.{cents}"
    
    regular_price_tag = soup.find("span", class_="kor-product-sale-price-value")
    regular_price = get_text(regular_price_tag)  

    valid_from = valid_to = None

    if regular_price == current_price:
        regular_price = None
        
    else:        
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