from __future__ import annotations

import re
import time
from collections import deque
from datetime import date

import pandas as pd
import requests
from bs4 import BeautifulSoup
from deep_translator import GoogleTranslator
from urllib.parse import urljoin, urlparse, urlunparse, parse_qs, urlencode
from datetime import date, datetime 
import requests
from bs4 import BeautifulSoup
from ah_core import get_text 
import re
from typing import Optional, Tuple, List
# ---------------------------------------------------------------------------
# Basic constants
# ---------------------------------------------------------------------------

DUTCH_MONTHS = {
    "jan": 1, "feb": 2, "mrt": 3, "apr": 4, "mei": 5, "jun": 6,
    "jul": 7, "aug": 8, "sep": 9, "okt": 10, "nov": 11, "dec": 12,
}


SUPERMARKET = "AH"

BASE_URL = "https://www.ah.nl/"

CATEGORY_URLS = [
    "https://www.ah.nl/producten/6401/groente-aardappelen",
    "https://www.ah.nl/producten/20885/fruit-verse-sappen",
    "https://www.ah.nl/producten/1301/maaltijden-salades",
    "https://www.ah.nl/producten/9344/vlees",
    "https://www.ah.nl/producten/1651/vis",
    "https://www.ah.nl/producten/20128/vegetarisch-vegan-en-plantaardig",
    "https://www.ah.nl/producten/5481/vleeswaren",
    "https://www.ah.nl/producten/1192/kaas",
    "https://www.ah.nl/producten/1730/zuivel-eieren",
    "https://www.ah.nl/producten/1355/bakkerij",
    "https://www.ah.nl/producten/4246/glutenvrij",
    "https://www.ah.nl/producten/20824/borrel-chips-snacks",
    "https://www.ah.nl/producten/1796/pasta-rijst-wereldkeuken",
    "https://www.ah.nl/producten/6409/soepen-sauzen-kruiden-olie",
    "https://www.ah.nl/producten/20129/koek-snoep-chocolade",
    "https://www.ah.nl/producten/6405/ontbijtgranen-beleg",
    "https://www.ah.nl/producten/2457/tussendoortjes",
    "https://www.ah.nl/producten/5881/diepvries",
    "https://www.ah.nl/producten/1043/koffie-thee",
    "https://www.ah.nl/producten/20130/frisdrank-sappen-water",
    "https://www.ah.nl/producten/6406/bier-wijn-aperitieven",
    "https://www.ah.nl/producten/11717/gezondheid-en-sport"
]


HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "nl-NL,nl;q=0.9,en;q=0.8",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
    "Upgrade-Insecure-Requests": "1",
}


session = requests.Session()


# ---------------------------------------------------------------------------
# HTTP / HTML helpers
# ---------------------------------------------------------------------------

def get_soup(url, timeout=10):
    try:
        response = session.get(url, headers=HEADERS, timeout=timeout)
        if response.status_code != 200:
            return None
        return BeautifulSoup(response.text, "html.parser")
    except Exception:
        return None

    
def is_same_domain(url: str) -> bool:
    """
    True if URL belongs to dirk.nl
    """
    netloc = urlparse(url).netloc
    return "ah.nl" in netloc


def is_product_url_path(path: str) -> bool:
    # AH product URLs: /producten/product/wi12345/...
    return path.startswith("/producten/product/")


def normalize_ah_url(full_url: str) -> tuple[str, str] | None:
    """
    Normalize AH /producten/ URLs to reduce BFS explosion.

    Rules:
    - Only keep URLs inside the /producten/ subtree; ignore everything else.
    - For product pages:
        * Remove all query parameters and fragments.
        * Return a clean, canonical product URL.
    - For category/listing pages:
        * Keep ONLY the `page` parameter; drop all filters/sort options.
        * Drop page=0 or page=1 (treat them as the first page).
    """
    # Parsed(
    # scheme='https',
    # netloc='www.ah.nl',
    # path='/producten/product/wi41080',
    # params='',
    # query='sort=price&page=3&brand=ah'
    # fragment='section1'
    # )
    parsed = urlparse(full_url)

    # Extract and normalize the path (remove trailing slash).
    path = parsed.path.rstrip("/") or "/"

    # Only crawl URLs under /producten.
    # Example discarded: /allerhande/recepten/soep
    if not path.startswith("/producten"):
        return None

    # Apply normalized path (no trailing slash).
    parsed = parsed._replace(path=path)

    # If this URL is a product page, return a clean version (no ?query or #fragment).
    if is_product_url_path(path):
        clean = parsed._replace(query="", fragment="")
        norm_url = urlunparse(clean) # Get an url from the parse
        return norm_url, path

    # For category/list pages: only preserve the `page` parameter, and keep only page > 1
    # Parse query parameters into a dict
    qs = parse_qs(parsed.query, keep_blank_values=False)
    # {
    # "page": ["3"],
    # "sort": ["price"],
    # "brand": ["ah"]
    # }
    keep: dict[str, list[str]] = {}

    page_values = qs.get("page") # ["3"]
    if page_values:
        page = page_values[0] # ["3"] → "3"

        # Treat page=0 or page=1 as the first page -> remove parameter.
        # Only keep page=2,3,4...
        if page not in ("0", "1", ""):
            keep["page"] = [page]

    # Build the new minimized query string.
    new_query = urlencode(keep, doseq=True)

    # Return normalized category/list URL.
    clean = parsed._replace(query=new_query, fragment="")
    norm_url = urlunparse(clean)
    return norm_url, path


# ---------------------------------------------------------------------------
# URL crawler
# ---------------------------------------------------------------------------

def crawl_urls(category_urls):
    """
    Breadth-first crawl (BFS) starting from Dirk category pages.
    - Start from the first url in the queue, collects all the links (hrefs) on that page:
        - If the link is a product page, then adds it to product_urls.
        - If it's another internal category or pagination page, add it to the queue for later crawling.
    - Repeats until nothing left in the queue (all pages are visited).

    Returns a sorted list of product URLs.
    """
    visited = set()
    product_urls = set()
    queue = deque(category_urls) 

    while queue:
        url = queue.popleft() 
        if url in visited:
            continue
        visited.add(url)
        
        soup = get_soup(url)
        if soup is None:
            continue
        
        for a in soup.find_all("a", href=True):
            full_url = urljoin(BASE_URL, a["href"])

            if not is_same_domain(full_url):
                continue
            
            # norm is normalized url
            res = normalize_ah_url(full_url)
            if res is None:
                continue

            # norm is the cleaned url
            norm, path = res

            if is_product_url_path(path):
                if norm not in product_urls:
                    product_urls.add(norm)
                    print(len(product_urls))
            else:
                if norm not in visited:
                    queue.append(norm)
        
        time.sleep(0.1)
    
    return sorted(product_urls)


# ---------------------------------------------------------------------------
# Soup helper
# ---------------------------------------------------------------------------
def get_text(tag):
    """Extract the text from a BeautifulSoup tag and strip extra whitespace."""
    return tag.get_text(strip=True) if tag else None


# ---------------------------------------------------------------------------
# Extract the promotion period
# ---------------------------------------------------------------------------
def _parse_price_str(s: str) -> float:
    """Normalize something like '3,99' or '3.99' into a float."""
    s = s.strip().replace("€", "").replace(",", ".")
    return float(s)


def parse_bonus_period_label(label: str, year: int | None = None) -> tuple[date, date]:
    """
    Transform the bonus period label, e.g. '17 t/m 23 nov' or '27 okt t/m 2 nov',
    into (start_date, end_date).
    """
    if year is None:
        year = date.today().year

    label = label.strip()
    left_part, right_part = [s.strip() for s in label.split("t/m")]

    # Right side: always 'day month'
    match_right = re.match(r"(\d+)\s+([a-zA-Z]+)", right_part)
    if not match_right:
        raise ValueError(f"Unexpected right part: '{right_part}'")

    end_day = int(match_right.group(1))
    end_month = DUTCH_MONTHS[match_right.group(2).lower()]

    # Left side: either 'day' or 'day month'
    match_left = re.match(r"(\d+)(?:\s+([a-zA-Z]+))?", left_part)
    if not match_left:
        raise ValueError(f"Unexpected left part: '{left_part}'")

    start_day = int(match_left.group(1))
    start_month_str = match_left.group(2)

    if start_month_str:
        start_month = DUTCH_MONTHS[start_month_str.lower()]
    else:
        start_month = end_month

    start_date = date(year, start_month, start_day)
    end_date = date(year, end_month, end_day)

    # Cross-year case: e.g. 28 dec t/m 3 jan
    if end_month < start_month:
        end_date = date(year + 1, end_month, end_day)

    return start_date, end_date


def get_current_bonus_period() -> tuple[date, date] | None:
    """
    AH doesn't show the promotion period on the product page.
    Use the Bonus page as the source of sales date.
    """
    resp = session.get("https://www.ah.nl/bonus", headers=HEADERS, timeout=10)
    if resp.status_code != 200:
        return None

    soup = BeautifulSoup(resp.text, "html.parser")
    label_tag = soup.select_one(
        '[data-testhook="period-toggle-button"] .period-toggle-button_label__rRyWQ'
    )
    if not label_tag:
        return None

    label = get_text(label_tag)  # e.g. "17 t/m 23 nov"
    return parse_bonus_period_label(label)


# ---------------------------------------------------------------------------
# Detect, extract and calculate the promotion
# ---------------------------------------------------------------------------
def _get_promo_window(lines: List[str], product_name: str) -> List[str]:
    """
    Find the index for product name.
    The promotion should be a few lines before the product name.
    e.g. '2 voor', '3.99', 'AH Roerbakgroente Italiaans', 
    """
    
    idx_name = None
    for i, ln in enumerate(lines):
        if ln == product_name:
            idx_name = i
            break

    if idx_name is None:
        # if we can't find the product name, then just return the first 50 lines
        return lines[:50]

    start = max(0, idx_name - 8)
    end = idx_name
    return lines[start:end]


def _detect_promo_type(lines: List[str], product_name: str) -> str:
    """
    Identify which pattern of promotion it is. 
    Return one of: 'multi', 'discount', 'none'
    - multi    -> '2 voor 3.99', '1+1 gratis', '2e halve prijs'
    - discount -> 'voor 0.99', '€1 korting', '10% korting'
    - none     -> no promo detected
    """
    window = _get_promo_window(lines, product_name)
    tokens = [t.lower() for t in window]

    # e.g. '2 voor'
    has_x_voor = any(re.search(r"\d+\s+voor", t) for t in tokens)

    # e.g.  '1+1', 'gratis'
    has_plus_gratis = any(re.search(r"\d+\+\d+", t) for t in tokens) and \
                      any("gratis" in t for t in tokens)
    
    # e.g. '2e', 'halve prijs'
    has_second_half = any("2e" == t for t in tokens) and \
                      any("halve prijs" in t for t in tokens)
    
    if has_x_voor or has_plus_gratis or has_second_half:
        return "multi"

    # e.g. '€1', 'korting'
    has_korting = any("korting" in t for t in tokens)
    # e.g. 'voor', '0.99'
    has_plain_voor = "voor" in tokens

    if has_korting or has_plain_voor:
        return "discount"

    return "none"


def _extract_promo_text(lines: List[str], product_name: str) -> Optional[str]:
    """
    Extract the promotion text and store it as a column in the dataset.
    """
    window = _get_promo_window(lines, product_name)
    tokens = window  

    # 1) '2 voor 3.99'
    for i, t in enumerate(tokens):
        if re.fullmatch(r"\d+\s+voor", t.lower()):
            for j in range(i + 1, min(len(tokens), i + 5)):
                if re.fullmatch(r"\d+[.,]\d{2}", tokens[j]):
                    return f"{t} {tokens[j]}"
            return t

    # 2) '1+1 gratis'
    for i, t in enumerate(tokens):
        if re.fullmatch(r"\d+\+\d+", t):
            if i + 1 < len(tokens) and "gratis" in tokens[i + 1].lower():
                return f"{t} {tokens[i + 1]}"
            return t

    # 3) '2e halve prijs'
    has_2e = None
    has_halve = None
    for tt in tokens:
        if tt.strip().lower() == "2e":
            has_2e = tt
        if "halve prijs" in tt.lower():
            has_halve = tt
    if has_2e and has_halve:
        return f"{has_2e} {has_halve}"

    return None


def compute_prices_and_promo(
    lines: List[str],
    product_name: str,   # ← add this
) -> Tuple[float, Optional[float], Optional[str]]:
    """
    Given the soup text as a list of lines, return (current_price, regular_price, promotion) according to the rules:

    For promo_type == "discount" (e.g. voor 0.99, €1 korting, 10% korting):
        current_price = closest candidate to "Voeg toe"
        regular_price = second closest if it exists (ignore the rest, so 6,17 drops out naturally)
        promotion = "{current_price:.2f}" 

    For promo_type == "multi" (e.g. 2 voor 3.99, 2e halve prijs, 1+1 gratis):
        regular_price = closest candidate to "Voeg toe" (AH's shelf price)
        Use the promo text to compute effective current_price (as we did before)
        promotion = textual promo ("2 voor 3.99", "1+1 gratis", …)

    For promo_type == "none"
        current_price = closest candidate to "Voeg toe"
        regular_price = None
    """
    promo_type = _detect_promo_type(lines, product_name)

    # find "Voeg toe" position
    # because the price is always before the "Voeg toe", and break into 3 lines. 
    # if the number is not in 3 lines, then it is not a price / not the price we want to find
    # e.g. 'Normale prijs per', 'KG', '€', '14,99', '3', '.', '69', '900 ml', 'Voeg toe'
    # we don't want '14,99' and '900 ml' as the candidate
    try:
        idx_voeg = lines.index("Voeg toe")
    except ValueError:
        idx_voeg = None


    candidates: List[Tuple[int, float]] = []

    limit = len(lines) - 2
    if idx_voeg is not None:
        limit = min(limit, idx_voeg)  

    for i in range(limit):
        # a, b, c should be in 3 lines
        a, b, c = lines[i], lines[i + 1], lines[i + 2]
        if re.fullmatch(r"\d+", a) and b in {".", ","} and re.fullmatch(r"\d{2}", c):
            price = float(f"{a}.{c}")
            if price != 0.00:
                candidates.append((i, price))

    if not candidates:
        return None, None, None

    if idx_voeg is not None:
        before = [(idx, val) for (idx, val) in candidates if idx < idx_voeg]
        if before:
            ordered = sorted(before, key=lambda x: idx_voeg - x[0])
        else:
            ordered = sorted(candidates, key=lambda x: x[0])
    else:
        ordered = sorted(candidates, key=lambda x: x[0])

    closest_prices = [val for (_, val) in ordered]

    current_price: Optional[float] = None
    regular_price: Optional[float] = None
    promotion: Optional[str] = None

    if promo_type == "discount":
        current_price = closest_prices[0]
        regular_price = closest_prices[1] if len(closest_prices) > 1 else None
        promotion = f"{current_price:.2f}"

    elif promo_type == "multi":
        regular_price = closest_prices[0]  

        promo_text = _extract_promo_text(lines, product_name)
        if promo_text is None:
            raise ValueError(
                f"[MULTI PROMO] Detected multi promo but could not extract "
                f"promo_text for product '{product_name}'"
            )

        promo = promo_text.lower()

        # 1) 'X voor Y'
        m = re.search(r"(\d+)\s+voor\s+(\d+[.,]\d{2})", promo)
        if m:
            n = int(m.group(1))
            total = _parse_price_str(m.group(2))
            current_price = total / n

        # 2) 'A+B gratis'
        elif re.search(r"\d+\+\d+", promo) and "gratis" in promo:
            m = re.search(r"(\d+)\+(\d+)", promo)
            if m:
                paid = int(m.group(1))
                free = int(m.group(2))
                current_price = regular_price * paid / (paid + free)
            else:
                current_price = regular_price

        # 3) '2e halve prijs'
        elif "halve prijs" in promo and "2e" in promo:
            current_price = regular_price * 0.75

        promotion = promo_text

    else:
        # promo_type == "None"
        # By right, if the promo_type is None, there is only 1 candicate.
        current_price = closest_prices[0]
        regular_price = None
        promotion = None

        # However, one exception is, if the promotion is "per 100 gram 2.59", we can't detect it in promo_type.
        # However, it is actually in promotion.
        # Since it has 2 prices in the candicates, we still take the closer candicate as the cp, and another candicate as the rp.
        if len(closest_prices) >= 2:
            second = closest_prices[1]
            if second > current_price + 0.01:  # 要求明显比 current 高一点
                regular_price = second
                promotion = f"{current_price:.2f}"

    return current_price, regular_price, promotion


# ---------------------------------------------------------------------------
# Product page parsing
# ---------------------------------------------------------------------------
def parse_ah_product_page(url: str, bonus_period):
    soup = get_soup(url)
    if soup is None:
        return None

    # Change the soup into text
    text = soup.get_text("\n", strip=True)
    # break into lines
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    print (lines)


    # -----------------------------
    # 1) product_name_du
    # -----------------------------
    product_name_du = None
    h1_tag = soup.find("h1")
    if h1_tag:
        product_name_du = get_text(h1_tag)
    else:
        title = soup.title.string.strip()
        suffix = " bestellen | Albert Heijn"
        if title.endswith(suffix):
            product_name_du = title[:-len(suffix)].strip()


    # -----------------------------
    # 2) unit_du
    # -----------------------------
    unit_du = None
    try:
        idx = lines.index("Inhoud en gewicht")
        # e.g. lines = 
        # 63 'Inhoud en gewicht'
        # 64 '400 Gram'
        # 65 'Portiegrootte:'
        # 66 '200 gram'
        # 67 'Aantal porties:'
        # 68 '2'
        for ln in lines[idx + 1: idx + 5]: # unit should be about one line after the "Inhoud en gewicht"; 
            if ln and not ln.endswith(":"):
                unit_du = ln
                break
    except ValueError:
        pass
    

    # -----------------------------
    # 3) current price
    # -----------------------------
    current_price, regular_price, promotion = compute_prices_and_promo(lines, product_name_du)

    # -----------------------------
    # 3) valid_from, valid_to
    # -----------------------------
    valid_from, valid_to = None, None
    if bonus_period and regular_price is not None:
        valid_from, valid_to = bonus_period

    return {
        "url": url,
        "product_name_du": product_name_du,
        "unit_du": unit_du,
        "regular_price": regular_price,
        "current_price": current_price,
        "promotion": promotion,
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