import requests
import re
import time
import math
import pandas as pd
from bs4 import BeautifulSoup
from datetime import date
from urllib.parse import urljoin, urlparse
from deep_translator import GoogleTranslator 
from supabase import create_client


BASE_URL = "https://www.dirk.nl"

# constants use ALL CAPS
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

CATEGORY_URLS = [
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

CATEGORY_KEYWORDS = [
    "aardappelen-groente-fruit",
    "vlees-vis",
    "brood-beleg-koek",
    "zuivel-kaas",
    "dranken-sap-koffie-thee",
    "voorraadkast",
    "maaltijden-salades-tapas",
    "diepvries",
    "snacks-snoep",
]


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

# Takes a URL string and returns a BeautifulSoup object parsed from that webpage.
session = requests.Session()

def get_soup(url, max_retries=2, timeout=15, backoff=1):
    for attempt in range(1, max_retries + 1):
        try:
            response = session.get(url, headers=HEADERS, timeout=timeout)

            if response.status_code != 200:
                return None

            return BeautifulSoup(response.text, "html.parser")

        except Exception as e:
            return None

    return None


# Check if it is a dirk.nl page
def is_same_domain(url: str) -> bool:
    # urlparse will return something like this:
    # ParseResult(scheme='https', netloc='www.dirk.nl', path='/boodschappen/fruit', params='', query='promo=true',fragment='')
    # We only need the netloc
    netloc = urlparse(url).netloc
    return "dirk.nl" in netloc

# Check if it is a food-related page
def is_food_related(url: str) -> bool:
    return any(key in url for key in CATEGORY_KEYWORDS)

# Check if it is a product page
def is_product_url(url: str) -> bool:
    path = urlparse(url).path
    if "/boodschappen/" not in path:
        return False
    # rstrip("/") is to strip the last "/" at the end of the url, like /4538/ -> it will return empty if we don't rstrip.
    last_seg = path.rstrip("/").split("/")[-1]
    # if it is digit, then it is a product page
    return last_seg.isdigit()

# Start from the product category pages. 
# It visits each page, collects all the links (hrefs), and:
    # If a link is a product page, it adds it to product_urls.
    # If it’s another internal category or pagination page, it adds it to the queue for later crawling.
# The process repeats until either all pages are visited or the max_pages limit is reached. 
def crawl_urls(category_urls):
    visited = set()
    product_urls = set()
    queue = list(category_urls)

    while queue:
        # url is the first element in the list, and this element will be deleted from the list
        url = queue.pop(0)
        # if we never visit the url before, put it in the "visited" list; we have have visited, skip.
        if url in visited:
            continue
        visited.add(url)
        
        soup = get_soup(url)
        if soup is None:
            continue

        for a in soup.find_all("a", href=True):
            # urljoin will automatically check if the url contains the domain
            full_url = urljoin(BASE_URL, a["href"])

            if not is_food_related(full_url):
                continue

            if is_product_url(full_url):
                product_urls.add(full_url)
            else:
                if full_url not in visited:
                    queue.append(full_url)

        time.sleep(0.3)
        
        print("finish the " + url)
        print(len(product_urls))

    return sorted(product_urls)

def parse_product_page(url):
    soup = get_soup(url)
    test = 1

    if soup is None:
        return None

    # Use a CSS selector to locate the element. A CSS selector is a pattern (or rule) used to select elements in an HTML page — either to style them (in CSS) or to find/extract them (in web scraping with BeautifulSoup).
    h1_tag = soup.find("h1")
    unit_tag = soup.find("p", class_="subtitle")
    regular_price_tag = soup.select_one(".regular-price span") #Find an element "span" nested inside an element with class "regular-price"
    price_large_tag = soup.select_one(".price-large")
    price_small_tag = soup.select_one(".price-small")
    valid_time_tag = soup.select_one(".offer-runtime")

    # Extract the text and strip extra whitespace
    product_name_du = h1_tag.get_text(strip=True) if h1_tag else None
    unit_du = unit_tag.get_text(strip=True) if unit_tag else None
    regular_price = regular_price_tag.get_text(strip=True) if regular_price_tag else None
    if price_large_tag: 
        if price_small_tag:
            current_price = f"{price_large_tag.get_text(strip=True)}.{price_small_tag.get_text(strip=True)}"  
        else: 
            current_price = f"0.{price_large_tag.get_text(strip=True)}"
    else: 
        # https://www.dirk.nl/boodschappen/aardappelen-groente-fruit/aardappelen/zoete%20aardappelen/25560, the product or promotion you were looking for is currently unavailable.
        current_price = None
    # valid_time is a string like "Geldig van woensdag 5 november t/m dinsdag 11 november 2025"
    valid_time = valid_time_tag.get_text(strip=True) if valid_time_tag else None 

    # Get the promotion start and end date
    valid_from = valid_to = None
    if valid_time:
        m = re.search(
            r"Geldig van\s+\w+\s+(\d{1,2})\s+(\w+)\s+t/m\s+\w+\s+(\d{1,2})\s+(\w+)\s+(\d{4})",
            valid_time,
            flags=re.IGNORECASE
        )
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


urls = crawl_urls(CATEGORY_URLS)
products = []

for url in urls:
    product = parse_product_page(url)
    if product is None:
        continue
    products.append(product)

df = pd.DataFrame(products)
df['supermarket'] = "dirk"
    
df.to_csv("dirk_prices.csv", index=False)

