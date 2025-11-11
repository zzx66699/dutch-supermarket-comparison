import requests
import time
import pandas as pd
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
from collections import deque 

BASE_URL = "https://www.dirk.nl"

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
def get_soup(url, timeout=10):
    try:
        response = session.get(url, headers=HEADERS, timeout=timeout)
        if response.status_code != 200:
            return None
        return BeautifulSoup(response.text, "html.parser")
    except Exception as e:
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
# The process repeats until either all pages are visited.
def crawl_urls(category_urls):
    visited = set()
    product_urls = set()
    queue = deque(category_urls) # ✅ faster queue

    while queue:
        # url is the first element in the list, and this element will be deleted from the list
        url = queue.popleft() 
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

            if not is_same_domain(full_url):
                continue
            if not is_food_related(full_url):
                continue
            if is_product_url(full_url):
                product_urls.add(full_url)
            else:
                if full_url not in visited:
                    queue.append(full_url)
        
        time.sleep(0.2)
        # print("finish the " + url)
        # print(len(product_urls))
    return sorted(product_urls)

# Crawl all product URLs
urls = crawl_urls(CATEGORY_URLS)

# Save to CSV
df_urls = pd.DataFrame({"url": urls})
df_urls.to_csv("dirk_product_urls.csv", index=False)