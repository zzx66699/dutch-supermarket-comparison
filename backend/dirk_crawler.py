import requests
import re
import pandas as pd
from bs4 import BeautifulSoup
from datetime import date

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

HEADERS = {
    "User-Agent": ( 
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) " 
        "AppleWebKit/537.36 (KHTML, like Gecko) " 
        "Chrome/122.0.0.0 Safari/537.36 " 
        "(compatible; dirk-scraper; +https://github.com/dirk-price)" 
        )
}

VALID_TIME_RE = re.compile(
    r"Geldig van\s+\w+\s+(\d{1,2})\s+(\w+)\s+t/m\s+\w+\s+(\d{1,2})\s+(\w+)\s+(\d{4})",
    flags=re.IGNORECASE,
)

session = requests.Session()
def get_soup(url, timeout=10):
    try:
        response = session.get(url, headers=HEADERS, timeout=timeout)
        if response.status_code != 200:
            return None
        return BeautifulSoup(response.text, "html.parser")
    except Exception as e:
        return None

def parse_product_page(url):
    soup = get_soup(url)
    if not soup:
        return None
    
    price_large_tag = soup.select_one(".price-large")
    price_small_tag = soup.select_one(".price-small")
    if not price_large_tag: 
        return None
    elif price_small_tag:
        current_price = f"{price_large_tag.get_text(strip=True)}.{price_small_tag.get_text(strip=True)}"  
    else: 
        current_price = f"0.{price_large_tag.get_text(strip=True)}"

    h1_tag = soup.find("h1")
    unit_tag = soup.find("p", class_="subtitle")
    regular_price_tag = soup.select_one(".regular-price span") 
    valid_time_tag = soup.select_one(".offer-runtime")

    def text(tag):
        return tag.get_text(strip=True) if tag else None
    product_name_du = text(h1_tag)
    unit_du = text(unit_tag)
    regular_price = text(regular_price_tag)
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


df_urls = pd.read_csv("dirk_product_urls.csv")
urls = df_urls["url"].tolist()

products = []
# count = 1

for url in urls:
    product = parse_product_page(url)
    # we will add None in the list if soup is None or price_large_tag is None. None will become empty row in df, so we should remove them.
    if product:
        products.append(product)
        # count += 1
        # print(f"{count} / {len(urls)}")
        
df = pd.DataFrame(products)

df['supermarket'] = "dirk"

df.to_csv("dirk_prices.csv", index=False)