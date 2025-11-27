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
    "User-Agent": "...",
    "Accept": "text/html,*/*",
    "Accept-Language": "nl-NL,nl;q=0.9,en;q=0.8",
    "Referer": "https://www.ah.nl/"
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

df = pd.read_csv("ah_food_products.csv").loc[:5]
print(df)

from ah_core import parse_ah_product_page, get_current_bonus_period

bonus_period = get_current_bonus_period()
print(bonus_period)

# urls = df['url'].tolist()
# products = []
# for url in urls:
#     product = parse_ah_product_page(url, bonus_period)
#     if product:
#         products.append(product)
#         print(len(products))

# product = pd.DataFrame(product)
# product.to_csv("ah_product_metrics.csv")


