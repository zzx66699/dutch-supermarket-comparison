import xml.etree.ElementTree as ET
import csv

INPUT_FILE = "sitemap-product-0 (1).xml"
OUTPUT_CSV = "hoogvliet_product_urls.csv"

# XML namespace used in sitemap
NS = {"ns": "http://www.sitemaps.org/schemas/sitemap/0.9"}

tree = ET.parse(INPUT_FILE)
root = tree.getroot()

urls = []

# find all <loc> elements
for loc in root.findall(".//ns:loc", NS):
    text = loc.text.strip()
    if text.startswith("https://www.hoogvliet.com/product/"):
        urls.append(text)

# write to CSV
with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(["url"])
    for u in urls:
        writer.writerow([u])

print(f"Found {len(urls)} product URLs.")
print(f"Saved to {OUTPUT_CSV}")
