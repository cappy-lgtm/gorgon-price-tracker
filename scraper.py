import requests
from bs4 import BeautifulSoup
import pandas as pd
import urllib.parse
from datetime import datetime
import time
import os

COMPETITORS = {
    "Mighty Ape": {"url": "https://www.mightyape.co.nz/mn/shop/?q=", "selector": ".price"},
    "Vagabond": {"url": "https://vagabond.co.nz/search?q=", "selector": ".price"},
    "Hobby Collective": {"url": "https://thehobbycollective.co.nz/search.php?search_query=", "selector": ".price--withoutTax"},
    "Iron Knight": {"url": "https://ironknightgaming.co.nz/search?q=", "selector": ".price-item--sale"},
    "Goblin Games": {"url": "https://goblingames.nz/search?q=", "selector": ".price__regular"},
    "Games Lab": {"url": "https://www.gameslab.co.nz/search?q=", "selector": ".product-card__price"},
    "Nova Games": {"url": "https://novagames.co.nz/search?q=", "selector": ".price-item--sale"},
    "Hobby Lords": {"url": "https://www.hobbylords.co.nz/shop/search?q=", "selector": ".price-current"},
    "Hobby Master": {"url": "https://hobbymaster.co.nz/search?q=", "selector": ".price"},
    "Bea DnD": {"url": "https://www.beadndgames.co.nz/search?q=", "selector": ".price-current"}
}

def fetch_price(query, config):
    encoded_query = urllib.parse.quote_plus(query)
    search_url = f"{config['url']}{encoded_query}"
    try:
        time.sleep(1.5) 
        headers = {"User-Agent": "Mozilla/5.0"}
        res = requests.get(search_url, headers=headers, timeout=15)
        soup = BeautifulSoup(res.text, 'html.parser')
        price_tag = soup.select_one(config['selector'])
        return price_tag.get_text(strip=True) if price_tag else "Not Found"
    except:
        return "Error"

# --- Main Script ---
print(f"--- Scrape Started at {datetime.now()} ---")

skus_df = pd.read_csv('skus.csv')
date_stamp = datetime.now().strftime("%Y-%m-%d")
new_rows = []

for _, row in skus_df.iterrows():
    search_term = f"{row['sku']} {row['name']}"
    for comp_name, config in COMPETITORS.items():
        print(f"Checking {comp_name} for: {search_term}...")
        price = fetch_price(search_term, config)
        new_rows.append({
            "Date": date_stamp, 
            "SKU": row['sku'], 
            "Product": row['name'], 
            "Competitor": comp_name, 
            "Price": price
        })

history_file = 'price_history.csv'
new_data_df = pd.DataFrame(new_rows)

if not os.path.isfile(history_file):
    print(f"!!! {history_file} not found. Creating a new one...")
    new_data_df.to_csv(history_file, index=False)
else:
    print(f"Found {history_file}. Appending {len(new_rows)} new lines...")
    new_data_df.to_csv(history_file, mode='a', header=False, index=False)

print("--- Scrape Complete! ---")