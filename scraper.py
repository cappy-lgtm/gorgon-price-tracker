import requests
from bs4 import BeautifulSoup
import pandas as pd
import urllib.parse
from datetime import datetime
import time
import os

# 1. THE COMPETITOR DATABASE
# These are mapped to their search URLs and the specific "Price" tag on their site.
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
    """Encodes the SKU+Name and attempts to find the price element."""
    encoded_query = urllib.parse.quote_plus(query)
    search_url = f"{config['url']}{encoded_query}"
    
    try:
        # 1.5 second delay keeps the bot from getting blocked by NZ servers
        time.sleep(1.5) 
        headers = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"}
        res = requests.get(search_url, headers=headers, timeout=15)
        
        if res.status_code != 200:
            return "Site Error"
            
        soup = BeautifulSoup(res.text, 'html.parser')
        price_tag = soup.select_one(config['selector'])
        
        if price_tag:
            return price_tag.get_text(strip=True)
        return "Not Found"
    except Exception as e:
        return "Timeout/Error"

# 2. LOAD YOUR SKUs
if not os.path.exists('skus.csv'):
    print("Error: skus.csv not found!")
    exit(1)

skus_df = pd.read_csv('skus.csv')
date_stamp = datetime.now().strftime("%Y-%m-%d")
new_rows = []

# 3. RUN THE SCRAPE (11 SKUs x 10 Competitors = 110 checks)
for _, row in skus_df.iterrows():
    # Combining SKU and Name for the most accurate search results
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

# 4. SAVE & APPEND
history_file = 'price_history.csv'
new_data_df = pd.DataFrame(new_rows)

# If the file doesn't exist, we create it with headers.
# If it does exist, we append the 110 new rows to the bottom.
if not os.path.isfile(history_file):
    new_data_df.to_csv(history_file, index=False)
    print(f"Initialized new file: {history_file}")
else:
    new_data_df.to_csv(history_file, mode='a', header=False, index=False)
    print(f"Successfully appended {len(new_rows)} rows to {history_file}")