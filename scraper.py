import requests
from bs4 import BeautifulSoup
import pandas as pd
import urllib.parse
from datetime import datetime
import time
import os

# CONFIGURATION: Selectors for each website
COMPETITORS = {
    "Mighty Ape": {"url": "https://www.mightyape.co.nz/mn/shop/?q=", "price": ".price .value", "oos": ".unavailable"},
    "Vagabond": {"url": "https://vagabond.co.nz/search.php?search_query=", "price": ".price", "oos": ".out-of-stock"},
    "Hobby Collective": {"url": "https://thehobbycollective.co.nz/search.php?search_query=", "price": ".price--withoutTax", "oos": ".out_of_stock"},
    "Iron Knight": {"url": "https://ironknightgaming.co.nz/search?q=", "price": ".price-item--sale", "oos": ".badge--sold-out"},
    "Goblin Games": {"url": "https://goblingames.nz/search?q=", "price": ".price__regular", "oos": ".product-price__sold-out"},
    "Games Lab": {"url": "https://www.gameslab.co.nz/products/search?keyword=", "price": ".product-card__price", "oos": ".product-card__out-of-stock"},
    "Nova Games": {"url": "https://novagames.co.nz/search?q=", "price": ".price-item--sale", "oos": ".badge--sold-out"},
    "Hobby Lords": {"url": "https://www.hobbylords.co.nz/shop/search?q=", "price": ".price-current", "oos": ".stock-out"},
    "Hobby Master": {"url": "https://hobbymaster.co.nz/products/search?search=", "price": ".product-price", "oos": ".out-of-stock"},
    "Bea DnD": {"url": "https://www.beadndgames.co.nz/search?q=", "price": ".price-current", "oos": ".out-of-stock"},
    "Kapiti Hobbies": {"url": "https://www.kapitihobbies.com/search?q=", "price": ".price-item--sale", "oos": ".badge--sold-out"}
}

    def fetch_price(query, config):
    encoded_query = urllib.parse.quote(query)
    search_url = f"{config['url']}{encoded_query}"
    
    try:
        time.sleep(2) # Increased delay to be safer
        headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept-Language": "en-GB,en-US;q=0.9,en;q=0.8",
            "Referer": "https://google.com"
        }
        res = requests.get(search_url, headers=headers, timeout=20)
        # If the site returns a 403 or 404, we'll know
        if res.status_code != 200:
            return f"Status {res.status_code}"
            
        soup = BeautifulSoup(res.text, 'html.parser')
        
        # 1. Find the price
        price_tag = soup.select_one(config['price'])
        if not price_tag:
            return "Not Found"
        
        price_text = price_tag.get_text(strip=True)
        
        # 2. Check if Out of Stock
        # We check the specific OOS selector AND look for common "sold out" text in the page
        oos_tag = soup.select_one(config['oos'])
        page_text = res.text.lower()
        
        if oos_tag or "sold out" in page_text or "out of stock" in page_text:
            return f"{price_text} (OOS)"
            
        return price_text
    except Exception as e:
        return "Error"

# --- MAIN EXECUTION ---
print(f"--- Scrape Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ---")

# Load your 3-column CSV
skus_df = pd.read_csv('skus.csv')
date_stamp = datetime.now().strftime("%Y-%m-%d")
new_rows = []

for _, row in skus_df.iterrows():
    # Use the specific 'search' column for the query
    search_query = row['search']
    product_name = row['name']
    sku = row['sku']
    
    for comp_name, config in COMPETITORS.items():
        print(f"Checking {comp_name} -> {search_query}")
        price_result = fetch_price(search_query, config)
        
        new_rows.append({
            "Date": date_stamp,
            "SKU": sku,
            "Product": product_name,
            "Competitor": comp_name,
            "Price": price_result
        })

# Save to CSV
history_file = 'price_history.csv'
new_df = pd.DataFrame(new_rows)

if not os.path.isfile(history_file):
    # Create the file with headers
    new_df.to_csv(history_file, index=False)
else:
    # Open the file in append mode and ensure we start on a new line
    with open(history_file, 'a') as f:
        f.write('\n') # This is the magic "Enter" key strike
        new_df.to_csv(f, header=False, index=False)

print("--- Scrape Complete! ---")