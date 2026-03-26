import requests
from bs4 import BeautifulSoup
import csv
from datetime import datetime

# 1. Your List of Competitors & SKUs
# Update the URLs below with your actual competitor links
targets = [
    {"sku": "GORGON-001", "name": "Dragon Fire Game", "url": "https://example-competitor.com/product/dragon-fire"},
    {"sku": "GORGON-002", "name": "Shadow Quest", "url": "https://another-shop.co.nz/shadow-quest"},
]

results = []

for item in targets:
    try:
        # Request the page (User-Agent makes you look like a real browser)
        headers = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"}
        res = requests.get(item['url'], headers=headers, timeout=10)
        soup = BeautifulSoup(res.text, 'html.parser')
        
        # 2. FINDING THE PRICE
        # This part is tricky. Most shops use a class like 'price' or 'amount'.
        # We try a few common ones automatically.
        price_element = soup.select_one('.price, .current-price, .amount, [data-price]')
        price = price_element.get_text(strip=True) if price_element else "Not Found"
        
        results.append({
            "date": datetime.now().strftime("%Y-%m-%d"),
            "sku": item['sku'],
            "name": item['name'],
            "price": price
        })
        print(f"Checked {item['name']}: {price}")
    except Exception as e:
        print(f"Error checking {item['sku']}: {e}")

# 3. Save to CSV (Appends new data to the bottom each week)
with open('prices.csv', 'a', newline='') as f:
    writer = csv.DictWriter(f, fieldnames=["date", "sku", "name", "price"])
    if f.tell() == 0: # Write header only if file is brand new
        writer.writeheader()
    writer.writerows(results)