"""
Gorgon Games — Competitor Price Benchmarking Scraper
Reads competitor URLs from CSV, scrapes price + stock status,
appends results to price_history.csv.

Usage:
    python scraper.py
    python scraper.py --urls path/to/urls.csv
    python scraper.py --dry-run   (print what would be scraped, no requests)

Dependencies:
    pip install requests beautifulsoup4 playwright
    playwright install chromium
"""

import argparse
import csv
import json
import re
import time
import random
from collections import Counter
from datetime import datetime
from pathlib import Path

import requests
from bs4 import BeautifulSoup

# Playwright is optional — only needed for Mighty Ape and Hobby Lords.
# If not installed, those stores will log a clear error rather than crashing.
try:
    from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout
    PLAYWRIGHT_AVAILABLE = True
except ImportError:
    PLAYWRIGHT_AVAILABLE = False


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

URLS_CSV = "Competitor_price_benchmarking_-_URLs.csv"
HISTORY_CSV = "price_history.csv"

# Polite delay between requests (seconds) — randomised to avoid rate limiting
REQUEST_DELAY = (1.5, 3.5)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-NZ,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

# ---------------------------------------------------------------------------
# Platform detection
# Determines which scraping strategy to use for a given URL/competitor.
# ---------------------------------------------------------------------------

# Shopify stores: use the /products/[handle].json API — no HTML parsing needed
SHOPIFY_DOMAINS = [
    "goblingames.nz",
    "novagames.co.nz",
    "ironknightgaming.co.nz",
    "beadndgames.co.nz",
]

# BigCommerce stores: consistent HTML structure, scrape with CSS selectors
BIGCOMMERCE_DOMAINS = [
    "vagabond.co.nz",
]

# Meta tag stores: price is in <meta property="product:price:amount" content="...">
# in the initial HTML — no JS rendering needed, fast plain request
META_TAG_DOMAINS = [
    "thehobbycollective.co.nz",
]

# Playwright stores: JS-rendered or Cloudflare-protected, need a real browser
PLAYWRIGHT_DOMAINS = [
    "mightyape.co.nz",
    "hobbylords.co.nz",
]


def detect_platform(url: str) -> str:
    """Return a platform key for routing to the right scraper."""
    if not url:
        return "none"
    domain = url.lower().split("/")[2] if "://" in url else ""
    if any(d in domain for d in SHOPIFY_DOMAINS):
        return "shopify"
    if any(d in domain for d in META_TAG_DOMAINS):
        return "meta_tag"
    if any(d in domain for d in BIGCOMMERCE_DOMAINS):
        return "bigcommerce"
    if any(d in domain for d in PLAYWRIGHT_DOMAINS):
        return "playwright"
    if "hobbymaster" in domain:
        return "hobby_master"
    if "gameslab" in domain:
        return "games_lab"
    return "generic"


# ---------------------------------------------------------------------------
# Price selectors
# For HTML scrapers: tried in order, first match with a valid price wins.
# For Playwright: same selectors, evaluated after JS has rendered.
# ---------------------------------------------------------------------------

PRICE_SELECTORS = {
    # Mighty Ape post-2024 platform (React/Next.js)
    "mighty_ape": [
        "[data-testid='product-price']",
        "[data-testid='price']",
        "[class*='ProductPrice']",
        "[class*='product-price']",
        "[itemprop='price']",
        "[class*='Price'] span",
        ".price",
    ],
    "bigcommerce": [
        "[data-product-price-without-tax]",
        "[data-product-price]",
        ".price--main .price",
        ".productView-price .price--main",
        "[itemprop='price']",
    ],
    # Hobby Lords: custom platform, JS-rendered
    "hobby_lords": [
        "[class*='product-price']",
        "[class*='price']",
        ".price",
        "[itemprop='price']",
        "span.price",
    ],
    "hobby_master": [
        # Scope to the main product section to avoid recommendation prices.
        # Their HTML: <section class="product-col product-details">
        #               <div class="price"><span class="price-new">$68.00</span>
        "section.product-details span.price-new",
        "section.product-details span.price-normal",
        ".product-col.product-details span.price-new",
        ".product-col.product-details span.price-normal",
        # Fallbacks (less precise, kept in case structure varies)
        "span.price-new",
        "span.price-normal",
        "[itemprop='price']",
    ],
    "games_lab": [
        "[data-hook='formatted-primary-price']",
        "[data-hook='product-page-price-range-from']",
        ".wixui-rich-text__text",
        "[class*='price']",
    ],
    "generic": [
        "[itemprop='price']",
        ".price",
        ".product-price",
        "span.price",
        "[data-price]",
    ],
}

# Text signals for stock status — checked against lowercased full page text
OUT_OF_STOCK_SIGNALS = [
    "out of stock",
    "sold out",
    "currently unavailable",
    "not available",
    "no longer available",
    "out-of-stock",
    "soldout",
]

IN_STOCK_SIGNALS = [
    "add to cart",
    "add to trolley",
    "buy now",
    "add to basket",
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def clean_price(raw: str) -> str | None:
    """
    Extract a numeric price from messy text. Returns '117.00' format.
    Enforces max 2 decimal places to prevent bleed-in from adjacent elements
    (e.g. Games Lab returning '117.004' where '4' bleeds from a nearby node).
    """
    if not raw:
        return None
    cleaned = raw.replace(",", "").replace("NZD", "").strip()
    match = re.search(r"\$?\s*(\d{1,6}(?:\.\d{1,2})?)", cleaned)
    if not match:
        return None
    value = float(match.group(1))
    # GW products in NZ are $10-$600. Anything outside is a false match.
    if not (10 <= value <= 600):
        return None
    return f"{value:.2f}"


def detect_stock_from_text(page_text: str) -> str:
    """Infer stock status from lowercased page text."""
    for signal in OUT_OF_STOCK_SIGNALS:
        if signal in page_text:
            return "out_of_stock"
    for signal in IN_STOCK_SIGNALS:
        if signal in page_text:
            return "in_stock"
    return "unknown"


# ---------------------------------------------------------------------------
# Scrapers
# ---------------------------------------------------------------------------

def scrape_shopify(url: str) -> dict:
    """
    Use Shopify's product JSON API — structured data, no HTML parsing.
    Gives us price and availability in one clean request.
    """
    base_url = url.split("?")[0].rstrip("/")
    json_url = base_url + ".json"

    try:
        resp = requests.get(json_url, headers=HEADERS, timeout=15)
        if resp.status_code == 404:
            return {"price": None, "stock_status": "not_found"}
        if resp.status_code != 200:
            return {"price": None, "stock_status": "error", "error": f"HTTP {resp.status_code}"}

        product = resp.json().get("product", {})
        variants = product.get("variants", [])

        if not variants:
            return {"price": None, "stock_status": "error", "error": "No variants in JSON response"}

        any_available = any(v.get("available", False) for v in variants)
        primary = next((v for v in variants if v.get("available")), variants[0])
        raw_price = primary.get("price", "")
        price = clean_price(str(raw_price))

        return {
            "price": price,
            "stock_status": "in_stock" if any_available else "out_of_stock",
        }

    except requests.exceptions.RequestException as e:
        return {"price": None, "stock_status": "error", "error": str(e)}
    except (json.JSONDecodeError, KeyError) as e:
        return {"price": None, "stock_status": "error", "error": f"Parse error: {e}"}


def scrape_meta_tag(url: str) -> dict:
    """
    Extract price from Open Graph meta tags in the page <head>.
    Both Mighty Ape and Hobby Collective embed price as:
      <meta property="product:price:amount" content="76">
      <meta property="product:price:currency" content="NZD">
    This is present in the initial HTML response — no JS rendering needed.
    Much faster and more reliable than scraping the visible page.
    """
    try:
        resp = requests.get(url, headers=HEADERS, timeout=15)
        if resp.status_code == 404:
            return {"price": None, "stock_status": "not_found"}
        if resp.status_code != 200:
            return {"price": None, "stock_status": "error", "error": f"HTTP {resp.status_code}"}

        soup = BeautifulSoup(resp.text, "html.parser")

        # Extract price from Open Graph meta tag
        price_tag = soup.find("meta", property="product:price:amount")
        price = clean_price(price_tag["content"]) if price_tag else None

        # Stock status from page text
        page_text = soup.get_text(" ", strip=True).lower()
        stock_status = detect_stock_from_text(page_text)
        if price and stock_status == "unknown":
            stock_status = "in_stock"

        return {"price": price, "stock_status": stock_status}

    except requests.exceptions.RequestException as e:
        return {"price": None, "stock_status": "error", "error": str(e)}



    """
    Fetch a page with requests and extract price + stock via CSS selectors.
    Used for BigCommerce, Hobby Master, Games Lab, and generic stores.
    """
    try:
        resp = requests.get(url, headers=HEADERS, timeout=15)
        if resp.status_code == 404:
            return {"price": None, "stock_status": "not_found"}
        if resp.status_code != 200:
            return {"price": None, "stock_status": "error", "error": f"HTTP {resp.status_code}"}

        soup = BeautifulSoup(resp.text, "html.parser")
        page_text = soup.get_text(" ", strip=True).lower()

        price = None
        selectors = PRICE_SELECTORS.get(platform, PRICE_SELECTORS["generic"])
        for selector in selectors:
            el = soup.select_one(selector)
            if el:
                raw = el.get("content") or el.get_text(strip=True)
                price = clean_price(raw)
                if price:
                    break

        stock_status = detect_stock_from_text(page_text)
        if price and stock_status == "unknown":
            stock_status = "in_stock"

        return {"price": price, "stock_status": stock_status}

    except requests.exceptions.RequestException as e:
        return {"price": None, "stock_status": "error", "error": str(e)}


def scrape_html(url: str, platform: str) -> dict:
    """
    Fetch a page with requests and extract price + stock via CSS selectors.
    Used for BigCommerce, Hobby Master, Games Lab, and generic stores.
    """
    try:
        resp = requests.get(url, headers=HEADERS, timeout=15)
        if resp.status_code == 404:
            return {"price": None, "stock_status": "not_found"}
        if resp.status_code != 200:
            return {"price": None, "stock_status": "error", "error": f"HTTP {resp.status_code}"}

        soup = BeautifulSoup(resp.text, "html.parser")
        page_text = soup.get_text(" ", strip=True).lower()

        price = None
        selectors = PRICE_SELECTORS.get(platform, PRICE_SELECTORS["generic"])
        for selector in selectors:
            el = soup.select_one(selector)
            if el:
                raw = el.get("content") or el.get_text(strip=True)
                price = clean_price(raw)
                if price:
                    break

        stock_status = detect_stock_from_text(page_text)
        if price and stock_status == "unknown":
            stock_status = "in_stock"

        return {"price": price, "stock_status": stock_status}

    except requests.exceptions.RequestException as e:
        return {"price": None, "stock_status": "error", "error": str(e)}


def scrape_playwright(url: str, platform: str) -> dict:
    """
    Launch a headless Chromium browser to render JS-heavy pages.
    Currently used for:
      - Hobby Lords: prices are rendered by JavaScript after page load

    Note: Mighty Ape was moved to scrape_meta_tag() — their price is in
    an Open Graph meta tag in the initial HTML, no browser needed.
    """
    if not PLAYWRIGHT_AVAILABLE:
        return {
            "price": None,
            "stock_status": "error",
            "error": (
                "Playwright not installed. "
                "Run: pip install playwright && playwright install chromium"
            ),
        }

    # Use hobby_lords selectors for that platform, mighty_ape selectors otherwise
    selector_key = "hobby_lords" if "hobbylords" in url else "mighty_ape"
    selectors = PRICE_SELECTORS.get(selector_key, PRICE_SELECTORS["generic"])

    try:
        with sync_playwright() as pw:
            browser = pw.chromium.launch(headless=True)
            context = browser.new_context(
                user_agent=HEADERS["User-Agent"],
                locale="en-NZ",
                viewport={"width": 1280, "height": 800},
            )
            page = context.new_page()

            # Block images, fonts, and media to speed up page loads
            page.route(
                "**/*.{png,jpg,jpeg,gif,webp,svg,woff,woff2,ttf,mp4,mp3}",
                lambda route: route.abort()
            )

            page.goto(url, wait_until="domcontentloaded", timeout=30000)

            # Give JS time to render the price after initial DOM load
            page.wait_for_timeout(2500)

            page_text = page.inner_text("body").lower()

            price = None
            for selector in selectors:
                try:
                    el = page.query_selector(selector)
                    if el:
                        raw = el.get_attribute("content") or el.inner_text()
                        price = clean_price(raw)
                        if price:
                            break
                except Exception:
                    continue

            stock_status = detect_stock_from_text(page_text)
            if price and stock_status == "unknown":
                stock_status = "in_stock"

            browser.close()
            return {"price": price, "stock_status": stock_status}

    except PlaywrightTimeout:
        return {"price": None, "stock_status": "error", "error": "Page load timed out"}
    except Exception as e:
        return {"price": None, "stock_status": "error", "error": f"Playwright error: {e}"}


def scrape_product(competitor: str, product_name: str, url: str) -> dict:
    """
    Entry point for a single product. Routes to the right scraper
    and returns a standardised result dict ready for CSV output.
    """
    base = {
        "scraped_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "competitor": competitor,
        "product_name": product_name,
        "url": url,
        "price": None,
        "stock_status": None,
        "error": None,
    }

    if not url:
        return {**base, "stock_status": "not_carried"}

    platform = detect_platform(url)

    if platform == "shopify":
        result = scrape_shopify(url)
    elif platform == "meta_tag":
        result = scrape_meta_tag(url)
    elif platform == "playwright":
        result = scrape_playwright(url, platform)
    else:
        result = scrape_html(url, platform)

    return {
        **base,
        "price": result.get("price"),
        "stock_status": result.get("stock_status", "unknown"),
        "error": result.get("error"),
    }


# ---------------------------------------------------------------------------
# CSV I/O
# ---------------------------------------------------------------------------

def load_urls(csv_path: str) -> list[dict]:
    """
    Load competitor URL mappings from the benchmarking spreadsheet.

    Supports both formats:
      New (3 columns): Competitor | Product Name | Link
        - Link is either a URL or "Not sold"
      Old (4 columns): Competitor | Product Name | Link | Status
        - Status column held "not available" / emoji flags

    Cleans tracking params from URLs automatically.
    """
    rows = []
    with open(csv_path, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            competitor = row.get("Competitor", "").strip()
            product = row.get("Product Name", "").strip()
            link = row.get("Link", "").strip()

            # Skip blank rows
            if not competitor or not product:
                continue

            # Values in the Link column that mean "not carried"
            not_carried_values = {"not sold", "not available", "not carried", "n/a", ""}

            # Legacy Status column (old 4-column format)
            status = row.get("Status", "").strip()

            if link.lower() in not_carried_values or "not sold" in link.lower() or "🚫" in link:
                url = ""
            elif "not available" in status.lower() or "🚫" in status:
                url = ""
            else:
                url = link

            # Strip tracking params
            if url:
                url = re.sub(r"[?&]srsltid=[^&]*", "", url)
                url = re.sub(r"[?&]utm_[^&]*", "", url)
                url = url.rstrip("?&").strip()
                if not url.startswith("http"):
                    url = ""

            rows.append({
                "competitor": competitor,
                "product_name": product,
                "url": url,
            })

    return rows


HISTORY_FIELDNAMES = [
    "scraped_at",
    "competitor",
    "product_name",
    "price",
    "stock_status",
    "url",
    "error",
]


def append_to_history(results: list[dict], csv_path: str):
    """Append results to price_history.csv, writing header if file is new."""
    file_exists = Path(csv_path).exists()
    with open(csv_path, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=HISTORY_FIELDNAMES, extrasaction="ignore")
        if not file_exists:
            writer.writeheader()
        writer.writerows(results)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Gorgon Games price scraper")
    parser.add_argument("--urls", default=URLS_CSV, help="Path to competitor URLs CSV")
    parser.add_argument("--output", default=HISTORY_CSV, help="Path to price history CSV")
    parser.add_argument("--dry-run", action="store_true", help="Print plan without making requests")
    args = parser.parse_args()

    print(f"\n{'='*60}")
    print(f"  Gorgon Games Price Benchmarking")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    if not PLAYWRIGHT_AVAILABLE:
        print(f"  ⚠️  Playwright not installed — Mighty Ape & Hobby Lords will error")
        print(f"     Fix: pip install playwright && playwright install chromium")
    print(f"{'='*60}\n")

    print(f"Loading URLs from: {args.urls}")
    products = load_urls(args.urls)
    has_url = sum(1 for p in products if p["url"])
    not_carried = sum(1 for p in products if not p["url"])
    print(f"  -> {len(products)} products: {has_url} to scrape, {not_carried} not carried\n")

    if args.dry_run:
        print("DRY RUN -- no requests will be made:\n")
        for p in products:
            platform = detect_platform(p["url"]) if p["url"] else "--"
            print(f"  [{platform:12}] {p['competitor']:16} {p['product_name']}")
        return

    results = []

    for i, product in enumerate(products, 1):
        competitor = product["competitor"]
        product_name = product["product_name"]
        url = product["url"]
        prefix = f"[{i:2}/{len(products)}]"

        if not url:
            result = scrape_product(competitor, product_name, "")
            results.append(result)
            print(f"{prefix} {competitor:16} {product_name:45} -> not carried")
            continue

        platform = detect_platform(url)
        print(f"{prefix} {competitor:16} {product_name:45} ({platform})", end=" ... ", flush=True)

        result = scrape_product(competitor, product_name, url)
        results.append(result)

        price = result["price"]
        status = result["stock_status"]
        error = result.get("error")

        if error:
            print(f"ERROR: {error}")
        elif status == "in_stock":
            print(f"${price} -- in stock")
        elif status == "out_of_stock":
            print(f"${price or '--'} -- OUT OF STOCK")
        else:
            print(f"${price or '--'} -- {status}")

        # Longer delay after Playwright requests (heavier operation)
        delay = random.uniform(3.0, 6.0) if platform == "playwright" else random.uniform(*REQUEST_DELAY)
        time.sleep(delay)

    # Write results
    append_to_history(results, args.output)

    # --- Sanity check: same price for every product at a competitor ---
    # Classic sign of a false selector match (e.g. Hobby Master $82.30 on all products).
    # Clears bad prices rather than writing them to history.
    for competitor_name in set(r["competitor"] for r in results):
        comp_results = [r for r in results if r["competitor"] == competitor_name and r["price"]]
        if len(comp_results) >= 3:
            prices = [r["price"] for r in comp_results]
            most_common_price, count = Counter(prices).most_common(1)[0]
            if count == len(comp_results):
                print(f"\n  WARNING: {competitor_name} returned ${most_common_price} for ALL "
                      f"{count} products -- likely a false selector (site-wide element).")
                print(f"  Clearing prices for {competitor_name} to avoid bad data in history.")
                for r in results:
                    if r["competitor"] == competitor_name:
                        r["price"] = None
                        r["error"] = f"Cleared: same price (${most_common_price}) on all products"

    # Summary
    in_stock = sum(1 for r in results if r["stock_status"] == "in_stock")
    out_of_stock = sum(1 for r in results if r["stock_status"] == "out_of_stock")
    no_carry = sum(1 for r in results if r["stock_status"] == "not_carried")
    errors = sum(1 for r in results if r["stock_status"] in ("error", "unknown", "not_found"))

    print(f"\n{'='*60}")
    print(f"  Run complete -- {len(results)} results written to {args.output}")
    print(f"  In stock:     {in_stock}")
    print(f"  Out of stock: {out_of_stock}")
    print(f"  Not carried:  {no_carry}")
    print(f"  Errors:       {errors}")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()