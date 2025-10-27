import os, csv, asyncio, time
from dotenv import load_dotenv
from pathlib import Path
from playwright.async_api import async_playwright

from parsers import extract_people

load_dotenv(Path(__file__).with_name(".env"))

START_URL     = os.getenv("START_URL")
RATE_SECONDS  = float(os.getenv("RATE_SECONDS", "1.0"))
MAX_PAGES     = int(os.getenv("MAX_PAGES", "5"))
HEADLESS      = os.getenv("HEADLESS", "true").lower() == "true"
READY_SELECTOR= os.getenv("READY_SELECTOR", ".alumni-card, .person-card, .profile-tile, li.alumni")
OUT_CSV       = os.getenv("OUT_CSV", "alumni_engineering.csv")

OUT_DIR = Path(__file__).with_name("output")
OUT_DIR.mkdir(exist_ok=True, parents=True)
OUT_PATH = OUT_DIR / OUT_CSV

async def run():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=HEADLESS)
        context = await browser.new_context(user_agent="UNT-SyntaxSyndicate-Scraper/1.0 (+contact you@unt.edu)")
        page = await context.new_page()

        seen_urls = set()
        page_count = 0

        with OUT_PATH.open("w", newline="", encoding="utf-8") as f:
            fieldnames = ["name","degree","year","major_or_department","location","source_url","scraped_at"]
            w = csv.DictWriter(f, fieldnames=fieldnames)
            w.writeheader()

            url = START_URL
            while url and page_count < MAX_PAGES:
                page_count += 1
                if url in seen_urls:
                    break
                seen_urls.add(url)

                await page.goto(url, wait_until="networkidle", timeout=60000)
                # Stabilize: wait for key container if provided
                if READY_SELECTOR:
                    try:
                        await page.wait_for_selector(READY_SELECTOR, timeout=10000)
                    except:
                        pass

                html = await page.content()
                now_iso = time.strftime("%Y-%m-%dT%H:%M:%S")
                for row in extract_people(html):
                    w.writerow({**row, "source_url": url, "scraped_at": now_iso})

                # Try common “next” patterns; adjust if your target differs
                next_sel_candidates = ['a[rel="next"]', 'a.next', 'button.next', 'a.pagination-next', 'button[aria-label="Next"]']
                next_handle = None
                for sel in next_sel_candidates:
                    nh = await page.query_selector(sel)
                    if nh:
                        next_handle = nh
                        break

                if next_handle:
                    try:
                        await next_handle.click()
                        await page.wait_for_load_state("networkidle", timeout=60000)
                        # Optional: wait again for selector
                        if READY_SELECTOR:
                            try:
                                await page.wait_for_selector(READY_SELECTOR, timeout=10000)
                            except:
                                pass
                        url = page.url
                    except:
                        url = None
                else:
                    url = None

                await page.wait_for_timeout(int(RATE_SECONDS * 1000))

        await browser.close()
    print(f"[OK] Saved: {OUT_PATH}")

if __name__ == "__main__":
    asyncio.run(run())
