"""
cdn_full_scraper.py
-------------------
Scans the full CDN range with CORRECT folder logic:
  - /8000/1000.mp4  through  /8000/9999.mp4
  - /9000/1000.mp4  through  /9000/9999.mp4

For title fetching:
  - Scrapes viralkand.com listing pages
  - For each card, extracts the video number from the thumbnail URL
  - Matches that number to what we found on the CDN
  - Falls back to scraping the individual video page if not found on listing

Also:
  - Deletes placeholder titles ("Video 8977" etc) from DB so they can be re-fetched
  - Skips video numbers already in DB with real titles
  - Translates titles to English (optional, graceful fallback)

Run:
  pip install requests beautifulsoup4 deep-translator
  python cdn_full_scraper.py

Daily automation (Windows Task Scheduler):
  Program: C:\\Python313\\python.exe
  Arguments: cdn_full_scraper.py
  Start in: C:\\path\\to\\your\\project

Daily automation (Linux/Mac cron):
  0 3 * * * cd /path/to/project && python cdn_full_scraper.py >> scraper.log 2>&1
"""

import sqlite3
import requests
from bs4 import BeautifulSoup
import re
import time
import random
import unicodedata
import logging
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

# ── Optional translation ───────────────────────────────────────────────────────
try:
    from deep_translator import GoogleTranslator
    TRANSLATE = True
except ImportError:
    TRANSLATE = False
    print("[WARN] pip install deep-translator  for English translations")

# ── Config ────────────────────────────────────────────────────────────────────
DB_PATH  = "videos.db"
CDN_BASE = "https://vk25cdn.viralkand.com"
SITE_URL = "https://viralkand.com"
THREADS  = 20       # parallel HEAD requests for CDN probing
TIMEOUT  = 6        # seconds per CDN request

# CORRECT folder structure:
# Each folder key is the CDN folder, value is (start_num, end_num) to probe
# e.g.  /8000/1000.mp4  /8000/1001.mp4 ... /8000/9999.mp4
#        /9000/1000.mp4  /9000/1001.mp4 ... /9000/9999.mp4
SCAN_FOLDERS = {
    8000: (1000, 9999),
    9000: (1000, 9999),
}

SITE_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Referer":    "https://viralkand.com/",
    "Accept":     "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
}

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(message)s",
    datefmt="%H:%M:%S",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("cdn_scraper.log", encoding="utf-8"),
    ]
)
log = logging.getLogger()


# ── Helpers ───────────────────────────────────────────────────────────────────
PLACEHOLDER_RE = re.compile(r'^video\s*\d+$|^v\d+$|^untitled\b|^\s*$', re.IGNORECASE)

def is_placeholder(title: str) -> bool:
    return not title or bool(PLACEHOLDER_RE.match(title.strip()))

def slugify(text: str) -> str:
    text = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode("ascii")
    text = text.lower()
    text = re.sub(r"[^\w\s-]", "", text)
    text = re.sub(r"[\s_]+", "-", text)
    return re.sub(r"-+", "-", text).strip("-")[:80]

def translate(text: str) -> str:
    """Translate to English. Returns original on failure."""
    if not TRANSLATE or not text:
        return text
    ascii_ratio = sum(1 for c in text if ord(c) < 128) / max(len(text), 1)
    if ascii_ratio > 0.75:
        return text  # already mostly English/Hinglish
    for attempt in range(3):
        try:
            result = GoogleTranslator(source='auto', target='en').translate(text[:4999])
            if result and not is_placeholder(result):
                return result
        except Exception:
            time.sleep(1.5 * (attempt + 1))
    return text


# ── DB helpers ────────────────────────────────────────────────────────────────
def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.row_factory = sqlite3.Row
    return conn

def get_existing(conn) -> dict:
    """
    Returns dict of {video_number: has_real_title}
    video_number extracted from video_url like /8000/8500.mp4 -> 8500
    """
    rows = conn.execute("SELECT id, video_url, title FROM videos WHERE video_url IS NOT NULL").fetchall()
    result = {}
    for r in rows:
        m = re.search(r'/(\d+)\.mp4$', r["video_url"] or "")
        if m:
            num = int(m.group(1))
            result[num] = not is_placeholder(r["title"] or "")
    return result

def delete_placeholders(conn) -> int:
    """Remove videos with placeholder titles so CDN scraper can re-insert with real titles."""
    rows = conn.execute("SELECT id, title FROM videos").fetchall()
    bad_ids = [r["id"] for r in rows if is_placeholder(r["title"] or "")]
    if bad_ids:
        conn.executemany("DELETE FROM videos WHERE id=?", [(i,) for i in bad_ids])
        conn.commit()
        log.info(f"[CLEAN] Deleted {len(bad_ids)} placeholder-titled videos")
    return len(bad_ids)

def ensure_tags_schema(conn):
    """Make sure tags tables exist (in case tagger.py hasn't been run)."""
    conn.execute("""CREATE TABLE IF NOT EXISTS tags (
        id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT UNIQUE, slug TEXT UNIQUE)""")
    conn.execute("""CREATE TABLE IF NOT EXISTS video_tags (
        video_id INTEGER, tag_id INTEGER, PRIMARY KEY(video_id, tag_id))""")
    conn.commit()

def insert_video(conn, v: dict) -> bool:
    try:
        conn.execute("""
            INSERT OR IGNORE INTO videos
                (post_id, title, page_url, video_url, thumbnail,
                 duration, views, rating, scraped_at, description)
            VALUES (?,?,?,?,?,?,?,?,?,?)
        """, (
            v.get("post_id") or f"cdn_{v['folder']}_{v['number']}",
            v["title"],
            v.get("page_url", ""),
            v["cdn_url"],
            v.get("thumbnail", f"https://viralkand.com/wp-content/uploads/{v['number']}.jpg"),
            v.get("duration", ""),
            v.get("views", ""),
            v.get("rating", ""),
            datetime.utcnow().isoformat(),
            v.get("description", ""),
        ))
        inserted = conn.execute("SELECT changes()").fetchone()[0] == 1
        if inserted:
            conn.commit()
        return inserted
    except Exception as e:
        log.warning(f"  [!] Insert error {v.get('number')}: {e}")
        return False


# ── Step 1: CDN Probe ─────────────────────────────────────────────────────────
def probe(folder: int, number: int):
    """HEAD request — returns dict if video exists, None if dead."""
    url = f"{CDN_BASE}/{folder}/{number}.mp4"
    try:
        r = requests.head(url, headers=SITE_HEADERS, timeout=TIMEOUT, allow_redirects=True)
        if r.status_code in (200, 206):
            return {"folder": folder, "number": number, "cdn_url": url}
    except Exception:
        pass
    return None

def scan_cdn(skip_numbers: set) -> list:
    """
    Probe all configured folders and number ranges.
    Returns list of {folder, number, cdn_url} for valid videos not in skip set.
    """
    all_found = []

    for folder, (start, end) in SCAN_FOLDERS.items():
        to_check = [n for n in range(start, end + 1) if n not in skip_numbers]
        log.info(f"[CDN] Folder {folder}: probing {len(to_check)} numbers "
                 f"({start}–{end}), skipping {end-start+1-len(to_check)} already in DB")

        found_this = []
        done = 0

        with ThreadPoolExecutor(max_workers=THREADS) as ex:
            futures = {ex.submit(probe, folder, n): n for n in to_check}
            for future in as_completed(futures):
                done += 1
                result = future.result()
                if result:
                    found_this.append(result)
                if done % 500 == 0:
                    pct = done / len(to_check) * 100
                    log.info(f"  [Folder {folder}] {done}/{len(to_check)} ({pct:.0f}%) — {len(found_this)} valid")

        log.info(f"  [Folder {folder}] Complete — {len(found_this)} valid URLs found")
        all_found.extend(found_this)
        time.sleep(1)

    return all_found


# ── Step 2: Fetch titles from site ────────────────────────────────────────────
def build_site_map(max_listing_pages: int = 10) -> dict:
    """
    Scrapes viralkand.com listing pages.
    Returns {video_number: {title, page_url, post_id, views, rating, duration, thumbnail}}

    How it works:
      - Each video card has a thumbnail like /wp-content/uploads/8975-400x225.jpg
      - The number in that URL (8975) IS the video number
      - We extract that and map it to the title/metadata on that card
    """
    site_map = {}
    base_url = f"{SITE_URL}/page/{{}}/"

    log.info(f"[SITE] Scraping {max_listing_pages} listing pages for metadata...")

    for pg in range(1, max_listing_pages + 1):
        url = SITE_URL if pg == 1 else base_url.format(pg)
        try:
            resp = requests.get(url, headers=SITE_HEADERS, timeout=15)
            if resp.status_code == 404:
                break
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, "html.parser")

            cards = soup.find_all("div", class_=lambda c: c and "video-block" in (c or "").split())
            for card in cards:
                # Extract video number from thumbnail URL
                img = card.find("img", class_="video-img")
                if not img:
                    continue
                thumb = img.get("data-src") or img.get("src") or ""
                m = re.search(r'/uploads/(\d+)', thumb)
                if not m:
                    continue

                number = int(m.group(1))
                link   = card.find("a", class_="thumb")
                title_tag = card.find("span", class_="title")
                views_tag = card.find("span", class_="views-number")
                rat_tag   = card.find("span", class_="rating")
                dur_tag   = card.find("span", class_="duration")
                post_id   = card.get("data-post-id", f"cdn_{number}")

                title = title_tag.get_text(strip=True) if title_tag else ""
                if not title:
                    continue

                site_map[number] = {
                    "title":     title,
                    "page_url":  link["href"] if link and link.get("href") else "",
                    "post_id":   post_id,
                    "thumbnail": f"https://viralkand.com/wp-content/uploads/{number}.jpg",
                    "views":     views_tag.get_text(strip=True).replace("\xa0"," ") if views_tag else "",
                    "rating":    rat_tag.get_text(strip=True) if rat_tag else "",
                    "duration":  dur_tag.get_text(strip=True) if dur_tag else "",
                }

            log.info(f"  [SITE] Page {pg}: {len(cards)} cards scraped "
                     f"(site_map now has {len(site_map)} entries)")
            time.sleep(random.uniform(1.2, 2.0))

        except Exception as e:
            log.warning(f"  [SITE] Page {pg} failed: {e}")

    return site_map


def fetch_meta_from_video_page(page_url: str, number: int) -> dict:
    """
    Fallback: visit the individual video page to get title + description.
    viralkand.com video pages usually have the video number in the description
    or we can extract from the thumbnail in the video element.
    """
    if not page_url:
        return {}
    try:
        resp = requests.get(page_url, headers=SITE_HEADERS, timeout=15)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")

        # Title from h1 or og:title
        title = ""
        h1 = soup.find("h1")
        if h1:
            title = h1.get_text(strip=True)
        if not title:
            og = soup.find("meta", property="og:title")
            if og:
                title = og.get("content", "")

        # Description from video-description div
        desc = ""
        div = soup.find("div", class_="video-description")
        if div:
            paras = div.find_all("p")
            desc = " ".join(p.get_text(strip=True) for p in paras if p.get_text(strip=True))

        # Views / rating / duration
        views = ""
        views_el = soup.find("span", class_="views-number")
        if views_el:
            views = views_el.get_text(strip=True).replace("\xa0"," ")

        rating = ""
        rat_el = soup.find("span", class_="rating")
        if rat_el:
            rating = rat_el.get_text(strip=True)

        duration = ""
        dur_el = soup.find("span", class_="duration")
        if dur_el:
            duration = dur_el.get_text(strip=True)

        return {
            "title":       title,
            "description": desc,
            "views":       views,
            "rating":      rating,
            "duration":    duration,
        }
    except Exception as e:
        log.warning(f"  [PAGE] Failed {page_url}: {e}")
        return {}


def find_page_url_by_number(number: int) -> str:
    """
    Try to find the viralkand.com page URL for a video number
    by searching their site. Uses Google-style query.
    """
    try:
        search_url = f"{SITE_URL}/?s={number}"
        resp = requests.get(search_url, headers=SITE_HEADERS, timeout=10)
        soup = BeautifulSoup(resp.text, "html.parser")
        # Find a card whose thumbnail matches our number
        for card in soup.find_all("div", class_=lambda c: c and "video-block" in (c or "").split()):
            img = card.find("img", class_="video-img")
            if not img:
                continue
            thumb = img.get("data-src") or img.get("src") or ""
            m = re.search(r'/uploads/(\d+)', thumb)
            if m and int(m.group(1)) == number:
                link = card.find("a", class_="thumb")
                if link and link.get("href"):
                    return link["href"]
    except Exception:
        pass
    return ""


def fetch_description(page_url: str) -> str:
    if not page_url:
        return ""
    try:
        resp = requests.get(page_url, headers=SITE_HEADERS, timeout=12)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")
        div  = soup.find("div", class_="video-description")
        if not div:
            return ""
        paras = div.find_all("p")
        return " ".join(p.get_text(strip=True) for p in paras if p.get_text(strip=True))
    except Exception:
        return ""


# ── Step 3: Auto-tagger (lightweight inline version) ─────────────────────────
TAGS = {
    "Bhabhi":["bhabhi","bhabi"],
    "Girlfriend":["girlfriend","gf ","ladki","kudi"],
    "Wife":["biwi","patni","wife"],
    "Blowjob":["blowjob","lund chusa","chusai","deepthroat"],
    "Doggy Style":["doggy","ghodi"],
    "Riding":["sawari","riding","uchal","cowgirl"],
    "Missionary":["missionary","leta ke"],
    "Hardcore":["chudai","choda","pela","jabardast","hardcore"],
    "Hotel":["hotel","room mein","oyo"],
    "Nepali":["nepali","nepal"],
    "Bengali":["bengali"],
    "College":["college","student","classmate"],
    "Hidden Cam":["hidden","chupke","chori se"],
    "Aunty":["aunty","auntie"],
    "Sister":["behen","behan","sali"],
    "Devar Bhabhi":["devar"],
    "Leaked MMS":["mms","leaked","viral"],
    "Amateur":["homemade","ghar pe"],
    "Couple":["couple","pati patni","married"],
    "Big Dick":["mota lund","bada lund"],
}

def get_tag_ids(conn) -> dict:
    tag_ids = {}
    for name in TAGS:
        s = re.sub(r'[^a-z0-9]+', '-', name.lower()).strip('-')
        conn.execute("INSERT OR IGNORE INTO tags (name, slug) VALUES (?,?)", (name, s))
        row = conn.execute("SELECT id FROM tags WHERE name=?", (name,)).fetchone()
        if row:
            tag_ids[name] = row[0]
    conn.commit()
    return tag_ids

def tag_video(conn, video_id: int, title: str, desc: str, tag_ids: dict):
    text = f"{title} {desc}".lower()
    for tag_name, keywords in TAGS.items():
        if any(kw in text for kw in keywords):
            conn.execute(
                "INSERT OR IGNORE INTO video_tags (video_id, tag_id) VALUES (?,?)",
                (video_id, tag_ids[tag_name])
            )


# ── Main ──────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    start_time = datetime.now()
    log.info("=" * 60)
    log.info(f"FapItUp CDN Full Scraper — {start_time.strftime('%Y-%m-%d %H:%M')}")
    log.info(f"Folders: {list(SCAN_FOLDERS.keys())} | Threads: {THREADS}")
    log.info("=" * 60)

    if not Path(DB_PATH).exists():
        log.error(f"[!] {DB_PATH} not found. Run load_db.py first.")
        exit(1)

    conn = get_db()
    ensure_tags_schema(conn)

    # ── 1. Clean placeholder titles ───────────────────────────────
    log.info("\n[STEP 1] Cleaning placeholder titles from DB...")
    deleted = delete_placeholders(conn)

    # ── 2. Get existing video numbers ────────────────────────────
    existing = get_existing(conn)
    # Only skip numbers that have REAL titles — re-fetch ones we deleted
    skip_set = {num for num, has_title in existing.items() if has_title}
    log.info(f"[DB] {len(skip_set)} valid videos in DB, {len(existing)-len(skip_set)} need re-fetch")

    # ── 3. Scan CDN ───────────────────────────────────────────────
    log.info("\n[STEP 2] Scanning CDN...")
    cdn_found = scan_cdn(skip_set)
    log.info(f"\n[CDN] Found {len(cdn_found)} valid new URLs")

    if not cdn_found:
        log.info("[INFO] No new videos found. DB is up to date.")
        conn.close()
        exit(0)

    # ── 4. Fetch site metadata (titles) ──────────────────────────
    log.info("\n[STEP 3] Fetching titles from site listing pages...")
    site_map = build_site_map(max_listing_pages=10)

    # ── 5. Merge, fetch missing titles from video pages ───────────
    log.info("\n[STEP 4] Matching titles and fetching descriptions...")
    videos_to_insert = []
    needs_page_fetch  = []

    for item in cdn_found:
        num = item["number"]
        if num in site_map:
            # Got title from listing page
            item.update(site_map[num])
            needs_page_fetch.append(item)  # still fetch description
        else:
            # Number not found on recent listing pages
            # Try searching the site for this video
            item["title"]    = ""
            item["page_url"] = ""
            item["post_id"]  = f"cdn_{item['folder']}_{num}"
            item["thumbnail"]= f"https://viralkand.com/wp-content/uploads/{num}.jpg"
            item["views"]    = ""
            item["rating"]   = ""
            item["duration"] = ""
            needs_page_fetch.append(item)

    # Fetch descriptions (and missing titles) from individual video pages
    log.info(f"[STEP 4] Fetching metadata from {len(needs_page_fetch)} video pages...")
    for i, item in enumerate(needs_page_fetch):
        # If we don't have a page_url, try to find it
        if not item.get("page_url") and not item.get("title"):
            item["page_url"] = find_page_url_by_number(item["number"])

        if item.get("page_url"):
            meta = fetch_meta_from_video_page(item["page_url"], item["number"])
            if meta.get("title") and not item.get("title"):
                item["title"] = meta["title"]
            if meta.get("description"):
                item["description"] = meta.get("description", "")
            if meta.get("views") and not item.get("views"):
                item["views"] = meta["views"]
            if meta.get("rating") and not item.get("rating"):
                item["rating"] = meta["rating"]
            if meta.get("duration") and not item.get("duration"):
                item["duration"] = meta["duration"]
        elif not item.get("description"):
            item["description"] = ""

        if (i + 1) % 20 == 0:
            log.info(f"  [{i+1}/{len(needs_page_fetch)}] pages fetched")
        time.sleep(random.uniform(0.6, 1.2))

    # ── 6. Translate titles ───────────────────────────────────────
    if TRANSLATE:
        log.info("\n[STEP 5] Translating titles to English...")
        translated = 0
        for item in needs_page_fetch:
            orig = item.get("title", "")
            if orig and not is_placeholder(orig):
                t = translate(orig)
                if t and not is_placeholder(t):
                    item["title"] = t
                    translated += 1
            time.sleep(0.25)
        log.info(f"[TRANS] Translated {translated} titles")
    else:
        log.info("\n[STEP 5] Skipping translation (deep-translator not installed)")

    # ── 7. Insert into DB ─────────────────────────────────────────
    log.info("\n[STEP 6] Inserting into DB...")
    tag_ids  = get_tag_ids(conn)
    inserted = 0
    rejected = 0

    for item in needs_page_fetch:
        title = (item.get("title") or "").strip()
        if is_placeholder(title):
            rejected += 1
            log.info(f"  [REJECT] {item['folder']}/{item['number']} — no title found")
            continue

        if insert_video(conn, item):
            inserted += 1
            # Tag the new video
            vid_row = conn.execute(
                "SELECT id FROM videos WHERE video_url=?", (item["cdn_url"],)
            ).fetchone()
            if vid_row:
                tag_video(conn, vid_row[0], title, item.get("description",""), tag_ids)

    conn.commit()
    conn.close()

    elapsed = (datetime.now() - start_time).seconds
    total   = sqlite3.connect(DB_PATH).execute("SELECT COUNT(*) FROM videos").fetchone()[0]
    log.info(f"\n{'='*60}")
    log.info(f"Done in {elapsed}s")
    log.info(f"Inserted: {inserted} | Rejected (no title): {rejected} | Deleted placeholders: {deleted}")
    log.info(f"Total videos in DB: {total}")
    log.info(f"{'='*60}")