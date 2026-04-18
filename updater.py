"""
updater.py
----------
Run once daily — handles everything:

  1. Scrapes new videos from viralkand.com
  2. Scrapes new videos from desitales2.com
  3. Merges desitales2.db into videos.db
  4. Tags all untagged videos
  5. Cleans dead links
  6. Pings IndexNow (Bing + Google) with new URLs

Schedule: 3:00 AM daily via Task Scheduler or cron
Logs:      updater.log

Setup IndexNow (free, 5 mins):
  1. Go to https://www.bing.com/indexnow
  2. Generate a key (any random string like a UUID)
  3. Set INDEXNOW_KEY below
  4. Add this route to server.py:

       @app.route("/<key_val>.txt")
       def indexnow_verify(key_val):
           if key_val == "YOUR_KEY_HERE":
               return "YOUR_KEY_HERE", 200, {"Content-Type":"text/plain"}
           abort(404)
"""

import sqlite3
import requests as req
from curl_cffi import requests as cffi_req
from bs4 import BeautifulSoup
import re, time, random, logging, unicodedata
from datetime import datetime
from pathlib import Path

# ── CONFIG ─────────────────────────────────────────────────────────────────────
VIDEOS_DB      = "videos.db"
DESITALES_DB   = "desitales2.db"
SITE_DOMAIN    = "https://fapitup.online"
INDEXNOW_KEY   = "385e35d3340a413aa9007cfc62129747"   # set this after getting key from bing.com/indexnow

VK_MAX_PAGES     = 20
DT_MAX_PAGES     = 5       # daily — only check newest pages
CHECK_DEAD       = True
DEAD_CHECK_LIMIT = 100

# ── LOGGING ────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(message)s",
    datefmt="%H:%M:%S",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("updater.log", encoding="utf-8"),
    ]
)
log = logging.getLogger()

# ── SESSIONS ───────────────────────────────────────────────────────────────────
VK_HEADERS = {
    "Referer":         "https://viralkand.com/",
    "User-Agent":      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
}
dt_session = cffi_req.Session(impersonate="chrome124")
dt_session.headers.update({
    "Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
})


# ══════════════════════════════════════════════════════════════════════════════
# SHARED HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def slugify(text: str) -> str:
    text = unicodedata.normalize("NFKD", text).encode("ascii","ignore").decode()
    text = text.lower()
    text = re.sub(r"[^\w\s-]","",text)
    text = re.sub(r"[\s_]+","-",text)
    return re.sub(r"-+","-",text).strip("-")[:80]

def get_db(path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.row_factory = sqlite3.Row
    return conn

def generate_slug(conn, title: str, post_id: str) -> str:
    base = slugify(title) or f"video-{post_id}"
    slug = base
    if conn.execute("SELECT 1 FROM videos WHERE slug=?", (slug,)).fetchone():
        slug = f"{base}-{post_id}"
    return slug

TAGS = {
    "Bhabhi":       ["bhabhi","bhabi","sister-in-law"],
    "Girlfriend":   ["girlfriend","gf "],
    "Wife":         ["biwi","patni","wife","housewife","newlywed","honeymoon"],
    "Aunty":        ["aunty","auntie","mature"],
    "Sister":       ["behen","behan","sali"],
    "Devar Bhabhi": ["devar"],
    "Couple":       ["couple","pati patni","married","husband"],
    "Threesome":    ["threesome","3some"],
    "Blowjob":      ["blowjob","lund chusa","chusai","deepthroat","suck","bj "],
    "Doggy Style":  ["doggy","ghodi"],
    "Riding":       ["riding","uchal","cowgirl"],
    "Missionary":   ["missionary"],
    "Anal":         ["anal","gaand"],
    "Fingering":    ["finger","fingering"],
    "Masturbation": ["masturbat","solo","self","nude show"],
    "Hardcore":     ["chudai","choda","pela","hardcore","fucked hard","pounded","drilled","banged","pumped"],
    "Kissing":      ["kissing","romance"],
    "Massage":      ["massage","malish"],
    "Striptease":   ["strip","nude","naked"],
    "Nepali":       ["nepali","nepal","pokhara"],
    "Bengali":      ["bengali","bangali","kolkata","bangladesh","boudi"],
    "Tamil":        ["tamil","coimbatore","chennai"],
    "Malayalam":    ["mallu","malayalam","kerala"],
    "Punjabi":      ["punjabi","chandigarh","mohali"],
    "Pakistani":    ["pakistani","pakistan","islamabad","karachi","lahori","paki"],
    "South Indian": ["south indian","kannada","andhra","mangalore","vizag"],
    "Bihari":       ["bihari","bihar"],
    "Assamese":     ["assamese","assam"],
    "Marathi":      ["marathi","mumbai","pune"],
    "Desi":         ["desi","indian","india","hindustani"],
    "Hotel":        ["hotel","oyo","lodge","room mein"],
    "Outdoor":      ["outdoor","jungle","park","fields","balcon"],
    "Office":       ["office","manager","boss"],
    "College":      ["college","student","university","mbbs"],
    "Big Boobs":    ["big boobs","busty","huge boobs","boobs"],
    "Chubby":       ["chubby","fat","bbw","thick"],
    "Leaked MMS":   ["mms","leaked","viral","hidden cam","spy"],
    "Hidden Cam":   ["hidden","chupke","secretly"],
    "Amateur":      ["amateur","homemade","ghar pe","self recorded"],
    "Hindi Audio":  ["hindi audio","hindi awaz"],
    "Cheating":     ["cheating","affair","secret sex","sneaky"],
    "First Time":   ["first time","first night","virgin"],
    "Webcam":       ["webcam","live cam","tiktok","tiktoker"],
    "Malay":        ["malay","malaysia"],
    "Indonesian":   ["indonesian","indonesia"],
    "Arab":         ["arab","hijab"],
    "Celebrity":    ["actress","model","celebrity","influencer"],
}

def ensure_tags_schema(conn):
    conn.execute("CREATE TABLE IF NOT EXISTS tags (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT UNIQUE, slug TEXT UNIQUE)")
    conn.execute("CREATE TABLE IF NOT EXISTS video_tags (video_id INTEGER, tag_id INTEGER, PRIMARY KEY(video_id,tag_id))")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_vt_video ON video_tags(video_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_vt_tag   ON video_tags(tag_id)")
    conn.commit()

def get_tag_ids(conn) -> dict:
    ids = {}
    for name in TAGS:
        s = re.sub(r'[^a-z0-9]+','-',name.lower()).strip('-')
        conn.execute("INSERT OR IGNORE INTO tags (name,slug) VALUES (?,?)",(name,s))
        row = conn.execute("SELECT id FROM tags WHERE name=?",(name,)).fetchone()
        if row: ids[name] = row[0]
    conn.commit()
    return ids

def tag_video(conn, video_id: int, title: str, desc: str, tag_ids: dict):
    text = f"{title} {desc}".lower()
    for name, kws in TAGS.items():
        if any(k in text for k in kws):
            conn.execute("INSERT OR IGNORE INTO video_tags (video_id,tag_id) VALUES (?,?)",
                         (video_id, tag_ids[name]))

def tag_untagged(conn):
    rows = conn.execute("""
        SELECT v.id, v.title, v.description FROM videos v
        LEFT JOIN video_tags vt ON vt.video_id=v.id
        WHERE vt.video_id IS NULL AND (v.archived IS NULL OR v.archived=0)
    """).fetchall()
    if not rows:
        log.info("[TAG] All videos tagged"); return 0
    log.info(f"[TAG] Tagging {len(rows)} untagged videos...")
    tag_ids = get_tag_ids(conn)
    for (vid_id, title, desc) in rows:
        tag_video(conn, vid_id, title or "", desc or "", tag_ids)
    conn.commit()
    log.info(f"[TAG] Done")
    return len(rows)


# ══════════════════════════════════════════════════════════════════════════════
# VIRALKAND
# ══════════════════════════════════════════════════════════════════════════════

def vk_folder(n): return 8000 if n >= 9000 else (n // 1000) * 1000
def vk_video(n): return f"https://vk25cdn.viralkand.com/{vk_folder(n)}/{n}.mp4"
def vk_thumb(n): return f"https://viralkand.com/wp-content/uploads/{n}.jpg"
def vk_num(url):
    m = re.search(r'/uploads/(\d+)',url)
    return int(m.group(1)) if m else None

def vk_parse(html):
    soup = BeautifulSoup(html,"html.parser")
    results = []
    for card in soup.find_all("div", class_=lambda c: c and "video-block" in c.split()):
        try:
            post_id  = card.get("data-post-id","").strip()
            t_tag    = card.find("span",class_="title")
            title    = t_tag.get_text(strip=True) if t_tag else ""
            link_tag = card.find("a",class_="thumb")
            page_url = link_tag["href"] if link_tag and link_tag.get("href") else ""
            img_tag  = card.find("img",class_="video-img")
            tsrc     = (img_tag.get("data-src") or img_tag.get("src") or "") if img_tag else ""
            n        = vk_num(tsrc)
            vtag     = card.find("span",class_="views-number")
            rtag     = card.find("span",class_="rating")
            dtag     = card.find("span",class_="duration")
            results.append({
                "post_id":       post_id,"title":title,"page_url":page_url,
                "video_url":     vk_video(n) if n else "",
                "thumbnail_url": vk_thumb(n) if n else tsrc,
                "views":         vtag.get_text(strip=True) if vtag else "",
                "rating":        rtag.get_text(strip=True) if rtag else "",
                "duration":      dtag.get_text(strip=True) if dtag else "",
                "description":   "","scraped_at":datetime.utcnow().isoformat(),
            })
        except: pass
    return results

def vk_desc(page_url):
    try:
        r = req.get(page_url,headers=VK_HEADERS,timeout=15)
        soup = BeautifulSoup(r.text,"html.parser")
        div = soup.find("div",class_="video-description")
        if not div: return ""
        return " ".join(p.get_text(strip=True) for p in div.find_all("p")) or div.get_text(" ",strip=True)
    except: return ""

def scrape_viralkand(conn) -> list[str]:
    existing = {r[0] for r in conn.execute("SELECT post_id FROM videos WHERE post_id IS NOT NULL").fetchall()}
    tag_ids  = get_tag_ids(conn)
    new_videos = []

    for pg in range(1, VK_MAX_PAGES + 1):
        url = "https://viralkand.com/" if pg == 1 else f"https://viralkand.com/page/{pg}/"
        log.info(f"[VK] Page {pg}")
        try:
            r = req.get(url,headers=VK_HEADERS,timeout=15)
            if r.status_code == 404: break
            r.raise_for_status()
        except Exception as e:
            log.warning(f"[VK] {e}"); break

        cards = vk_parse(r.text)
        if not cards: break
        hit = False
        for card in cards:
            if card["post_id"] in existing: hit=True; break
            if card["video_url"]: new_videos.append(card)
        log.info(f"[VK] Page {pg}: {len(new_videos)} new so far")
        if hit: break
        time.sleep(random.uniform(1.5,2.5))

    log.info(f"[VK] {len(new_videos)} new videos")
    new_slugs = []
    for v in new_videos:
        v["description"] = vk_desc(v["page_url"])
        try:
            conn.execute("""INSERT OR IGNORE INTO videos
                (post_id,title,page_url,video_url,thumbnail,duration,views,rating,scraped_at,description)
                VALUES (?,?,?,?,?,?,?,?,?,?)""",
                (v["post_id"],v["title"],v["page_url"],v["video_url"],
                 v["thumbnail_url"],v["duration"],v["views"],v["rating"],
                 v["scraped_at"],v["description"]))
            if conn.execute("SELECT changes()").fetchone()[0]:
                row = conn.execute("SELECT id FROM videos WHERE post_id=?",(v["post_id"],)).fetchone()
                if row:
                    slug = generate_slug(conn,v["title"],v["post_id"])
                    conn.execute("UPDATE videos SET slug=? WHERE id=?",(slug,row[0]))
                    tag_video(conn,row[0],v["title"],v["description"],tag_ids)
                    new_slugs.append(slug)
        except Exception as e:
            log.warning(f"[VK INSERT] {e}")
        time.sleep(random.uniform(0.8,1.5))

    conn.commit()
    log.info(f"[VK] Inserted {len(new_slugs)}")
    return new_slugs


# ══════════════════════════════════════════════════════════════════════════════
# DESITALES2
# ══════════════════════════════════════════════════════════════════════════════

DT_BASE = "https://www.desitales2.com"
DT_CDN  = "https://cdn.desitales2.com"

def dt_ids(url):
    m = re.search(r'/videos_screenshots/(\d+)/(\d+)/',url)
    return (int(m.group(1)),int(m.group(2))) if m else (None,None)
def dt_cdn(f,v): return f"{DT_CDN}/{f}/{v}/{v}.mp4"
def dt_thumb(f,v): return f"{DT_BASE}/videos/contents/videos_screenshots/{f}/{v}/320x180/1.jpg"

def dt_init(conn):
    conn.execute("""CREATE TABLE IF NOT EXISTS videos (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        post_id TEXT UNIQUE,title TEXT NOT NULL,
        page_url TEXT,video_url TEXT NOT NULL,
        thumbnail TEXT,duration TEXT,views TEXT,rating TEXT,
        scraped_at TEXT,description TEXT,slug TEXT,
        archived INTEGER DEFAULT 0,site_views INTEGER DEFAULT 0)""")
    conn.execute("CREATE TABLE IF NOT EXISTS tags (id INTEGER PRIMARY KEY AUTOINCREMENT,name TEXT UNIQUE,slug TEXT UNIQUE)")
    conn.execute("CREATE TABLE IF NOT EXISTS video_tags (video_id INTEGER,tag_id INTEGER,PRIMARY KEY(video_id,tag_id))")
    conn.commit()

def dt_parse(html):
    soup,results = BeautifulSoup(html,"html.parser"),[]
    for item in soup.find_all("div",class_="item"):
        if "ad" in (item.get("class") or []): continue
        try:
            link = item.find("a",href=True)
            if not link: continue
            page_url = link["href"]
            if not page_url.startswith("http"): page_url = DT_BASE + page_url
            if "/videos/" not in page_url or page_url.rstrip("/").endswith("/videos"): continue
            title = link.get("title","").strip()
            if not title:
                s = item.find("strong",class_="title")
                if s: title = s.get_text(strip=True)
            if not title: continue
            vid_id,folder = None,None
            fav = item.find(attrs={"data-fav-video-id":True})
            if fav:
                try: vid_id = int(fav["data-fav-video-id"])
                except: pass
            img = item.find("img")
            if img:
                src = img.get("src") or img.get("data-webp") or ""
                f,v = dt_ids(src)
                if f: folder=f
                if v and not vid_id: vid_id=v
            if not vid_id or not folder: continue
            dur   = item.find("div",class_="duration")
            views = item.find("div",class_="views")
            rat   = item.find("div",class_="rating")
            results.append({
                "post_id":f"dt2_{vid_id}","video_id":vid_id,"folder":folder,
                "title":title,"page_url":page_url,
                "video_url":dt_cdn(folder,vid_id),"thumbnail":dt_thumb(folder,vid_id),
                "duration":dur.get_text(strip=True) if dur else "",
                "views":views.get_text(strip=True) if views else "",
                "rating":rat.get_text(strip=True).replace("%","").strip() if rat else "",
            })
        except: pass
    return results

def dt_desc(page_url):
    try:
        r = dt_session.get(page_url,timeout=15)
        soup = BeautifulSoup(r.text,"html.parser")
        for div in soup.find_all("div",class_="item"):
            if "Description:" in div.get_text():
                em = div.find("em")
                if em: return em.get_text(" ",strip=True)[:1000]
        og = soup.find("meta",property="og:description")
        if og: return og.get("content","")[:500]
    except: pass
    return ""

def dt_fetch(page_num):
    ts = int(time.time()*1000)
    if page_num == 1:
        url = f"{DT_BASE}/videos/"
        dt_session.headers["Referer"] = f"{DT_BASE}/"
        dt_session.headers.pop("X-Requested-With",None)
    else:
        pn = page_num-1
        url = (f"{DT_BASE}/videos/latest-updates/{pn}/"
               f"?mode=async&function=get_block"
               f"&block_id=list_videos_latest_videos_list"
               f"&sort_by=post_date&from={page_num}&_={ts}")
        dt_session.headers["Referer"] = f"{DT_BASE}/videos/latest-updates/{pn}/"
        dt_session.headers["X-Requested-With"] = "XMLHttpRequest"
    for attempt in range(3):
        try:
            r = dt_session.get(url,timeout=20)
            if r.status_code==200 and len(r.text)>200: return r.text
            if r.status_code==404: return None
        except Exception as e:
            log.warning(f"  [DT fetch attempt {attempt+1}] {e}")
        time.sleep(3*(attempt+1))
    return None

def scrape_desitales2() -> list[str]:
    log.info("[DT] Warming up session...")
    try:
        dt_session.get(f"{DT_BASE}/",timeout=15)
        time.sleep(random.uniform(2,3))
    except: pass

    dt_conn = get_db(DESITALES_DB)
    dt_init(dt_conn)
    dt_tag_ids = get_tag_ids(dt_conn)
    existing = {r[0] for r in dt_conn.execute("SELECT post_id FROM videos WHERE post_id IS NOT NULL").fetchall()}
    log.info(f"[DT] {len(existing)} already in desitales2.db")

    new_slugs = []
    consec_empty = 0

    for pg in range(1, DT_MAX_PAGES+1):
        log.info(f"[DT] Page {pg}")
        html = dt_fetch(pg)
        if not html:
            consec_empty += 1
            if consec_empty >= 2: break
            continue
        cards = dt_parse(html)
        if not cards:
            consec_empty += 1
            if consec_empty >= 2: break
            continue
        consec_empty = 0
        log.info(f"[DT] {len(cards)} cards")
        hit = False
        for card in cards:
            if card["post_id"] in existing: hit=True; break
            card["description"] = dt_desc(card["page_url"])
            slug = slugify(card["title"]) or f"video-{card['post_id']}"
            if dt_conn.execute("SELECT 1 FROM videos WHERE slug=?",(slug,)).fetchone():
                slug = f"{slug}-{card['post_id']}"
            card["slug"] = slug
            try:
                dt_conn.execute("""INSERT OR IGNORE INTO videos
                    (post_id,title,page_url,video_url,thumbnail,duration,views,rating,scraped_at,description,slug)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
                    (card["post_id"],card["title"],card["page_url"],card["video_url"],
                     card["thumbnail"],card["duration"],card["views"],card["rating"],
                     datetime.utcnow().isoformat(),card["description"],slug))
                if dt_conn.execute("SELECT changes()").fetchone()[0]:
                    row = dt_conn.execute("SELECT id FROM videos WHERE post_id=?",(card["post_id"],)).fetchone()
                    if row:
                        tag_video(dt_conn,row[0],card["title"],card["description"],dt_tag_ids)
                        new_slugs.append(slug)
                        existing.add(card["post_id"])
            except Exception as e:
                log.warning(f"  [DT INSERT] {e}")
            time.sleep(random.uniform(0.8,1.4))
        dt_conn.commit()
        log.info(f"[DT] Page {pg}: {len(new_slugs)} new total")
        if hit: log.info("[DT] Hit known — done"); break
        time.sleep(random.uniform(2.5,4.0))

    dt_conn.close()
    log.info(f"[DT] {len(new_slugs)} new videos in desitales2.db")
    return new_slugs


# ══════════════════════════════════════════════════════════════════════════════
# MERGE desitales2.db → videos.db
# ══════════════════════════════════════════════════════════════════════════════

def merge_desitales(main_conn) -> list[str]:
    if not Path(DESITALES_DB).exists():
        log.info("[MERGE] desitales2.db not found"); return []

    src = sqlite3.connect(DESITALES_DB)
    src.row_factory = sqlite3.Row
    rows = src.execute("SELECT * FROM videos").fetchall()

    existing_ids   = {r[0] for r in main_conn.execute("SELECT post_id FROM videos WHERE post_id IS NOT NULL").fetchall()}
    existing_slugs = {r[0] for r in main_conn.execute("SELECT slug FROM videos WHERE slug IS NOT NULL").fetchall()}
    tag_ids = get_tag_ids(main_conn)

    merged = []
    for v in rows:
        v = dict(v)
        if v["post_id"] in existing_ids: continue
        base = v.get("slug") or slugify(v["title"]) or f"video-{v['post_id']}"
        slug = base
        if slug in existing_slugs: slug = f"{base}-{v['post_id']}"
        existing_slugs.add(slug)
        try:
            main_conn.execute("""INSERT OR IGNORE INTO videos
                (post_id,title,page_url,video_url,thumbnail,duration,views,rating,scraped_at,description,slug)
                VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
                (v["post_id"],v["title"],v.get("page_url",""),v["video_url"],v.get("thumbnail",""),
                 v.get("duration",""),v.get("views",""),v.get("rating",""),
                 v.get("scraped_at",""),v.get("description",""),slug))
            if main_conn.execute("SELECT changes()").fetchone()[0]:
                existing_ids.add(v["post_id"])
                row = main_conn.execute("SELECT id FROM videos WHERE post_id=?",(v["post_id"],)).fetchone()
                if row:
                    tag_video(main_conn,row[0],v["title"],v.get("description",""),tag_ids)
                merged.append(slug)
        except Exception as e:
            log.warning(f"  [MERGE ERR] {e}")

    for (name,sl) in src.execute("SELECT name,slug FROM tags").fetchall():
        main_conn.execute("INSERT OR IGNORE INTO tags (name,slug) VALUES (?,?)",(name,sl))
    main_conn.commit()
    src.close()
    log.info(f"[MERGE] Merged {len(merged)} new videos")
    return merged


# ══════════════════════════════════════════════════════════════════════════════
# DEAD LINK CLEANER
# ══════════════════════════════════════════════════════════════════════════════

def clean_dead_links(conn):
    rows = conn.execute(
        "SELECT id,video_url FROM videos WHERE (archived IS NULL OR archived=0) ORDER BY id DESC LIMIT ?",
        (DEAD_CHECK_LIMIT,)
    ).fetchall()
    log.info(f"[DEAD] Checking {len(rows)} recent videos...")
    dead = 0
    for row in rows:
        vid_id,url = row[0],row[1]
        if not url: continue
        try:
            ref = "https://www.desitales2.com/" if "desitales2" in url else "https://viralkand.com/"
            r = req.head(url,headers={"Referer":ref,"User-Agent":VK_HEADERS["User-Agent"]},
                         timeout=5,allow_redirects=True)
            if r.status_code == 404:
                conn.execute("DELETE FROM videos WHERE id=?",(vid_id,))
                dead += 1
                log.info(f"  [DEAD] {url[:70]}")
        except: pass
        time.sleep(0.15)
    conn.commit()
    log.info(f"[DEAD] Removed {dead}")


# ══════════════════════════════════════════════════════════════════════════════
# INDEXNOW PING
# ══════════════════════════════════════════════════════════════════════════════

def ping_indexnow(slugs: list[str]):
    if not INDEXNOW_KEY or INDEXNOW_KEY == "YOUR_KEY_HERE":
        log.info("[PING] IndexNow key not set — skipping")
        log.info("[PING] Get a free key at https://www.bing.com/indexnow")
        return
    if not slugs:
        log.info("[PING] No new URLs to ping"); return

    urls = [f"{SITE_DOMAIN}/watch/{s}" for s in slugs if s][:10000]
    payload = {
        "host":        "fapitup.online",
        "key":         INDEXNOW_KEY,
        "keyLocation": f"{SITE_DOMAIN}/{INDEXNOW_KEY}.txt",
        "urlList":     urls,
    }
    log.info(f"[PING] Pinging {len(urls)} URLs to IndexNow...")
    for endpoint,name in [
        ("https://api.indexnow.org/indexnow","IndexNow"),
        ("https://www.bing.com/indexnow","Bing"),
    ]:
        try:
            r = req.post(endpoint,json=payload,timeout=15,
                         headers={"Content-Type":"application/json; charset=utf-8"})
            log.info(f"  [PING] {name} → HTTP {r.status_code}")
        except Exception as e:
            log.warning(f"  [PING] {name} failed: {e}")


# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    start = datetime.now()
    log.info("=" * 60)
    log.info(f"FapItUp Daily Updater — {start.strftime('%Y-%m-%d %H:%M')}")
    log.info("=" * 60)

    if not Path(VIDEOS_DB).exists():
        log.error(f"[!] {VIDEOS_DB} not found"); exit(1)

    main_conn = get_db(VIDEOS_DB)
    ensure_tags_schema(main_conn)
    all_new_slugs = []

    # 1. Viralkand
    log.info("\n── 1. VIRALKAND ──────────────────────────────────────────────")
    try:
        slugs = scrape_viralkand(main_conn)
        all_new_slugs.extend(slugs)
    except Exception as e:
        log.error(f"[VK] {e}")

    # 2. DesiTales2
    log.info("\n── 2. DESITALES2 ─────────────────────────────────────────────")
    try:
        scrape_desitales2()
    except Exception as e:
        log.error(f"[DT] {e}")

    # 3. Merge
    log.info("\n── 3. MERGE ──────────────────────────────────────────────────")
    try:
        slugs = merge_desitales(main_conn)
        all_new_slugs.extend(slugs)
    except Exception as e:
        log.error(f"[MERGE] {e}")

    # 4. Tag untagged
    log.info("\n── 4. TAG ────────────────────────────────────────────────────")
    try:
        tag_untagged(main_conn)
    except Exception as e:
        log.error(f"[TAG] {e}")

    # 5. Dead links
    log.info("\n── 5. DEAD LINKS ─────────────────────────────────────────────")
    if CHECK_DEAD:
        try:
            clean_dead_links(main_conn)
        except Exception as e:
            log.error(f"[DEAD] {e}")

    # 6. Clean empty URLs
    main_conn.execute("DELETE FROM videos WHERE video_url IS NULL OR TRIM(video_url)=''")
    main_conn.commit()
    main_conn.close()

    # 7. Ping IndexNow
    log.info("\n── 6. INDEXNOW ───────────────────────────────────────────────")
    ping_indexnow(all_new_slugs)

    # Summary
    elapsed  = (datetime.now() - start).seconds
    total_db = sqlite3.connect(VIDEOS_DB).execute("SELECT COUNT(*) FROM videos").fetchone()[0]
    log.info("\n" + "=" * 60)
    log.info(f"Done in {elapsed}s — {len(all_new_slugs)} new videos today — {total_db} total")
    log.info("=" * 60)
