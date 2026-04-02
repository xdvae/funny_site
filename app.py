"""
server.py — FapItUp Production Backend
Install: pip install flask flask-cors curl_cffi
Run:     python server.py
"""

from flask import Flask, jsonify, request, send_from_directory, Response, session, redirect
from flask_cors import CORS
from curl_cffi import requests as req
import sqlite3, os, re, unicodedata, hashlib, secrets
from datetime import datetime, timedelta
from functools import wraps

app = Flask(__name__, static_folder=".")
# Fixed secret key — sessions survive server restarts
# CHANGE THIS to any random string before going live, then never change it again
app.secret_key = "fapitup-secret-key-change-this-NOW-abc123xyz"
CORS(app)

DB_PATH = "videos.db"

# ── ADMIN CONFIG — change these before going live ─────────────────────────────
ADMIN_USERNAME = "admin"
ADMIN_PASSWORD = "fapitup2026!"          # Change this
ADMIN_PATH     = "/alfa-gama-beta"       # Secret admin URL

# ── CDN Session ───────────────────────────────────────────────────────────────
cdn_session = req.Session(impersonate="chrome124")
cdn_session.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Referer":    "https://viralkand.com/",
    "Origin":     "https://viralkand.com",
})

def warm_up_session():
    try:
        cdn_session.get("https://viralkand.com/", timeout=10)
        print("[*] Session warmed up")
    except Exception as e:
        print(f"[!] Warmup failed: {e}")


# ── DB ────────────────────────────────────────────────────────────────────────
def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def slugify(text: str) -> str:
    text = unicodedata.normalize("NFKD", text).encode("ascii","ignore").decode("ascii")
    text = text.lower()
    text = re.sub(r"[^\w\s-]", "", text)
    text = re.sub(r"[\s_]+", "-", text)
    return re.sub(r"-+", "-", text).strip("-")[:80]


def ensure_all():
    """Run all DB migrations on startup."""
    conn = get_db()

    # Slugs
    try:
        conn.execute("ALTER TABLE videos ADD COLUMN slug TEXT")
        conn.commit()
    except Exception: pass

    # Archived flag
    try:
        conn.execute("ALTER TABLE videos ADD COLUMN archived INTEGER DEFAULT 0")
        conn.commit()
    except Exception: pass

    # Views counter (internal click tracking)
    try:
        conn.execute("ALTER TABLE videos ADD COLUMN site_views INTEGER DEFAULT 0")
        conn.commit()
    except Exception: pass

    # Removal requests table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS removal_requests (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            video_id     INTEGER,
            video_title  TEXT,
            name         TEXT,
            email        TEXT,
            reason       TEXT,
            status       TEXT DEFAULT 'pending',
            submitted_at TEXT DEFAULT (datetime('now'))
        )
    """)
    try:
        conn.execute("ALTER TABLE removal_requests ADD COLUMN status TEXT DEFAULT 'pending'")
        conn.commit()
    except Exception: pass

    # Analytics events table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS analytics (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            video_id   INTEGER,
            event      TEXT,
            ip_hash    TEXT,
            user_agent TEXT,
            created_at TEXT DEFAULT (datetime('now'))
        )
    """)

    # Populate slugs
    rows = conn.execute("SELECT id, title, post_id FROM videos WHERE slug IS NULL OR slug = ''").fetchall()
    if rows:
        print(f"[*] Generating slugs for {len(rows)} videos...")
        used = set(r[0] for r in conn.execute("SELECT slug FROM videos WHERE slug IS NOT NULL AND slug != ''").fetchall())
        for row in rows:
            base = slugify(row["title"]) or f"video-{row['post_id']}"
            slug = base
            if slug in used:
                slug = f"{base}-{row['post_id']}"
            used.add(slug)
            conn.execute("UPDATE videos SET slug=? WHERE id=?", (slug, row["id"]))
        conn.commit()
        print("[*] Slugs done")

    conn.commit()
    conn.close()


# ── ADMIN AUTH ────────────────────────────────────────────────────────────────
def hash_password(pw):
    return hashlib.sha256(pw.encode()).hexdigest()

ADMIN_HASH = hash_password(ADMIN_PASSWORD)

def admin_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if not session.get("admin_logged_in"):
            return jsonify({"error": "Unauthorized"}), 401
        return f(*args, **kwargs)
    return decorated


# ── ANALYTICS HELPER ──────────────────────────────────────────────────────────
def log_event(video_id, event):
    try:
        ip = request.headers.get("X-Forwarded-For", request.remote_addr or "")
        ip_hash = hashlib.md5(ip.encode()).hexdigest()  # anonymized
        ua = (request.headers.get("User-Agent",""))[:200]
        conn = get_db()
        conn.execute(
            "INSERT INTO analytics (video_id, event, ip_hash, user_agent) VALUES (?,?,?,?)",
            (video_id, event, ip_hash, ua)
        )
        if event == "play":
            conn.execute("UPDATE videos SET site_views = COALESCE(site_views,0)+1 WHERE id=?", (video_id,))
        conn.commit()
        conn.close()
    except Exception:
        pass


# ═══════════════════════════════════════════════════════════════
# PUBLIC ROUTES
# ═══════════════════════════════════════════════════════════════
# SSR ROUTES — Return full HTML with meta for Googlebot
# Human users get the SPA loaded inside; bots get full content
# ═══════════════════════════════════════════════════════════════

SITE_DOMAIN = "https://fapitup.online"  # Change to your real domain

# ═══════════════════════════════════════════════════════════════
# SSR ROUTES — Server-side rendering for Google indexing
# ═══════════════════════════════════════════════════════════════

SITE_DOMAIN = "https://fapitup.online"

BOT_AGENTS = re.compile(
    r'bot|crawl|spider|google|bing|baidu|yandex|duckduck|facebook|twitter|linkedin|slack|telegram|whatsapp|preview|fetch|curl|wget|python|java|ruby',
    re.IGNORECASE
)

def is_bot():
    ua = request.headers.get("User-Agent", "")
    return bool(BOT_AGENTS.search(ua))


def ssr_shell(title, description, canonical, og_image="", extra_schema="", body_content=""):
    og_image   = og_image or f"{SITE_DOMAIN}/assets/logo.png"
    safe_title = title.replace('"', '&quot;').replace("'", "&#39;")
    safe_desc  = description[:160].replace('"', '&quot;').replace("'", "&#39;")

    # For bots: pure static HTML, NO javascript redirect at all
    # Google sees exactly what's in the HTML — canonical, title, content
    if is_bot():
        return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1.0"/>
<title>{title}</title>
<meta name="description" content="{safe_desc}"/>
<meta name="robots" content="index, follow, max-image-preview:large"/>
<link rel="canonical" href="{canonical}"/>
<link rel="icon" type="image/png" href="/assets/logo.png"/>
<meta property="og:type" content="video.other"/>
<meta property="og:title" content="{safe_title}"/>
<meta property="og:description" content="{safe_desc}"/>
<meta property="og:image" content="{og_image}"/>
<meta property="og:url" content="{canonical}"/>
<meta property="og:site_name" content="FapItUp"/>
<meta name="twitter:card" content="summary_large_image"/>
<meta name="twitter:title" content="{safe_title}"/>
<meta name="twitter:image" content="{og_image}"/>
{extra_schema}
<style>
*{{box-sizing:border-box;margin:0;padding:0;}}
body{{background:#0a0a0f;color:#f0e8f0;font-family:sans-serif;padding:20px;}}
.wrap{{max-width:900px;margin:0 auto;}}
img{{width:100%;border-radius:8px;aspect-ratio:16/9;object-fit:cover;display:block;}}
h1{{font-size:1.3rem;margin:14px 0 8px;line-height:1.4;}}
p{{font-size:.88rem;color:#aaa;line-height:1.7;margin-bottom:12px;}}
.meta{{font-size:.78rem;color:#666;margin-bottom:12px;}}
.tags{{display:flex;flex-wrap:wrap;gap:7px;margin-bottom:16px;}}
.tag{{padding:3px 11px;border:1px solid #333;border-radius:20px;font-size:.73rem;color:#aaa;text-decoration:none;}}
.grid{{display:grid;grid-template-columns:repeat(auto-fill,minmax(160px,1fr));gap:10px;margin-top:20px;}}
.card{{text-decoration:none;color:#ccc;}}
.card img{{aspect-ratio:16/9;border-radius:6px;}}
.card p{{font-size:.74rem;margin:5px 0 0;}}
a{{color:#ff6fa8;}}
</style>
</head>
<body><div class="wrap">{body_content}</div></body>
</html>"""

    # For real users: store path, redirect to full SPA
    # SPA reads sessionStorage and opens the right video/tag/search
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1.0"/>
<title>{title}</title>
<meta name="description" content="{safe_desc}"/>
<meta name="robots" content="index, follow"/>
<link rel="canonical" href="{canonical}"/>
<link rel="icon" type="image/png" href="/assets/logo.png"/>
<meta property="og:type" content="video.other"/>
<meta property="og:title" content="{safe_title}"/>
<meta property="og:description" content="{safe_desc}"/>
<meta property="og:image" content="{og_image}"/>
<meta property="og:url" content="{canonical}"/>
{extra_schema}
<style>
  body{{margin:0;background:#0a0a0f;display:flex;align-items:center;justify-content:center;height:100vh;}}
  .loader{{width:40px;height:40px;border-radius:50%;border:3px solid rgba(255,45,120,.2);border-top-color:#ff2d78;animation:spin .7s linear infinite;}}
  @keyframes spin{{to{{transform:rotate(360deg)}}}}
</style>
</head>
<body>
<div class="loader"></div>
<script>
sessionStorage.setItem('ssr_path', '{canonical.replace(SITE_DOMAIN, "")}');
window.location.replace('/');
</script>
</body>
</html>"""


@app.route("/watch/<slug>")
def watch_page(slug):
    conn = get_db()
    row = conn.execute("""
        SELECT v.id, v.title, v.description, v.thumbnail, v.duration,
               v.views, v.rating, v.slug, v.scraped_at
        FROM videos v WHERE v.slug=? AND (v.archived IS NULL OR v.archived=0)
    """, (slug,)).fetchone()

    if not row:
        conn.close()
        return send_from_directory(".", "index.html")

    row = dict(row)

    tags = conn.execute("""
        SELECT t.name, t.slug FROM tags t
        JOIN video_tags vt ON vt.tag_id=t.id WHERE vt.video_id=?
    """, (row["id"],)).fetchall()

    related = conn.execute("""
        SELECT v.title, v.thumbnail, v.slug FROM videos v
        JOIN video_tags vt ON vt.video_id=v.id
        WHERE vt.tag_id IN (SELECT tag_id FROM video_tags WHERE video_id=?)
        AND v.id != ? AND (v.archived IS NULL OR v.archived=0)
        ORDER BY RANDOM() LIMIT 8
    """, (row["id"], row["id"])).fetchall()
    conn.close()

    raw_title   = row['title']
    # SEO-optimised title: "Video Name – Watch Free Desi MMS | FapItUp"
    page_title  = f"{raw_title} – Watch Free Desi MMS Video Online | FapItUp"
    description = (row.get("description") or
                   f"Watch {raw_title} free online. Desi Indian MMS viral sex video on FapItUp – new videos added daily.")
    canonical   = f"{SITE_DOMAIN}/watch/{slug}"
    og_image    = row.get("thumbnail") or f"{SITE_DOMAIN}/assets/logo.png"
    upload_date = (row.get("scraped_at") or "2026-01-01")[:10]
    safe_title  = raw_title.replace('"','').replace("'",'')
    safe_desc   = description[:200].replace('"','').replace("'",'')

    # Tag links
    tag_html = ""
    if tags:
        tag_html = '<div class="tags">' + "".join(
            f'<a class="tag" href="{SITE_DOMAIN}/tag/{t["slug"]}">{t["name"]}</a>'
            for t in tags
        ) + "</div>"

    # Related grid with internal links
    related_html = ""
    if related:
        cards = "".join(
            f'<a class="card" href="{SITE_DOMAIN}/watch/{r["slug"]}">'
            f'<img src="{r["thumbnail"] or ""}" alt="{r["title"]}" loading="lazy"/>'
            f'<p>{r["title"]}</p></a>'
            for r in related
        )
        related_html = f'<h2>More Videos Like This</h2><div class="grid">{cards}</div>'

    schema = f"""<script type="application/ld+json">{{
  "@context":"https://schema.org",
  "@type":"VideoObject",
  "name":"{safe_title}",
  "description":"{safe_desc}",
  "thumbnailUrl":"{og_image}",
  "uploadDate":"{upload_date}",
  "embedUrl":"{canonical}",
  "publisher":{{"@type":"Organization","name":"FapItUp","url":"{SITE_DOMAIN}","logo":{{"@type":"ImageObject","url":"{SITE_DOMAIN}/assets/logo.png"}}}}
}}</script>"""

    body = f"""
<img src="{og_image}" alt="{raw_title}" />
<h1>{raw_title}</h1>
<div class="meta">👁 {row.get('views') or '—'} &nbsp;|&nbsp; 👍 {row.get('rating') or '—'} &nbsp;|&nbsp; ⏱ {row.get('duration') or '—'}</div>
<p>{description}</p>
{tag_html}
{related_html}
<p style="margin-top:20px;font-size:.8rem;color:#555;">
  <a href="{SITE_DOMAIN}">← Back to FapItUp – Free Desi MMS Videos</a>
</p>"""

    return ssr_shell(page_title, description, canonical, og_image, schema, body)


@app.route("/search/<query>")
def search_page(query):
    conn = get_db()
    q    = query.replace("-", " ")
    rows = conn.execute("""
        SELECT id, title, thumbnail, slug, duration FROM videos
        WHERE (archived IS NULL OR archived=0) AND (title LIKE ? OR description LIKE ?)
        ORDER BY id DESC LIMIT 12
    """, (f"%{q}%", f"%{q}%")).fetchall()
    total = conn.execute(
        "SELECT COUNT(*) FROM videos WHERE (archived IS NULL OR archived=0) AND (title LIKE ? OR description LIKE ?)",
        (f"%{q}%", f"%{q}%")
    ).fetchone()[0]
    conn.close()

    q_title     = q.title()
    page_title  = f"Watch {q_title} Sex Videos Free Online – Desi {q_title} MMS | FapItUp"
    description = f"Watch {total}+ free {q} desi sex videos and MMS clips online. Hot Indian {q} adult videos updated daily on FapItUp – India's top desi video site."
    canonical   = f"{SITE_DOMAIN}/search/{query}"

    cards = "".join(
        f'<a class="card" href="{SITE_DOMAIN}/watch/{r["slug"]}">'
        f'<img src="{r["thumbnail"]}" alt="{r["title"]}" loading="lazy"/>'
        f'<p>{r["title"]}</p></a>'
        for r in rows
    ) if rows else "<p>No videos found – <a href='/'>browse all videos</a></p>"

    schema = f"""<script type="application/ld+json">{{
  "@context":"https://schema.org",
  "@type":"SearchResultsPage",
  "name":"{page_title}",
  "description":"{description[:200]}",
  "url":"{canonical}"
}}</script>"""

    body = f"""
<h1>{q_title} Videos – Free Desi {q_title} MMS Online</h1>
<p>{description}</p>
<div class="grid">{cards}</div>
<p style="margin-top:20px;font-size:.8rem;color:#555;">
  <a href="{SITE_DOMAIN}">← Browse all desi MMS videos on FapItUp</a>
</p>"""

    return ssr_shell(page_title, description, canonical, "", schema, body)


@app.route("/tag/<tag_slug>")
def tag_page(tag_slug):
    conn = get_db()
    tag  = conn.execute("SELECT id, name, slug FROM tags WHERE slug=?", (tag_slug,)).fetchone()
    if not tag:
        conn.close()
        return send_from_directory(".", "index.html")

    rows  = conn.execute("""
        SELECT v.id, v.title, v.thumbnail, v.slug, v.duration
        FROM videos v JOIN video_tags vt ON vt.video_id=v.id
        WHERE vt.tag_id=? AND (v.archived IS NULL OR v.archived=0)
        ORDER BY v.id DESC LIMIT 12
    """, (tag["id"],)).fetchall()
    total = conn.execute("""
        SELECT COUNT(*) FROM videos v JOIN video_tags vt ON vt.video_id=v.id
        WHERE vt.tag_id=? AND (v.archived IS NULL OR v.archived=0)
    """, (tag["id"],)).fetchone()[0]

    # Related tags for internal linking
    related_tags = conn.execute("""
        SELECT DISTINCT t.name, t.slug FROM tags t
        JOIN video_tags vt ON vt.tag_id=t.id
        WHERE t.id != ? ORDER BY RANDOM() LIMIT 8
    """, (tag["id"],)).fetchall()
    conn.close()

    name        = tag["name"]
    page_title  = f"Desi {name} Sex Videos – Watch Free {name} MMS Online | FapItUp"
    description = f"Watch {total}+ free {name} desi sex videos and MMS clips. Hot Indian {name} adult videos, leaked MMS and more – updated daily on FapItUp."
    canonical   = f"{SITE_DOMAIN}/tag/{tag_slug}"

    cards = "".join(
        f'<a class="card" href="{SITE_DOMAIN}/watch/{r["slug"]}">'
        f'<img src="{r["thumbnail"]}" alt="{r["title"]}" loading="lazy"/>'
        f'<p>{r["title"]}</p></a>'
        for r in rows
    )

    related_tag_html = ""
    if related_tags:
        links = " · ".join(
            f'<a href="{SITE_DOMAIN}/tag/{t["slug"]}">{t["name"]}</a>'
            for t in related_tags
        )
        related_tag_html = f'<p style="margin-top:16px;font-size:.82rem;color:#666;">Related: {links}</p>'

    schema = f"""<script type="application/ld+json">{{
  "@context":"https://schema.org",
  "@type":"CollectionPage",
  "name":"{page_title}",
  "description":"{description[:200]}",
  "url":"{canonical}",
  "numberOfItems":{total}
}}</script>"""

    body = f"""
<h1>Desi {name} Sex Videos – Free Online</h1>
<p>{description}</p>
<div class="grid">{cards}</div>
{related_tag_html}
<p style="margin-top:20px;font-size:.8rem;color:#555;">
  <a href="{SITE_DOMAIN}">← Browse all desi MMS videos</a>
</p>"""

    return ssr_shell(page_title, description, canonical, "", schema, body)


# ── Catch-all for everything else (SPA) ──────────────────────────────────────
@app.route("/", defaults={"path": ""})
@app.route("/<path:path>")
def index(path):
    if path.startswith(("api/", "proxy", "sitemap", "robots", "alfa-gama-beta", "favicon", "assets/")):
        from flask import abort; abort(404)
    # Always serve index.html for SPA routes — including ?_spa=1 requests from SSR pages
    return send_from_directory(".", "index.html")


# ── Favicon — served directly from assets folder ──────────────────────────────
@app.route("/favicon")
def favicon():
    return send_from_directory("assets", "logo.png", mimetype="image/png")

# Also serve at /assets/logo.png so Google can crawl it directly
@app.route("/assets/<path:filename>")
def assets(filename):
    return send_from_directory("assets", filename)


@app.route("/sitemap.xml")
def sitemap():
    conn = get_db()
    videos = conn.execute(
        "SELECT slug, scraped_at FROM videos WHERE slug IS NOT NULL AND (archived IS NULL OR archived=0) ORDER BY id DESC"
    ).fetchall()
    tags = conn.execute("SELECT slug FROM tags").fetchall()
    conn.close()

    domain = SITE_DOMAIN

    # Video pages
    video_urls = [
        f"  <url><loc>{domain}/watch/{r['slug']}</loc><lastmod>{(r['scraped_at'] or '2026-01-01')[:10]}</lastmod><changefreq>monthly</changefreq><priority>0.8</priority></url>"
        for r in videos
    ]

    # Tag pages
    tag_urls = [
        f"  <url><loc>{domain}/tag/{t['slug']}</loc><changefreq>weekly</changefreq><priority>0.6</priority></url>"
        for t in tags
    ]

    # Key search pages
    key_searches = ["bhabhi","nepali","hindi","girlfriend","chudai","viral","bengali","hotel","doggy","blowjob","desi","hidden","college","aunty","wife"]
    search_urls = [
        f"  <url><loc>{domain}/search/{q}</loc><changefreq>weekly</changefreq><priority>0.5</priority></url>"
        for q in key_searches
    ]

    xml = '<?xml version="1.0" encoding="UTF-8"?>\n<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n'
    xml += f'  <url><loc>{domain}/</loc><changefreq>daily</changefreq><priority>1.0</priority></url>\n'
    xml += "\n".join(video_urls + tag_urls + search_urls)
    xml += "\n</urlset>"
    return Response(xml, mimetype="application/xml")


@app.route("/robots.txt")
def robots():
    domain = request.host_url.rstrip("/")
    # Block admin, block indexing of API
    txt = f"""User-agent: *
Allow: /
Disallow: {ADMIN_PATH}
Disallow: /api/
Disallow: /proxy
Sitemap: {domain}/sitemap.xml
"""
    return Response(txt, mimetype="text/plain")


# ── Public video API ──────────────────────────────────────────────────────────
@app.route("/api/videos")
def list_videos():
    page   = max(1, int(request.args.get("page", 1)))
    limit  = min(48, int(request.args.get("limit", 24)))
    offset = (page - 1) * limit
    search = request.args.get("q", "").strip()
    conn   = get_db()

    base_where = "(archived IS NULL OR archived=0)"
    if search:
        like = f"%{search}%"
        rows  = conn.execute(f"SELECT id,title,thumbnail,duration,views,rating,slug FROM videos WHERE {base_where} AND (title LIKE ? OR description LIKE ?) ORDER BY id DESC LIMIT ? OFFSET ?", (like,like,limit,offset)).fetchall()
        total = conn.execute(f"SELECT COUNT(*) FROM videos WHERE {base_where} AND (title LIKE ? OR description LIKE ?)", (like,like)).fetchone()[0]
    else:
        rows  = conn.execute(f"SELECT id,title,thumbnail,duration,views,rating,slug FROM videos WHERE {base_where} ORDER BY id DESC LIMIT ? OFFSET ?", (limit,offset)).fetchall()
        total = conn.execute(f"SELECT COUNT(*) FROM videos WHERE {base_where}").fetchone()[0]

    conn.close()
    return jsonify({"videos":[dict(r) for r in rows], "total":total, "page":page, "pages":-(-total//limit)})


# ── Homepage sections ─────────────────────────────────────────────────────────
@app.route("/api/videos/hot")
def hot_videos():
    """Random videos from entire DB — rotates on every request."""
    limit = min(24, int(request.args.get("limit", 12)))
    conn  = get_db()
    rows  = conn.execute("""
        SELECT id,title,thumbnail,duration,views,rating,slug
        FROM videos WHERE archived IS NULL OR archived=0
        ORDER BY RANDOM() LIMIT ?
    """, (limit,)).fetchall()
    conn.close()
    return jsonify([dict(r) for r in rows])


@app.route("/api/videos/recent")
def recent_videos():
    """Most recently added videos."""
    limit = min(24, int(request.args.get("limit", 12)))
    conn  = get_db()
    rows  = conn.execute("""
        SELECT id,title,thumbnail,duration,views,rating,slug,scraped_at
        FROM videos WHERE archived IS NULL OR archived=0
        ORDER BY id DESC LIMIT ?
    """, (limit,)).fetchall()
    conn.close()
    return jsonify([dict(r) for r in rows])


@app.route("/api/videos/<int:video_id>")
def get_video(video_id):
    conn = get_db()
    row = conn.execute(
        "SELECT id,title,video_url,thumbnail,duration,views,rating,page_url,description,slug,archived FROM videos WHERE id=?",
        (video_id,)
    ).fetchone()
    if not row:
        conn.close()
        return jsonify({"error":"Not found"}), 404
    if row["archived"]:
        conn.close()
        return jsonify({"error":"This video has been removed"}), 410
    data = dict(row)
    # Attach tags
    tags = conn.execute("""
        SELECT t.name, t.slug FROM tags t
        JOIN video_tags vt ON vt.tag_id=t.id
        WHERE vt.video_id=?
    """, (video_id,)).fetchall()
    data["tags"] = [dict(t) for t in tags]
    conn.close()
    return jsonify(data)


@app.route("/api/videos/slug/<slug>")
def get_video_by_slug(slug):
    conn = get_db()
    row = conn.execute(
        "SELECT id,title,video_url,thumbnail,duration,views,rating,page_url,description,slug,archived FROM videos WHERE slug=?",
        (slug,)
    ).fetchone()
    conn.close()
    if not row: return jsonify({"error":"Not found"}), 404
    if row["archived"]: return jsonify({"error":"This video has been removed"}), 410
    return jsonify(dict(row))


# ── Analytics event tracking ──────────────────────────────────────────────────
@app.route("/api/analytics", methods=["POST"])
def track_event():
    data     = request.get_json() or {}
    video_id = data.get("video_id")
    event    = data.get("event")  # "view", "play", "share"
    if video_id and event in ("view","play","share"):
        log_event(video_id, event)
    return jsonify({"ok": True})


# ── Removal request ───────────────────────────────────────────────────────────
@app.route("/api/removal", methods=["POST"])
def submit_removal():
    data        = request.get_json() or {}
    email       = data.get("email","").strip()
    reason      = data.get("reason","").strip()
    if not email or not reason:
        return jsonify({"error":"Email and reason required"}), 400
    conn = get_db()
    conn.execute(
        "INSERT INTO removal_requests (video_id,video_title,name,email,reason) VALUES (?,?,?,?,?)",
        (data.get("video_id"), data.get("video_title",""), data.get("name","").strip(), email, reason)
    )
    conn.commit(); conn.close()
    return jsonify({"success":True})


# ── Public: list all tags with counts ────────────────────────────────────────
@app.route("/api/tags")
def list_tags():
    conn = get_db()
    rows = conn.execute("""
        SELECT t.id, t.name, t.slug, COUNT(vt.video_id) as count
        FROM tags t
        LEFT JOIN video_tags vt ON vt.tag_id = t.id
        LEFT JOIN videos v ON v.id = vt.video_id AND (v.archived IS NULL OR v.archived=0)
        GROUP BY t.id HAVING count > 0
        ORDER BY count DESC
    """).fetchall()
    conn.close()
    return jsonify([dict(r) for r in rows])


# ── Public: videos by tag slug ────────────────────────────────────────────────
@app.route("/api/tags/<tag_slug>/videos")
def videos_by_tag(tag_slug):
    page  = max(1, int(request.args.get("page", 1)))
    limit = min(48, int(request.args.get("limit", 24)))
    offset= (page-1)*limit
    conn  = get_db()
    tag   = conn.execute("SELECT id, name, slug FROM tags WHERE slug=?", (tag_slug,)).fetchone()
    if not tag:
        conn.close()
        return jsonify({"error": "Tag not found"}), 404
    rows  = conn.execute("""
        SELECT v.id, v.title, v.thumbnail, v.duration, v.views, v.rating, v.slug
        FROM videos v
        JOIN video_tags vt ON vt.video_id = v.id
        WHERE vt.tag_id=? AND (v.archived IS NULL OR v.archived=0)
        ORDER BY v.id DESC LIMIT ? OFFSET ?
    """, (tag["id"], limit, offset)).fetchall()
    total = conn.execute("""
        SELECT COUNT(*) FROM videos v
        JOIN video_tags vt ON vt.video_id=v.id
        WHERE vt.tag_id=? AND (v.archived IS NULL OR v.archived=0)
    """, (tag["id"],)).fetchone()[0]
    conn.close()
    return jsonify({
        "tag": dict(tag),
        "videos": [dict(r) for r in rows],
        "total": total, "page": page, "pages": -(-total//limit)
    })



@app.route("/proxy")
def proxy_video():
    video_url = request.args.get("url")
    if not video_url: return "Missing url", 400
    if "vk25cdn.viralkand.com" not in video_url: return "Forbidden", 403

    headers = {"Referer":"https://viralkand.com/", "Range":request.headers.get("Range","bytes=0-")}
    upstream = cdn_session.get(video_url, headers=headers, stream=True, timeout=15)
    if upstream.status_code in (403,503):
        warm_up_session()
        upstream = cdn_session.get(video_url, headers=headers, stream=True, timeout=15)

    resp_headers = {"Content-Type": upstream.headers.get("Content-Type","video/mp4"), "Accept-Ranges":"bytes"}
    for h in ("Content-Length","Content-Range"):
        if h in upstream.headers: resp_headers[h] = upstream.headers[h]

    return Response(upstream.iter_content(chunk_size=1024*64), status=upstream.status_code, headers=resp_headers, direct_passthrough=True)


# ═══════════════════════════════════════════════════════════════
# ADMIN ROUTES — secret URL, backend session auth
# ═══════════════════════════════════════════════════════════════

@app.route(ADMIN_PATH)
def admin_panel():
    return send_from_directory(".", "admin.html")


@app.route("/api/admin/login", methods=["POST"])
def admin_login():
    data = request.get_json() or {}
    username = data.get("username","")
    password = data.get("password","")
    if username == ADMIN_USERNAME and hash_password(password) == ADMIN_HASH:
        session["admin_logged_in"] = True
        session.permanent = True
        app.permanent_session_lifetime = timedelta(hours=8)
        return jsonify({"success":True})
    return jsonify({"error":"Invalid credentials"}), 401


@app.route("/api/admin/logout", methods=["POST"])
def admin_logout():
    session.pop("admin_logged_in", None)
    return jsonify({"success":True})


@app.route("/api/admin/check")
def admin_check():
    return jsonify({"authenticated": bool(session.get("admin_logged_in"))})


@app.route("/api/admin/stats")
@admin_required
def admin_stats():
    conn = get_db()
    total_videos      = conn.execute("SELECT COUNT(*) FROM videos").fetchone()[0]
    archived_videos   = conn.execute("SELECT COUNT(*) FROM videos WHERE archived=1").fetchone()[0]
    total_requests    = conn.execute("SELECT COUNT(*) FROM removal_requests").fetchone()[0]
    pending_requests  = conn.execute("SELECT COUNT(*) FROM removal_requests WHERE status='pending'").fetchone()[0]
    reviewed_requests = conn.execute("SELECT COUNT(*) FROM removal_requests WHERE status='reviewed'").fetchone()[0]

    # Analytics
    total_plays  = conn.execute("SELECT COUNT(*) FROM analytics WHERE event='play'").fetchone()[0]
    total_views  = conn.execute("SELECT COUNT(*) FROM analytics WHERE event='view'").fetchone()[0]
    today_plays  = conn.execute("SELECT COUNT(*) FROM analytics WHERE event='play' AND date(created_at)=date('now')").fetchone()[0]
    today_views  = conn.execute("SELECT COUNT(*) FROM analytics WHERE event='view' AND date(created_at)=date('now')").fetchone()[0]
    week_plays   = conn.execute("SELECT COUNT(*) FROM analytics WHERE event='play' AND created_at >= datetime('now','-7 days')").fetchone()[0]

    # Top videos by plays
    top_videos = conn.execute("""
        SELECT v.id, v.title, v.slug, v.duration,
               COUNT(a.id) as play_count,
               v.site_views
        FROM videos v
        LEFT JOIN analytics a ON a.video_id=v.id AND a.event='play'
        WHERE v.archived IS NULL OR v.archived=0
        GROUP BY v.id ORDER BY play_count DESC LIMIT 10
    """).fetchall()

    # Plays per day last 7 days
    daily = conn.execute("""
        SELECT date(created_at) as day, COUNT(*) as count
        FROM analytics WHERE event='play' AND created_at >= datetime('now','-7 days')
        GROUP BY day ORDER BY day
    """).fetchall()

    # Search terms
    searches = conn.execute("""
        SELECT event, COUNT(*) as count FROM analytics
        WHERE event='view' GROUP BY ip_hash
        ORDER BY count DESC LIMIT 5
    """).fetchall()

    conn.close()
    return jsonify({
        "total_videos": total_videos, "archived_videos": archived_videos,
        "total_requests": total_requests, "pending_requests": pending_requests,
        "reviewed_requests": reviewed_requests,
        "total_plays": total_plays, "total_views": total_views,
        "today_plays": today_plays, "today_views": today_views,
        "week_plays": week_plays,
        "top_videos": [dict(r) for r in top_videos],
        "daily_plays": [dict(r) for r in daily],
    })


@app.route("/api/admin/requests")
@admin_required
def admin_requests():
    conn = get_db()
    rows = conn.execute("""
        SELECT r.*, v.slug as video_slug
        FROM removal_requests r
        LEFT JOIN videos v ON v.id=r.video_id
        ORDER BY r.submitted_at DESC
    """).fetchall()
    conn.close()
    return jsonify([dict(r) for r in rows])


@app.route("/api/admin/requests/<int:rid>/review", methods=["POST"])
@admin_required
def review_request(rid):
    conn = get_db()
    conn.execute("UPDATE removal_requests SET status='reviewed' WHERE id=?", (rid,))
    conn.commit(); conn.close()
    return jsonify({"success":True})


@app.route("/api/admin/videos")
@admin_required
def admin_videos():
    page   = max(1, int(request.args.get("page",1)))
    limit  = 30
    offset = (page-1)*limit
    search = request.args.get("q","").strip()
    status = request.args.get("status","all")

    where, params = [], []
    if search: where.append("v.title LIKE ?"); params.append(f"%{search}%")
    if status=="live": where.append("(v.archived IS NULL OR v.archived=0)")
    elif status=="archived": where.append("v.archived=1")
    ws = ("WHERE "+" AND ".join(where)) if where else ""

    conn = get_db()
    rows = conn.execute(f"""
        SELECT v.id, v.title, v.slug, v.duration, v.views, v.rating, v.archived,
               COALESCE(v.site_views,0) as site_views,
               COUNT(a.id) as play_count
        FROM videos v
        LEFT JOIN analytics a ON a.video_id=v.id AND a.event='play'
        {ws} GROUP BY v.id ORDER BY v.id DESC LIMIT ? OFFSET ?
    """, params+[limit,offset]).fetchall()

    total    = conn.execute(f"SELECT COUNT(*) FROM videos v {ws}", params).fetchone()[0]
    live_cnt = conn.execute("SELECT COUNT(*) FROM videos WHERE archived IS NULL OR archived=0").fetchone()[0]
    arch_cnt = conn.execute("SELECT COUNT(*) FROM videos WHERE archived=1").fetchone()[0]
    conn.close()

    return jsonify({
        "videos":[dict(r) for r in rows], "total":total,
        "page":page, "pages":-(-total//limit),
        "live":live_cnt, "archived":arch_cnt
    })


@app.route("/api/admin/videos/<int:vid>/archive", methods=["POST"])
@admin_required
def archive_video(vid):
    conn=get_db(); conn.execute("UPDATE videos SET archived=1 WHERE id=?", (vid,)); conn.commit(); conn.close()
    return jsonify({"success":True})


@app.route("/api/admin/videos/<int:vid>/restore", methods=["POST"])
@admin_required
def restore_video(vid):
    conn=get_db(); conn.execute("UPDATE videos SET archived=0 WHERE id=?", (vid,)); conn.commit(); conn.close()
    return jsonify({"success":True})


@app.route("/api/admin/videos/<int:vid>", methods=["DELETE"])
@admin_required
def delete_video(vid):
    conn=get_db(); conn.execute("DELETE FROM videos WHERE id=?", (vid,)); conn.execute("DELETE FROM analytics WHERE video_id=?", (vid,)); conn.commit(); conn.close()
    return jsonify({"success":True})


if __name__ == "__main__":
    if not os.path.exists(DB_PATH):
        print(f"[!] {DB_PATH} not found — run load_db.py first")
    else:
        ensure_all()
        warm_up_session()
        print(f"[*] Admin panel: http://localhost:5000{ADMIN_PATH}")
        print("[*] Starting FapItUp on http://localhost:5000")
        app.run(debug=False, port=5000)