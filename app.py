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
app.secret_key = secrets.token_hex(32)  # Random secret each restart — sessions invalidated on restart
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

@app.route("/", defaults={"path": ""})
@app.route("/<path:path>")
def index(path):
    if path.startswith(("api/", "proxy", "sitemap", "robots", "alfa-gama-beta", "favicon")):
        from flask import abort; abort(404)
    return send_from_directory(".", "index.html")


# ── Favicon — served directly from assets folder ──────────────────────────────
@app.route("/favicon")
def favicon():
    return send_from_directory("assets", "logo.png", mimetype="image/png")


@app.route("/sitemap.xml")
def sitemap():
    conn = get_db()
    rows = conn.execute(
        "SELECT slug, scraped_at FROM videos WHERE slug IS NOT NULL AND (archived IS NULL OR archived=0) ORDER BY id DESC"
    ).fetchall()
    conn.close()
    domain = request.host_url.rstrip("/")
    urls = [f"""  <url><loc>{domain}/watch/{r['slug']}</loc><lastmod>{(r['scraped_at'] or '2026-01-01')[:10]}</lastmod><changefreq>monthly</changefreq><priority>0.8</priority></url>"""
            for r in rows]
    xml = f'<?xml version="1.0" encoding="UTF-8"?>\n<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n  <url><loc>{domain}/</loc><changefreq>daily</changefreq><priority>1.0</priority></url>\n' + "\n".join(urls) + "\n</urlset>"
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


@app.route("/api/videos/<int:video_id>")
def get_video(video_id):
    conn = get_db()
    row = conn.execute(
        "SELECT id,title,video_url,thumbnail,duration,views,rating,page_url,description,slug,archived FROM videos WHERE id=?",
        (video_id,)
    ).fetchone()
    conn.close()
    if not row: return jsonify({"error":"Not found"}), 404
    if row["archived"]: return jsonify({"error":"This video has been removed"}), 410
    return jsonify(dict(row))


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


# ── Video proxy ───────────────────────────────────────────────────────────────
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
