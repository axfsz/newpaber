#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
World News -> Telegram
- Google News 强力解析为“出版社直链”（支持 ?url= / CBMi… urlsafe-b64 / 解析 gnews HTML）
- 优先抓 publisher 页 og:image，避免 G 占位图
- 所有媒体先落地 data/ 再上传（缓存 7 天自动清理）
- 相册 + 中英双语标题 + 推送列表 + 广告位（/ad_ 命令）
- 群发言统计（≥3字符），日报/月报
"""

import argparse
import base64
import hashlib
import json
import logging
import mimetypes
import os
import re
import signal
import sqlite3
import sys
import time
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple
from urllib.parse import quote, urljoin, urlparse, parse_qs, unquote

import feedparser
import pytz
import requests
from dateutil import parser as dateparser
from dotenv import load_dotenv

# Optional for robust OG parsing
try:
    from bs4 import BeautifulSoup  # type: ignore
    HAS_BS4 = True
except Exception:
    HAS_BS4 = False

# ===================== .env =====================
load_dotenv(dotenv_path=os.getenv("ENV_FILE", ".env"), override=True)

def env_bool(name: str, default: bool) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return v.strip().lower() in ("1", "true", "yes", "on")

# ---------- Basics ----------
LOCAL_TZ = os.getenv("LOCAL_TZ", "Asia/Shanghai")
NEWS_LANG = os.getenv("NEWS_LANG", "en-US")
NEWS_GEO = os.getenv("NEWS_GEO", "US")
NEWS_CEID = os.getenv("NEWS_CEID", "US:en")
MAX_ITEMS_PER_PUSH = int(os.getenv("MAX_ITEMS_PER_PUSH", "6"))
DB_PATH = os.getenv("DB_PATH", "news_cache.db")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

BILINGUAL = env_bool("BILINGUAL", True)
TRANSLATE_PROVIDER = os.getenv("TRANSLATE_PROVIDER", "googletrans")  # googletrans | libre | none
LIBRE_TRANSLATE_URL = os.getenv("LIBRE_TRANSLATE_URL", "")
LIBRE_TRANSLATE_API_KEY = os.getenv("LIBRE_TRANSLATE_API_KEY", "")

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")


# ---- Webhook Off (确保轮询接管) ----
def ensure_polling_mode():
    try:
        requests.get(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/deleteWebhook",
                     params={"drop_pending_updates": "true"}, timeout=10)
    except Exception:
        pass

# ---------- Media / Album ----------
MEDIA_ONLY = env_bool("MEDIA_ONLY", True)
ENABLE_OG_SCRAPE = env_bool("ENABLE_OG_SCRAPE", True)
FOLLOW_REDIRECTS_FOR_MEDIA = env_bool("FOLLOW_REDIRECTS_FOR_MEDIA", True)
ALBUM_SUMMARY_BELOW = env_bool("ALBUM_SUMMARY_BELOW", True)
ALBUM_CAPTION_NUMBERING = env_bool("ALBUM_CAPTION_NUMBERING", True)
ALWAYS_TRY_OG_REPLACE = env_bool("ALWAYS_TRY_OG_REPLACE", True)
OG_FETCH_TIMEOUT = int(os.getenv("OG_FETCH_TIMEOUT", "8"))
MAX_OG_FETCH_PER_CYCLE = int(os.getenv("MAX_OG_FETCH_PER_CYCLE", "60"))
USER_AGENT = os.getenv(
    "USER_AGENT",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122 Safari/537.36",
)
ALBUM_MAX = 10  # Telegram 限制

# ---------- 本地缓存目录（持久化） ----------
DATA_DIR = os.getenv("DATA_DIR", "data")
IMAGES_DIR = os.path.join(DATA_DIR, "images")
VIDEOS_DIR = os.path.join(DATA_DIR, "videos")
MEDIA_RETENTION_DAYS = int(os.getenv("MEDIA_RETENTION_DAYS", "7"))
IMAGE_MAX_BYTES = int(os.getenv("IMAGE_MAX_BYTES", "0")) or None  # None=不限制
VIDEO_MAX_BYTES = int(os.getenv("VIDEO_MAX_BYTES", "0")) or None

# 识别为占位图的常见域名
PLACEHOLDER_DOMAINS = set(
    os.getenv(
        "PLACEHOLDER_IMAGE_DOMAINS",
        ",".join(
            [
                "encrypted-tbn0.gstatic.com",
                "tbn0.gstatic.com",
                "gstatic.com",
                "news.google.com",
                "news.googleusercontent.com",
                "lh3.googleusercontent.com",
                "lh4.googleusercontent.com",
                "lh5.googleusercontent.com",
                "lh6.googleusercontent.com",
                "gnews.google.com",
                "googleusercontent.com",
            ]
        ),
    )
    .lower()
    .split(",")
)

# ---------- Ads ----------
AD_ENABLED_DEFAULT = env_bool("AD_ENABLED", True)
AD_DEFAULT_HTML = os.getenv("AD_DEFAULT_HTML", "")
AD_DISABLE_WEB_PREVIEW = env_bool("AD_DISABLE_WEB_PREVIEW", False)
ADMIN_USER_IDS = {int(x) for x in os.getenv("ADMIN_USER_IDS", "").strip().split(",") if x.strip().isdigit()}

# ---------- Stats ----------
STATS_ENABLED = env_bool("STATS_ENABLED", True)
STATS_POLL_INTERVAL_SEC = int(os.getenv("STATS_POLL_INTERVAL_SEC", "10"))
STATS_POLL_LIMIT = int(os.getenv("STATS_POLL_LIMIT", "100"))
STATS_INCLUDE_BOTS = env_bool("STATS_INCLUDE_BOTS", False)
MIN_MSG_CHARS = int(os.getenv("MIN_MSG_CHARS", "3"))
DAILY_STATS_TIME = os.getenv("DAILY_STATS_TIME", "23:59")
MONTHLY_STATS_TIME = os.getenv("MONTHLY_STATS_TIME", "09:10")

# ---------- Categories ----------
CATEGORY_QUERIES: Dict[str, List[str]] = {
    "sea": [
        "Southeast Asia",
        "Vietnam OR Thailand OR Cambodia OR Singapore OR Malaysia OR Indonesia OR Philippines",
        "东南亚 OR 越南 OR 泰国 OR 柬埔寨 OR 新加坡 OR 马来西亚 OR 印度尼西亚 OR 菲律宾",
    ],
    "finance": [
        "global markets OR equities OR bonds OR commodities OR crypto",
        "Federal Reserve OR interest rates OR inflation OR CPI OR PPI OR dollar",
        "财经 OR 金融 OR 利率 OR 通胀 OR 美联储 OR 汇率 OR 股票 OR 债券 OR 大宗商品 OR 加密货币",
    ],
    "war": [
        "war OR conflict OR fighting OR offensive OR strikes",
        "战争 OR 冲突 OR 交火 OR 前线 OR 停火 OR 以色列 OR 乌克兰 OR 加沙 OR 红海 OR 台海",
    ],
}

# ===================== Utils =====================
def ensure_data_dirs():
    os.makedirs(IMAGES_DIR, exist_ok=True)
    os.makedirs(VIDEOS_DIR, exist_ok=True)

def cleanup_data_dir():
    """删除超过 MEDIA_RETENTION_DAYS 天的缓存文件。"""
    if MEDIA_RETENTION_DAYS <= 0:
        return
    cutoff = time.time() - MEDIA_RETENTION_DAYS * 86400
    for folder in (IMAGES_DIR, VIDEOS_DIR):
        try:
            for name in os.listdir(folder):
                path = os.path.join(folder, name)
                try:
                    if os.path.isfile(path) and os.path.getmtime(path) < cutoff:
                        os.remove(path)
                except Exception:
                    pass
        except FileNotFoundError:
            pass

def google_news_rss(q: str) -> str:
    return f"https://news.google.com/rss/search?q={quote(q)}&hl={NEWS_LANG}&gl={NEWS_GEO}&ceid={NEWS_CEID}"

def utcnow() -> datetime:
    return datetime.now(timezone.utc)

def tz_now() -> datetime:
    return datetime.now(pytz.timezone(LOCAL_TZ))

def parse_entry_datetime(entry) -> datetime:
    for k in ("published", "updated", "created"):
        v = entry.get(k)
        if v:
            try:
                dt = dateparser.parse(v)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt.astimezone(timezone.utc)
            except Exception:
                pass
    if getattr(entry, "published_parsed", None):
        try:
            return datetime.fromtimestamp(time.mktime(entry.published_parsed), tz=timezone.utc)
        except Exception:
            pass
    return utcnow()

def safe_html(s: str) -> str:
    return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

def fmt_dt_local(dt: datetime) -> str:
    return dt.astimezone(pytz.timezone(LOCAL_TZ)).strftime("%Y-%m-%d %H:%M")

def is_placeholder_image(url: str) -> bool:
    try:
        host = urlparse(url).netloc.lower()
        return any(d and d in host for d in PLACEHOLDER_DOMAINS)
    except Exception:
        return False

# ---- Google News 直链解析 ----
def _try_b64_http(s: str) -> Optional[str]:
    """尝试把 urlsafe-base64 字符串解码为 http(s) 文本。"""
    try:
        t = s + ("=" * ((4 - len(s) % 4) % 4))
        raw = base64.urlsafe_b64decode(t.encode("ascii"))
        txt = raw.decode("utf-8", "ignore").strip()
        if txt.startswith("http"):
            return txt
    except Exception:
        pass
    return None

def decode_gnews_articles(url: str) -> Optional[str]:
    """
    处理 https://news.google.com/articles/CBMi...：
    在 token 内扫描 urlsafe-b64 子串，解出以 http 开头的真实 URL。
    """
    if "news.google." not in (url or "") or "/articles/" not in (url or ""):
        return None
    try:
        token = urlparse(url).path.split("/articles/", 1)[1].split("?", 1)[0]
        got = _try_b64_http(token)
        if got:
            return got
        # 扫描子串
        n = len(token)
        alphabet = set("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_")
        for i in range(n):
            if token[i] not in alphabet:
                continue
            for j in range(i + 16, min(i + 200, n) + 1):
                sub = token[i:j]
                if any(ch not in alphabet for ch in sub):
                    break
                got = _try_b64_http(sub)
                if got:
                    return got
    except Exception:
        return None
    return None

def extract_direct_from_gnews(url: str) -> Optional[str]:
    """从 gnews 链接的 ?url= 参数取直链。"""
    try:
        qs = parse_qs(urlparse(url).query)
        v = (qs.get("url") or [None])[0]
        if not v:
            return None
        v = unquote(v)
        if v.startswith("http"):
            return v
    except Exception:
        pass
    return None

def _is_google_host(host: str) -> bool:
    host = host.lower()
    bad_hosts = (
        "news.google.",           # news.google.com/...
        ".google.com", "google.com",
        ".googleapis.com", "googleapis.com",  # fonts.googleapis.com 等
        ".gstatic.com", "gstatic.com",
        ".googleusercontent.com", "googleusercontent.com",
        "fonts.googleapis.com", "fonts.gstatic.com",
    )
    return any(host.endswith(h) or h in host for h in bad_hosts)

def _valid_external_url(u: str) -> bool:
    """是否是一个可作为‘出版社直链’的候选 URL。"""
    u = u.replace("\\/", "/")
    if not u.startswith(("http://", "https://")):
        return False
    try:
        p = urlparse(u)
        host = p.netloc.lower()
        path = (p.path or "").lower()
    except Exception:
        return False

    if _is_google_host(host):
        return False

    # 明显的静态资源与字体资源（目录式或扩展名）
    if path in ("/css", "/js", "/image", "/images", "/static", "/assets"):
        return False
    if re.search(r'\.(?:jpg|jpeg|png|gif|webp|svg|ico|css|js|woff2?|ttf|otf)(?:\?|#|$)', path, flags=re.I):
        return False

    return True

def extract_publisher_from_gnews_html(url: str) -> Optional[str]:
    """抓 gnews 文章页，从 HTML 中提取外部真实正文链接（忽略 Google/Fonts 等域名）。"""
    headers = {"User-Agent": USER_AGENT, "Accept": "text/html,application/xhtml+xml"}
    try:
        r = requests.get(url, headers=headers, timeout=OG_FETCH_TIMEOUT)
        if r.status_code != 200 or "text/html" not in (r.headers.get("Content-Type", "")):
            return None
        html = r.text or ""

        # 1) 有 BeautifulSoup 就先从 <a href> 提取
        if HAS_BS4:
            soup = BeautifulSoup(html, "html.parser")
            for a in soup.find_all("a", href=True):
                href = urljoin(url, a.get("href"))
                if _valid_external_url(href):
                    logging.debug("gnews html anchor -> %s", href)
                    return href

        # 2) JSON 中的 "url": "https://..."
        for m in re.findall(r'"url"\s*:\s*"([^"]+)"', html):
            u = m.replace("\\/", "/")
            if _valid_external_url(u):
                logging.debug("gnews html json url -> %s", u)
                return u

        # 3) 兜底：任意 http(s)
        for m in re.findall(r'https?://[^\s"\'<>]+', html):
            u = m.replace("\\/", "/")
            if _valid_external_url(u):
                logging.debug("gnews html any http -> %s", u)
                return u
    except Exception as e:
        logging.debug("extract_publisher_from_gnews_html error: %s", e)
        return None
    return None

def publisher_url_from_entry(entry) -> str:
    """
    综合多途径拿“出版社直链”：
    1) entry.link / entry.links 中非 news.google 域名
    2) gnews ?url= 参数
    3) /articles/CBMi... token 解码
    4) summary 里的首个外链
    5) 在线抓 gnews HTML，解析外站链接
    """
    candidates = []
    if entry.get("link"):
        candidates.append(entry.get("link"))
    for l in (entry.get("links") or []):
        href = (l.get("href") or "").strip()
        if href:
            candidates.append(href)

    for u in candidates:
        if "news.google." not in u:
            logging.debug("publisher_url_from_entry: non-gnews %s", u)
            return u

    for u in candidates:
        real = extract_direct_from_gnews(u)
        if real:
            logging.debug("publisher_url_from_entry: url= -> %s", real)
            return real

    for u in candidates:
        real = decode_gnews_articles(u)
        if real:
            logging.debug("publisher_url_from_entry: CBMi decoded -> %s", real)
            return real

    summary = entry.get("summary", "") or entry.get("description", "")
    for href in re.findall(r'href=["\']([^"\']+)["\']', summary, flags=re.I):
        if href.startswith("http") and "news.google." not in href:
            logging.debug("publisher_url_from_entry: summary link -> %s", href)
            return href

    for u in candidates:
        if "news.google." in u:
            real = extract_publisher_from_gnews_html(u)
            if real:
                logging.debug("publisher_url_from_entry: html extract -> %s", real)
                return real

    fallback = candidates[0] if candidates else ""
    logging.debug("publisher_url_from_entry: fallback %s", fallback)
    return fallback

# ===================== DB =====================
def init_db(conn: sqlite3.Connection):
    conn.execute(
        """CREATE TABLE IF NOT EXISTS sent_articles (
        id TEXT PRIMARY KEY, title TEXT, link TEXT, category TEXT, sent_at TEXT
    )"""
    )
    conn.execute("""CREATE TABLE IF NOT EXISTS tg_state (key TEXT PRIMARY KEY, value TEXT)""")
    conn.execute("""CREATE TABLE IF NOT EXISTS ads (scope TEXT PRIMARY KEY, content TEXT, updated_at TEXT)""")
    conn.execute(
        """CREATE TABLE IF NOT EXISTS msg_counts (
        day TEXT, chat_id INTEGER, user_id INTEGER,
        username TEXT, first_name TEXT, last_name TEXT,
        count INTEGER DEFAULT 0,
        PRIMARY KEY (day, chat_id, user_id)
    )"""
    )
    conn.execute("""CREATE TABLE IF NOT EXISTS stats_runs (kind TEXT, period TEXT, sent_at TEXT, PRIMARY KEY(kind,period))""")
    conn.commit()

def make_id(t: str, l: str) -> str:
    return hashlib.sha1(f"{t}|{l}".encode("utf-8")).hexdigest()

def get_state(conn: sqlite3.Connection, key: str, default: Optional[str] = None) -> Optional[str]:
    row = conn.execute("SELECT value FROM tg_state WHERE key=?", (key,)).fetchone()
    return row[0] if row else default

def set_state(conn: sqlite3.Connection, key: str, value: str):
    conn.execute(
        "INSERT INTO tg_state(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", (key, value)
    )
    conn.commit()

def already_sent(conn: sqlite3.Connection, aid: str) -> bool:
    return conn.execute("SELECT 1 FROM sent_articles WHERE id=?", (aid,)).fetchone() is not None

def mark_sent(conn: sqlite3.Connection, aid: str, title: str, link: str, category: str):
    conn.execute(
        "INSERT OR IGNORE INTO sent_articles VALUES(?,?,?,?,?)",
        (aid, title, link, category, utcnow().isoformat()),
    )
    conn.commit()

def ad_enabled(conn: sqlite3.Connection) -> bool:
    v = get_state(conn, "ad_enabled", None)
    return (v == "1") if v is not None else AD_ENABLED_DEFAULT

def get_ad(conn: sqlite3.Connection, scope: str) -> str:
    r = conn.execute("SELECT content FROM ads WHERE scope=?", (scope,)).fetchone()
    if r and r[0]:
        return r[0]
    r = conn.execute("SELECT content FROM ads WHERE scope='global'").fetchone()
    return (r[0] if r and r[0] else AD_DEFAULT_HTML) or ""

def set_ad(conn: sqlite3.Connection, scope: str, content: str):
    conn.execute(
        "INSERT INTO ads(scope,content,updated_at) VALUES(?,?,?) "
        "ON CONFLICT(scope) DO UPDATE SET content=excluded.content, updated_at=excluded.updated_at",
        (scope, content, utcnow().isoformat()),
    )
    conn.commit()

def clear_ad(conn: sqlite3.Connection, scope: str):
    conn.execute("DELETE FROM ads WHERE scope=?", (scope,))
    conn.commit()

# ===================== Media extraction =====================
VIDEO_EXT_RE = re.compile(r"\.(mp4|mov|m4v|webm)(\?|#|$)", re.I)

def _first_ok_url(urls: List[str]) -> Optional[str]:
    for u in urls:
        if u and u.startswith(("http://", "https://")) and not u.lower().startswith("data:"):
            return u
    return None

def resolve_publisher_url(url: str) -> str:
    if not FOLLOW_REDIRECTS_FOR_MEDIA:
        return url
    headers = {"User-Agent": USER_AGENT}
    try:
        r = requests.get(url, headers=headers, timeout=OG_FETCH_TIMEOUT, allow_redirects=True, stream=True)
        final = r.url or url
        if "text/html" in (r.headers.get("Content-Type", "")) and HAS_BS4:
            try:
                html = r.text
                soup = BeautifulSoup(html, "html.parser")
                can = soup.find("link", rel=lambda x: x and "canonical" in x.lower())
                if can and can.get("href"):
                    final = urljoin(final, can["href"])
                og = soup.find("meta", attrs={"property": "og:url"})
                if og and og.get("content"):
                    final = urljoin(final, og["content"])
            except Exception:
                pass
        return final
    except Exception:
        return url

def extract_media_from_entry(entry) -> Tuple[Optional[str], Optional[str]]:
    imgs, vids = [], []
    for k in ("media_content", "media_thumbnail"):
        for m in entry.get(k, []) or []:
            u = m.get("url") or m.get("href")
            t = (m.get("type") or "").lower()
            if not u:
                continue
            if t.startswith("video") or VIDEO_EXT_RE.search(u):
                vids.append(u)
            else:
                imgs.append(u)
    for enc in entry.get("enclosures", []) or []:
        u = enc.get("href") or enc.get("url")
        t = (enc.get("type") or "").lower()
        if not u:
            continue
        if t.startswith("video") or VIDEO_EXT_RE.search(u):
            vids.append(u)
        elif t.startswith("image") or not t:
            imgs.append(u)
    summary = entry.get("summary", "") or entry.get("description", "")
    if summary:
        for u in re.findall(r'<img[^>]+src=["\']([^"\']+)["\']', summary, flags=re.I):
            imgs.append(u)
    return _first_ok_url(imgs), _first_ok_url(vids)

def fetch_og_image(article_url: str, timeout: int = OG_FETCH_TIMEOUT) -> Optional[str]:
    if not ENABLE_OG_SCRAPE:
        return None
    headers = {"User-Agent": USER_AGENT, "Accept": "text/html,application/xhtml+xml"}
    try:
        r = requests.get(article_url, headers=headers, timeout=timeout)
        if r.status_code != 200 or "text/html" not in (r.headers.get("Content-Type", "")):
            return None
        html = r.text or ""
        if HAS_BS4:
            soup = BeautifulSoup(html, "html.parser")
            for sel in [
                ('meta[property="og:image"]', "content"),
                ('meta[property="og:image:url"]', "content"),
                ('meta[name="twitter:image"]', "content"),
            ]:
                tag = soup.select_one(sel[0])
                if tag and tag.get(sel[1]):
                    return urljoin(article_url, tag.get(sel[1]))
        m = re.search(r'<meta[^>]+property=["\']og:image["\'][^>]+content=["\']([^"\']+)["\']', html, flags=re.I)
        if m:
            return urljoin(article_url, m.group(1))
        m = re.search(r'<meta[^>]+name=["\']twitter:image["\'][^>]+content=["\']([^"\']+)["\']', html, flags=re.I)
        if m:
            return urljoin(article_url, m.group(1))
    except Exception:
        return None
    return None

# ===================== Download to data/ =====================
def _pick_ext_by_ct(ct: str, is_video: bool) -> str:
    ct = (ct or "").lower()
    if is_video:
        if "mp4" in ct:
            return ".mp4"
        if "webm" in ct:
            return ".webm"
        if "quicktime" in ct or "mov" in ct:
            return ".mov"
        return ".mp4"
    else:
        if "jpeg" in ct or "jpg" in ct:
            return ".jpg"
        if "png" in ct:
            return ".png"
        if "webp" in ct:
            return ".webp"
        if "gif" in ct:
            return ".gif"
        return ".jpg"

def _pick_ext_by_url(url: str, is_video: bool) -> Optional[str]:
    path = urlparse(url).path.lower()
    for ext in ((".mp4", ".webm", ".mov", ".m4v") if is_video else (".jpg", ".jpeg", ".png", ".webp", ".gif")):
        if path.endswith(ext):
            return ext
    return None

def _find_existing_path(key: str, is_video: bool) -> Optional[str]:
    folder = VIDEOS_DIR if is_video else IMAGES_DIR
    if not os.path.isdir(folder):
        return None
    for name in os.listdir(folder):
        if name.startswith(key + ".") or name.startswith(key + "_"):
            return os.path.join(folder, name)
    return None

def download_to_data(url: str, key: str, is_video: bool, size_limit: Optional[int]) -> Optional[str]:
    """
    按 key 下载到 data/ 子目录。命中已缓存文件则直接复用。返回本地绝对路径。
    """
    ensure_data_dirs()

    exist = _find_existing_path(key, is_video)
    if exist and os.path.getsize(exist) > 0:
        logging.debug("cache hit: %s", exist)
        return os.path.abspath(exist)

    headers = {"User-Agent": USER_AGENT}
    folder = VIDEOS_DIR if is_video else IMAGES_DIR
    os.makedirs(folder, exist_ok=True)

    ext = _pick_ext_by_url(url, is_video) or (".mp4" if is_video else ".jpg")
    tmp_path = os.path.join(folder, f"{key}.part")
    final_path = os.path.join(folder, f"{key}{ext}")

    try:
        with requests.get(url, headers=headers, timeout=120 if is_video else 60, stream=True) as r:
            r.raise_for_status()
            ct = r.headers.get("Content-Type", "").lower()
            if "text/html" in ct:
                logging.debug("download_to_data got HTML for %s", url)
                return None
            ext2 = _pick_ext_by_ct(ct, is_video)
            final_path = os.path.join(folder, f"{key}{ext2}")

            written = 0
            with open(tmp_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=1 << 16):
                    if not chunk:
                        continue
                    f.write(chunk)
                    written += len(chunk)
                    if size_limit and written > size_limit:
                        try:
                            os.remove(tmp_path)
                        except Exception:
                            pass
                        logging.debug("download_to_data exceeded limit for %s", url)
                        return None
        os.replace(tmp_path, final_path)
        logging.debug("downloaded -> %s", final_path)
        return os.path.abspath(final_path)
    except Exception as e:
        logging.debug("download failed %s: %s", url, e)
        try:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
        except Exception:
            pass
        return None

# ===================== Fetch news =====================
def fetch_category_news(category: str, lookback_minutes: int) -> List[Dict]:
    cutoff = utcnow() - timedelta(minutes=lookback_minutes)
    items: List[Dict] = []
    for q in CATEGORY_QUERIES.get(category, []):
        feed = feedparser.parse(google_news_rss(q))
        for e in getattr(feed, "entries", []):
            title = e.get("title") or ""
            link = e.get("link") or ""
            if not title or not link:
                continue
            dt = parse_entry_datetime(e)
            if dt < cutoff:
                continue
            source = ""
            if "source" in e and e["source"] and "title" in e["source"]:
                source = e["source"]["title"]
            elif "author" in e:
                source = e.get("author", "")
            img, vid = extract_media_from_entry(e)
            pub_link = publisher_url_from_entry(e)  # 解析直链
            items.append(
                {
                    "title": title.strip(),
                    "link": link.strip(),
                    "publisher_link": pub_link,
                    "dt": dt,
                    "source": (source or "").strip(),
                    "category": category,
                    "img": img,
                    "vid": vid,
                }
            )
    dedup: Dict[Tuple[str, str], Dict] = {}
    for it in items:
        k = (it["title"], it["link"])
        if k not in dedup:
            dedup[k] = it
    ordered = sorted(dedup.values(), key=lambda x: x["dt"], reverse=True)
    return ordered[:MAX_ITEMS_PER_PUSH]

# ===================== Translation / captions =====================
class SimpleTranslator:
    def __init__(self, provider: str = "googletrans"):
        self.provider = provider
        self.cache: Dict[str, str] = {}
        self._gt = None
        if provider == "googletrans":
            try:
                from googletrans import Translator as GT
                self._gt = GT()
            except Exception as e:
                logging.warning("googletrans not available: %s", e)
                self.provider = "libre" if LIBRE_TRANSLATE_URL else "none"

    def translate(self, text: str, target: str = "zh") -> str:
        if not text:
            return text
        if text in self.cache:
            return self.cache[text]
        out = text
        try:
            if self.provider == "googletrans" and self._gt:
                out = self._gt.translate(text, dest="zh-cn").text
            elif self.provider == "libre" and LIBRE_TRANSLATE_URL:
                payload = {"q": text, "source": "auto", "target": "zh", "format": "text"}
                if LIBRE_TRANSLATE_API_KEY:
                    payload["api_key"] = LIBRE_TRANSLATE_API_KEY
                r = requests.post(LIBRE_TRANSLATE_URL, data=payload, timeout=OG_FETCH_TIMEOUT)
                if r.status_code == 200 and "translatedText" in r.json():
                    out = r.json()["translatedText"]
        except Exception:
            out = text
        self.cache[text] = out
        return out

def truncate(s: str, n: int) -> str:
    return s if len(s) <= n else (s[: max(0, n - 1)] + "…")

def build_caption(idx: int, item: Dict, tr: Optional[SimpleTranslator]) -> str:
    t_en = item["title"]
    t_zh = tr.translate(t_en) if (BILINGUAL and tr) else None
    main = t_zh or t_en
    prefix = f"{idx}. " if ALBUM_CAPTION_NUMBERING else ""
    if BILINGUAL and t_zh and t_zh.strip() and t_zh.strip() != t_en.strip():
        cap = f"{prefix}<b>{safe_html(truncate(main, 300))}</b>\n<i>EN:</i> {safe_html(truncate(t_en, 300))}"
    else:
        cap = f"{prefix}<b>{safe_html(truncate(main, 600))}</b>"
    when = fmt_dt_local(item["dt"])
    src = f" · 📰 {safe_html(item['source'])}" if item["source"] else ""
    return truncate(cap + f"\n🕒 {when}{src}", 950)

def category_header(cat: str) -> str:
    icon = {"sea": "🌏", "finance": "💹", "war": "⚔️"}.get(cat, "🗞️")
    zh = {"sea": "东南亚", "finance": "财经", "war": "战争"}.get(cat, cat)
    en = {"sea": "Southeast Asia", "finance": "Finance", "war": "War"}.get(cat, cat)
    return f"{icon} <b>{zh} · {en}</b>\n<code>────────────────────────────────</code>"

# ===================== Telegram senders =====================
def send_message_html(text: str, disable_preview: bool = True) -> Tuple[bool, str]:
    api = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    data = {"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "HTML", "disable_web_page_preview": disable_preview}
    try:
        r = requests.post(api, data=data, timeout=30)
        return (r.status_code == 200, r.text if r.status_code != 200 else "ok")
    except Exception as e:
        return False, str(e)

def send_media_group_with_paths(media_list: List[dict], path_map: Dict[str, Tuple[str, str]]) -> Tuple[bool, str]:
    api = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMediaGroup"
    files = {}
    try:
        for key, (path, fname) in path_map.items():
            mime = mimetypes.guess_type(fname)[0] or ("video/mp4" if path.endswith(".mp4") else "image/jpeg")
            files[key] = (fname, open(path, "rb"), mime)
        payload = {"chat_id": TELEGRAM_CHAT_ID, "media": json.dumps(media_list, ensure_ascii=False)}
        r = requests.post(api, data=payload, files=files, timeout=600)
        ok = r.status_code == 200
        return ok, (r.text if not ok else "ok")
    finally:
        for key in list(files.keys()):
            try:
                files[key][1].close()
            except Exception:
                pass

def send_single_photo_path(path: str, caption: str) -> Tuple[bool, str]:
    api = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendPhoto"
    fname = os.path.basename(path)
    mime = mimetypes.guess_type(fname)[0] or "image/jpeg"
    files = {"photo": (fname, open(path, "rb"), mime)}
    data = {"chat_id": TELEGRAM_CHAT_ID, "caption": caption, "parse_mode": "HTML"}
    try:
        r = requests.post(api, data=data, files=files, timeout=300)
        return (r.status_code == 200, r.text if r.status_code != 200 else "ok")
    finally:
        try:
            files["photo"][1].close()
        except Exception:
            pass

def send_single_video_path(path: str, caption: str) -> Tuple[bool, str]:
    api = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendVideo"
    fname = os.path.basename(path)
    mime = mimetypes.guess_type(fname)[0] or "video/mp4"
    files = {"video": (fname, open(path, "rb"), mime)}
    data = {"chat_id": TELEGRAM_CHAT_ID, "caption": caption, "parse_mode": "HTML"}
    try:
        r = requests.post(api, data=data, files=files, timeout=1200)
        return (r.status_code == 200, r.text if r.status_code != 200 else "ok")
    finally:
        try:
            files["video"][1].close()
        except Exception:
            pass

# ===================== Album + summary + ads =====================
def build_album_summary(items: List[Dict], tr: Optional[SimpleTranslator]) -> str:
    lines = ["🗞️ <b>本次推送列表</b>", "<code>────────────────</code>"]
    for i, it in enumerate(items, 1):
        t_en = it["title"]
        t_zh = tr.translate(t_en) if (BILINGUAL and tr) else None
        main = t_zh or t_en
        title = safe_html(truncate(main, 120))
        link = safe_html(it.get("publisher_link") or it["link"])
        src = f" · 📰 {safe_html(it['source'])}" if it["source"] else ""
        when = fmt_dt_local(it["dt"])
        lines.append(f"{i}. <a href=\"{link}\"><b>{title}</b></a>\n   🕒 {when}{src}")
        if BILINGUAL and t_zh and t_zh.strip() != t_en.strip():
            lines.append(f"   <i>EN:</i> {safe_html(truncate(t_en, 140))}")
    return "\n".join(lines)

def send_album_with_ad(
    conn: sqlite3.Connection, category: str, items: List[Dict], tr: Optional[SimpleTranslator], og_budget_ref: List[int]
):
    """组装相册：始终从 data/ 读取本地文件并上传。"""
    chosen: List[Dict] = []

    for it in items:
        aid = make_id(it["title"], it["link"])
        if already_sent(conn, aid):
            continue

        # 1) 解析/跟随，拿 publisher 直链
        final_link = it.get("publisher_link") or it.get("link") or ""
        if "news.google." in (final_link or ""):
            cand = extract_publisher_from_gnews_html(final_link)
            if cand:
                final_link = cand
            else:
                final_link = resolve_publisher_url(final_link)
        it["publisher_link"] = final_link
        logging.debug("publisher=%s", final_link)

        # 2) 媒体优先级：视频 > 图片；图片若为空/占位 -> 抓 og:image（并用 -og 键缓存）
        img, vid = it.get("img"), it.get("vid")
        if img and is_placeholder_image(img):
            img = None
            it["img"] = None

        used_og = False
        if (not vid) and ENABLE_OG_SCRAPE and final_link and og_budget_ref[0] > 0:
            og = fetch_og_image(final_link)
            if og:
                it["img"] = img = og
                used_og = True
                logging.debug("og:image -> %s", og)
            og_budget_ref[0] -= 1

        if MEDIA_ONLY and not (img or vid):
            logging.debug("skip no-media: %s", it["title"])
            continue

        # 3) 统一落地到 data/ 再上传（区分键，避免复用旧的 G 图缓存）
        local_path = None
        if vid:
            local_path = download_to_data(vid, f"{aid}-vid", True, VIDEO_MAX_BYTES)
        if not local_path and img:
            key = f"{aid}-og" if used_og else f"{aid}-img"
            local_path = download_to_data(img, key, False, IMAGE_MAX_BYTES)

        if not local_path and ENABLE_OG_SCRAPE and final_link:
            og = fetch_og_image(final_link)
            if og:
                local_path = download_to_data(og, f"{aid}-og", False, IMAGE_MAX_BYTES)

        if not local_path:
            logging.debug("still no media, drop: %s", it["title"])
            continue

        it["_local_path"] = local_path
        it["_is_video"] = local_path.startswith(os.path.abspath(VIDEOS_DIR))
        chosen.append(it)

    if not chosen:
        return

    # 分类标题
    send_message_html(category_header(category), disable_preview=True)

    # 分批上传
    sent_any = False

    def flush_batch(batch: List[Dict]):
        nonlocal sent_any
        if not batch:
            return

        if len(batch) == 1:
            itx = batch[0]
            cap = build_caption(1, itx, tr)
            if itx["_is_video"]:
                ok, _ = send_single_video_path(itx["_local_path"], cap)
            else:
                ok, _ = send_single_photo_path(itx["_local_path"], cap)
            if ok:
                mark_sent(conn, make_id(itx["title"], itx["link"]), itx["title"], itx["link"], category)
                sent_any = True
            time.sleep(0.6)
            return

        media_payload: List[dict] = []
        attachments: Dict[str, Tuple[str, str]] = {}
        for idx, itx in enumerate(batch, 1):
            cap = build_caption(idx, itx, tr)
            key = f"file{idx}"
            fname = os.path.basename(itx["_local_path"])
            attachments[key] = (itx["_local_path"], fname)
            media_payload.append(
                {
                    "type": "video" if itx["_is_video"] else "photo",
                    "media": f"attach://{key}",
                    "caption": cap,
                    "parse_mode": "HTML",
                }
            )
        ok, msg = send_media_group_with_paths(media_payload, attachments)
        if ok:
            for it2 in batch:
                mark_sent(conn, make_id(it2["title"], it2["link"]), it2["title"], it2["link"], category)
            sent_any = True
        else:
            logging.warning("Album send failed: %s", msg)
        time.sleep(0.6)

    cur: List[Dict] = []
    for it in chosen:
        cur.append(it)
        if len(cur) == ALBUM_MAX:
            flush_batch(cur)
            cur = []
    flush_batch(cur)

    # 编号清单 + 广告
    if sent_any and ALBUM_SUMMARY_BELOW:
        summary = build_album_summary(chosen, tr)
        send_message_html(summary, disable_preview=True)
    if sent_any and ad_enabled(conn):
        ad_html = get_ad(conn, category)
        if ad_html.strip():
            send_message_html(ad_html, disable_preview=AD_DISABLE_WEB_PREVIEW)

# ===================== Push once =====================
def push_once(conn: sqlite3.Connection, lookback_minutes: int, tr: Optional[SimpleTranslator]) -> None:
    cleanup_data_dir()  # 每轮推送前清理过期缓存
    og_budget = [MAX_OG_FETCH_PER_CYCLE]
    for cat in ["sea", "finance", "war"]:
        items = fetch_category_news(cat, lookback_minutes)
        if items:
            send_album_with_ad(conn, cat, items, tr, og_budget)

# ===================== Updates / Admin / Stats / Reports =====================
def get_target_chat_id_int() -> Optional[int]:
    try:
        return int(TELEGRAM_CHAT_ID)
    except Exception:
        return None

def tg_get_updates(offset: Optional[int], timeout: int = 0, limit: int = 100) -> Tuple[bool, dict]:
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getUpdates"
    params = {"timeout": timeout, "limit": limit, "allowed_updates": '["message","callback_query","chat_member","my_chat_member"]'}
    if offset is not None:
        params["offset"] = offset
    try:
        r = requests.get(url, params=params, timeout=timeout + 15)
        return True, r.json()
    except Exception as e:
        return False, {"error": str(e)}

def is_admin(uid: Optional[int]) -> bool:
    return uid is not None and (uid in ADMIN_USER_IDS or len(ADMIN_USER_IDS) == 0)

def get_countable_text(msg: dict) -> str:
    if not isinstance(msg, dict):
        return ""
    if msg.get("text"):
        return msg["text"]
    if msg.get("caption"):
        return msg["caption"]
    return ""

def handle_admin_command(conn: sqlite3.Connection, msg: dict) -> bool:
    txt = msg.get("text") or ""
    if not txt.startswith("/ad_"):
        return False
    uid = (msg.get("from") or {}).get("id")
    if not is_admin(uid):
        send_message_html("❌ 你没有权限编辑广告。")
        return True

    parts = txt.strip().split(maxsplit=2)
    cmd = parts[0]
    scopes = {"global", "sea", "finance", "war"}
    if cmd == "/ad_help":
        send_message_html(
            "🧩 <b>广告管理命令</b>\n"
            "/ad_set &lt;scope&gt; &lt;HTML&gt;\n"
            "/ad_show [scope]\n"
            "/ad_clear &lt;scope&gt;\n"
            "/ad_enable | /ad_disable",
            True,
        )
        return True
    if cmd == "/ad_enable":
        set_state(conn, "ad_enabled", "1")
        send_message_html("✅ 广告位已开启")
        return True
    if cmd == "/ad_disable":
        set_state(conn, "ad_enabled", "0")
        send_message_html("✅ 广告位已关闭")
        return True
    if cmd == "/ad_show":
        scope = parts[1] if len(parts) >= 2 else "global"
        if scope not in scopes:
            send_message_html("❗scope 需为 global/sea/finance/war")
            return True
        send_message_html(get_ad(conn, scope) or "(空)", AD_DISABLE_WEB_PREVIEW)
        return True
    if cmd == "/ad_clear" and len(parts) >= 2:
        scope = parts[1]
        if scope not in scopes:
            send_message_html("❗scope 需为 global/sea/finance/war")
            return True
        clear_ad(conn, scope)
        send_message_html(f"🧹 已清除 <b>{scope}</b> 广告")
        return True
    if cmd == "/ad_set" and len(parts) >= 3:
        scope = parts[1]
        if scope not in scopes:
            send_message_html("❗scope 需为 global/sea/finance/war")
            return True
        set_ad(conn, scope, parts[2])
        send_message_html(f"✅ 已更新 <b>{scope}</b> 广告")
        return True
    send_message_html("❓命令错误，发送 /ad_help 查看用法")
    return True

def stats_poll_and_count(conn: sqlite3.Connection):
    if not STATS_ENABLED and not ADMIN_USER_IDS:
        return
    chat_id = get_target_chat_id_int()
    if chat_id is None:
        return
    last_update = get_state(conn, "last_update_id", None)
    offset = int(last_update) + 1 if last_update is not None else None
    ok, data = tg_get_updates(offset=offset, timeout=0, limit=STATS_POLL_LIMIT)
    if not ok or not data.get("ok"):
        return
    max_update_id = None
    for upd in data.get("result", []):
        max_update_id = max(max_update_id or 0, upd.get("update_id", 0))
        # callback_query handling
        cb = upd.get("callback_query")
        if cb:
            # stop spinner
            try:
                requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/answerCallbackQuery",
                              data={"callback_query_id": cb.get("id")}, timeout=10)
            except Exception:
                pass
            # echo callback data for diagnosis
            frm = cb.get("from") or {}
            tgt_chat = (cb.get("message") or {}).get("chat", {}).get("id") or frm.get("id")
            data_str = cb.get("data") or ""
            try:
                requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                              data={"chat_id": tgt_chat, "text": f"收到回调：{data_str}"}, timeout=30)
            except Exception:
                pass
            continue
        msg = upd.get("message")
        if not msg or msg.get("chat", {}).get("id") != chat_id:
            continue
        txt = msg.get("text") or ""
        # 诊断命令：/testkb 发送一个内联按钮
        if txt.strip().lower() == "/testkb":
            kb = {"inline_keyboard": [[{"text": "点我试试", "callback_data": "echo::hello"}]]}
            try:
                requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                              data={"chat_id": chat_id, "text": "测试按钮：", "reply_markup": json.dumps(kb, ensure_ascii=False)}, timeout=30)
            except Exception:
                pass
            continue
        if txt.startswith("/ad_"):
            if handle_admin_command(conn, msg):
                continue
        frm = msg.get("from")
        if not frm:
            continue
        if frm.get("is_bot") and not STATS_INCLUDE_BOTS:
            continue
        content = get_countable_text(msg)
        if content.lstrip().startswith("/"):
            continue
        if len((content or "").strip()) < MIN_MSG_CHARS:
            continue
        dt_local = datetime.fromtimestamp(msg.get("date", int(time.time())), tz=timezone.utc).astimezone(
            pytz.timezone(LOCAL_TZ)
        )
        day = dt_local.strftime("%Y-%m-%d")
        conn.execute(
            """INSERT INTO msg_counts(day,chat_id,user_id,username,first_name,last_name,count)
               VALUES (?,?,?,?,?,?,1)
               ON CONFLICT(day,chat_id,user_id) DO UPDATE SET
                 count = count + 1,
                 username=excluded.username, first_name=excluded.first_name, last_name=excluded.last_name""",
            (
                day,
                chat_id,
                frm.get("id"),
                frm.get("username") or "",
                frm.get("first_name") or "",
                frm.get("last_name") or "",
            ),
        )
    if max_update_id is not None:
        set_state(conn, "last_update_id", str(max_update_id))
    conn.commit()

def human_name(username: str, first: str, last: str) -> str:
    if username:
        return f"@{username}"
    nm = (first or "") + (" " + last if last else "")
    return nm.strip() or "Unknown"

def build_daily_report(conn: sqlite3.Connection, day: str) -> Optional[str]:
    chat_id = get_target_chat_id_int()
    if chat_id is None:
        return None
    rows = conn.execute(
        """SELECT user_id,username,first_name,last_name,count FROM msg_counts
           WHERE day=? AND chat_id=? ORDER BY count DESC LIMIT 10""",
        (day, chat_id),
    ).fetchall()
    if not rows:
        return None
    total = conn.execute("SELECT SUM(count) FROM msg_counts WHERE day=? AND chat_id=?", (day, chat_id)).fetchone()[0] or 0
    speakers = conn.execute("SELECT COUNT(*) FROM msg_counts WHERE day=? AND chat_id=?", (day, chat_id)).fetchone()[0] or 0
    head = f"📊 <b>群发言日报 · Daily Top Talkers</b>\n<i>{day} ({LOCAL_TZ})</i>\n<code>────────────────────────</code>\n"
    lines = [head] + [f"{i}. {safe_html(human_name(u, f, l))} — <b>{c}</b> 条 / msgs" for i, (_, u, f, l, c) in enumerate(rows, 1)]
    lines.append(f"\n🧮 合计：<b>{total}</b> 条 · 参与人数：<b>{speakers}</b> 人")
    return "\n".join(lines)

def build_monthly_report(conn: sqlite3.Connection, ym: str) -> Optional[str]:
    chat_id = get_target_chat_id_int()
    if chat_id is None:
        return None
    rows = conn.execute(
        """SELECT user_id,username,first_name,last_name,SUM(count) AS cnt FROM msg_counts
           WHERE day LIKE ? AND chat_id=? GROUP BY user_id,username,first_name,last_name
           ORDER BY cnt DESC LIMIT 10""",
        (ym + "%", chat_id),
    ).fetchall()
    if not rows:
        return None
    total = conn.execute("SELECT SUM(count) FROM msg_counts WHERE day LIKE ? AND chat_id=?", (ym + "%", chat_id)).fetchone()[0] or 0
    speakers = (
        conn.execute("SELECT COUNT(DISTINCT user_id) FROM msg_counts WHERE day LIKE ? AND chat_id=?", (ym + "%", chat_id))
        .fetchone()[0]
        or 0
    )
    head = f"📈 <b>群发言月报 · Monthly Top Talkers</b>\n<i>{ym} ({LOCAL_TZ})</i>\n<code>────────────────────────</code>\n"
    lines = [head] + [f"{i}. {safe_html(human_name(u, f, l))} — <b>{c}</b> 条 / msgs" for i, (_, u, f, l, c) in enumerate(rows, 1)]
    lines.append(f"\n🧮 合计：<b>{total}</b> 条 · 参与人数：<b>{speakers}</b> 人")
    return "\n".join(lines)

def has_run(conn: sqlite3.Connection, kind: str, period: str) -> bool:
    return conn.execute("SELECT 1 FROM stats_runs WHERE kind=? AND period=?", (kind, period)).fetchone() is not None

def mark_run(conn: sqlite3.Connection, kind: str, period: str):
    conn.execute("INSERT OR IGNORE INTO stats_runs(kind,period,sent_at) VALUES(?,?,?)", (kind, period, utcnow().isoformat()))
    conn.commit()

def check_and_send_daily_report(conn: sqlite3.Connection):
    now = tz_now()
    if now.strftime("%H:%M") != DAILY_STATS_TIME:
        return
    yday = (now - timedelta(days=1)).strftime("%Y-%m-%d")
    if has_run(conn, "daily", yday):
        return
    t = build_daily_report(conn, yday)
    if t:
        send_message_html(t, True)
    mark_run(conn, "daily", yday)

def check_and_send_monthly_report(conn: sqlite3.Connection):
    now = tz_now()
    if now.strftime("%H:%M") != MONTHLY_STATS_TIME:
        return
    last_month = (now.replace(day=1) - timedelta(days=1)).strftime("%Y-%m")
    if has_run(conn, "monthly", last_month):
        return
    t = build_monthly_report(conn, last_month)
    if t:
        send_message_html(t, True)
    mark_run(conn, "monthly", last_month)

# ===================== Schedulers =====================
def seconds_until(hhmm: str) -> int:
    tz = pytz.timezone(LOCAL_TZ)
    now = datetime.now(tz)
    hh, mm = map(int, hhmm.split(":"))
    tgt = now.replace(hour=hh, minute=mm, second=0, microsecond=0)
    if tgt <= now:
        tgt += timedelta(days=1)
    return int((tgt - now).total_seconds())

def run_realtime(conn: sqlite3.Connection, interval_minutes: int, tr: Optional[SimpleTranslator]):
    lookback = max(interval_minutes * 2, 60)
    logging.info("Realtime every %d min (lookback %d)", interval_minutes, lookback)
    ensure_data_dirs()
    cleanup_data_dir()

    stop = {"s": False}

    def handler(*_):
        stop["s"] = True

    signal.signal(signal.SIGINT, handler)
    try:
        signal.signal(signal.SIGTERM, handler)
    except Exception:
        pass

    last_news = 0.0
    last_poll = 0.0
    while not stop["s"]:
        now_ts = time.time()
        if now_ts - last_news >= interval_minutes * 60:
            try:
                push_once(conn, lookback, tr)
            except Exception as e:
                logging.exception("News loop error: %s", e)
            last_news = now_ts
        if now_ts - last_poll >= STATS_POLL_INTERVAL_SEC:
            try:
                stats_poll_and_count(conn)
            except Exception as e:
                logging.exception("Stats poll error: %s", e)
            last_poll = now_ts
        try:
            check_and_send_daily_report(conn)
            check_and_send_monthly_report(conn)
        except Exception as e:
            logging.exception("Report error: %s", e)
        time.sleep(1)

def run_digest(conn: sqlite3.Connection, hhmm: str, tr: Optional[SimpleTranslator]):
    logging.info("Digest at %s (%s)", hhmm, LOCAL_TZ)
    ensure_data_dirs()
    cleanup_data_dir()

    stop = {"s": False}

    def handler(*_):
        stop["s"] = True

    signal.signal(signal.SIGINT, handler)
    try:
        signal.signal(signal.SIGTERM, handler)
    except Exception:
        pass
    last_poll = 0.0
    while not stop["s"]:
        if time.time() - last_poll >= STATS_POLL_INTERVAL_SEC:
            try:
                stats_poll_and_count(conn)
            except Exception as e:
                logging.exception("Stats poll error: %s", e)
            last_poll = time.time()
        try:
            check_and_send_daily_report(conn)
            check_and_send_monthly_report(conn)
        except Exception as e:
            logging.exception("Report error: %s", e)
        wait = seconds_until(hhmm)
        for _ in range(min(wait, 60)):
            if stop["s"]:
                break
            time.sleep(1)
        if stop["s"]:
            break
        try:
            push_once(conn, 24 * 60, tr)
        except Exception as e:
            logging.exception("Digest push error: %s", e)

# ===================== main =====================
def main():
    parser = argparse.ArgumentParser(
        description="World News -> Telegram (Local cache + Album + Ads + Stats + Strong GNews decode)"
    )
    parser.add_argument("--mode", choices=["realtime", "digest"], default=os.getenv("MODE", "realtime"))
    parser.add_argument("--interval", type=int, default=int(os.getenv("INTERVAL_MINUTES", "60")))
    parser.add_argument("--digest-time", default=os.getenv("DIGEST_TIME", "09:00"))
    parser.add_argument("--log-level", default=LOG_LEVEL)
    args = parser.parse_args()

    logging.basicConfig(level=getattr(logging, args.log_level.upper(), logging.INFO), format="%(asctime)s %(levelname)s: %(message)s")

    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        logging.error("请在 .env 中设置 TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID")
        sys.exit(1)

    ensure_polling_mode()

    conn = sqlite3.connect(DB_PATH)
    init_db(conn)

    tr: Optional[SimpleTranslator] = None
    if BILINGUAL and TRANSLATE_PROVIDER.lower() != "none":
        tr = SimpleTranslator(TRANSLATE_PROVIDER.lower())
        logging.info("Bilingual ON, provider=%s", tr.provider)
    else:
        logging.info("Bilingual OFF")

    if args.mode == "realtime":
        run_realtime(conn, args.interval, tr)
    else:
        run_digest(conn, args.digest_time, tr)

if __name__ == "__main__":
    main()

