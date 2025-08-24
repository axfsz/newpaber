#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Telegram 群机器人 - 新闻 / 统计 / 积分 / 广告 / 按钮菜单
数据层：MySQL 版本（PyMySQL）
可观测性增强：结构化日志、关键事件埋点、异常堆栈
"""

import os
import re
import sys
import json
import html
import time
import uuid
import traceback
import logging
from datetime import datetime, timedelta
from typing import List, Tuple, Optional, Dict

import requests
import feedparser
from bs4 import BeautifulSoup
from dateutil import tz
from dotenv import load_dotenv
import pymysql

# ====================== ENV ======================
load_dotenv()

BOT_TOKEN = (os.getenv("BOT_TOKEN") or os.getenv("TELEGRAM_BOT_TOKEN") or "").strip()
if not BOT_TOKEN:
    raise SystemExit("请在环境变量或 .env 中配置 BOT_TOKEN 或 TELEGRAM_BOT_TOKEN")

API_BASE = f"https://api.telegram.org/bot{BOT_TOKEN}"

LOCAL_TZ_NAME = os.getenv("LOCAL_TZ", "Asia/Shanghai")
LOCAL_TZ = tz.gettz(LOCAL_TZ_NAME)

# MySQL
MYSQL_HOST = os.getenv("MYSQL_HOST", "127.0.0.1")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3306"))
MYSQL_DB   = os.getenv("MYSQL_DB", "newsbot")
MYSQL_USER = os.getenv("MYSQL_USER", "root")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "")

# 超时
POLL_TIMEOUT = int(os.getenv("POLL_TIMEOUT", "50"))
HTTP_TIMEOUT = int(os.getenv("HTTP_TIMEOUT", str(max(60, POLL_TIMEOUT + 15))))

# 新闻 & 统计 & 规则
INTERVAL_MINUTES = int(os.getenv("INTERVAL_MINUTES", "60"))
NEWS_ITEMS_PER_CAT = int(os.getenv("NEWS_ITEMS_PER_CAT", "8"))
STATS_ENABLED = os.getenv("STATS_ENABLED", "1") == "1"
MIN_MSG_CHARS = int(os.getenv("MIN_MSG_CHARS", "3"))
ADMIN_USER_IDS = {int(x) for x in re.split(r"[,\s]+", os.getenv("ADMIN_USER_IDS", "").strip()) if x.isdigit()}
SCORE_CHECKIN_POINTS = int(os.getenv("SCORE_CHECKIN_POINTS", "1"))
SCORE_TOP_LIMIT = int(os.getenv("SCORE_TOP_LIMIT", "10"))
TOP_REWARD_SIZE = int(os.getenv("TOP_REWARD_SIZE", "10"))
DAILY_TOP_REWARD_START = int(os.getenv("DAILY_TOP_REWARD_START", "10"))
MONTHLY_TOP_REWARD_START = int(os.getenv("MONTHLY_TOP_REWARD_START", "10"))
REDEEM_RATE = int(os.getenv("REDEEM_RATE", "100"))
REDEEM_MIN_U = int(os.getenv("REDEEM_MIN_U", "10"))
STATS_DAILY_AT = os.getenv("STATS_DAILY_AT", "23:50")
STATS_MONTHLY_AT = os.getenv("STATS_MONTHLY_AT", "00:10")
NEWS_CHAT_IDS = [int(x) for x in re.split(r"[,\s]+", os.getenv("NEWS_CHAT_IDS", "").strip()) if x.isdigit()]
STATS_CHAT_IDS = [int(x) for x in re.split(r"[,\s]+", os.getenv("STATS_CHAT_IDS", "").strip()) if x.isdigit()]
AD_DEFAULT_ENABLED = os.getenv("AD_DEFAULT_ENABLED", "1") == "1"

# 日志
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
LOG_JSON = os.getenv("LOG_JSON", "0") == "1"
RUN_ID = os.getenv("RUN_ID") or uuid.uuid4().hex[:8]

# ====================== 日志器 ======================
def setup_logger():
    logger = logging.getLogger("newsbot")
    logger.setLevel(getattr(logging, LOG_LEVEL, logging.INFO))
    h = logging.StreamHandler(sys.stdout)

    if LOG_JSON:
        class JsonFormatter(logging.Formatter):
            def format(self, record):
                payload = {
                    "ts": datetime.utcnow().isoformat(timespec="seconds") + "Z",
                    "level": record.levelname,
                    "run": RUN_ID,
                    "msg": record.getMessage(),
                }
                # 附加标准字段（若存在）
                for k in ("chat_id", "user_id", "cmd", "event", "category", "count", "error"):
                    if hasattr(record, k):
                        payload[k] = getattr(record, k)
                return json.dumps(payload, ensure_ascii=False)
        h.setFormatter(JsonFormatter())
    else:
        fmt = "%(asctime)s %(levelname)s [%(name)s] %(message)s"
        h.setFormatter(logging.Formatter(fmt=fmt, datefmt="%Y-%m-%d %H:%M:%S"))
    logger.handlers.clear()
    logger.addHandler(h)
    return logger

logger = setup_logger()

def log(level, msg, **ctx):
    if LOG_JSON:
        # 通过记录属性注入到 JSON
        record = logger.makeRecord("newsbot", level, fn="", lno=0, msg=msg, args=(), exc_info=None)
        for k, v in ctx.items():
            setattr(record, k, v)
        logger.handle(record)
    else:
        if ctx:
            msg = f"{msg} | {json.dumps(ctx, ensure_ascii=False)}"
        logger.log(level, msg)

# ====================== 新闻源 ======================
def _env_list(key: str, default: List[str]) -> List[str]:
    raw = os.getenv(key, "").strip()
    if not raw:
        return default
    return [u.strip() for u in raw.split(";") if u.strip()]

FEEDS_FINANCE = _env_list("FEEDS_FINANCE", [
    "https://www.reuters.com/finance/rss",
    "https://www.wsj.com/xml/rss/3_7014.xml",
    "https://www.ft.com/myft/following/atom/public/industry:Financials",
])
FEEDS_SEA = _env_list("FEEDS_SEA", [
    "https://www.straitstimes.com/news/world/asia/rss.xml",
    "https://e.vnexpress.net/rss/world.rss",
    "https://www.bangkokpost.com/rss/data/world.xml",
])
FEEDS_WAR = _env_list("FEEDS_WAR", [
    "https://www.aljazeera.com/xml/rss/all.xml",
    "https://feeds.bbci.co.uk/news/world/rss.xml",
])

CATEGORY_MAP = {
    "finance": ("财经", FEEDS_FINANCE),
    "sea": ("东南亚", FEEDS_SEA),
    "war": ("战争", FEEDS_WAR),
}

# ====================== 工具函数 ======================
def tz_now() -> datetime:
    return datetime.now(tz=LOCAL_TZ)

def utcnow() -> datetime:
    return datetime.utcnow().replace(tzinfo=tz.UTC)

def parse_hhmm(s: str) -> Tuple[int, int]:
    m = re.match(r"^\s*(\d{1,2}):(\d{2})\s*$", s or "")
    if not m: return (0, 0)
    h, mi = int(m.group(1)), int(m.group(2))
    return max(0, min(23, h)), max(0, min(59, mi))

def safe_html(s: str) -> str:
    return html.escape(s or "", quote=False)

def human_name(username: str, first: str, last: str) -> str:
    if username: return f"@{username}"
    full = f"{first or ''} {last or ''}".strip()
    return full or "（匿名）"

def http_get(method: str, params=None, json_data=None, files=None, timeout: Optional[int] = None):
    url = f"{API_BASE}/{method}"
    t = timeout if timeout is not None else HTTP_TIMEOUT
    try:
        if json_data is not None:
            r = requests.post(url, json=json_data, timeout=t)
        elif files is not None:
            r = requests.post(url, data=params or {}, files=files, timeout=t)
        else:
            r = requests.get(url, params=params or {}, timeout=t)
        r.raise_for_status()
        data = r.json()
        if not data.get("ok"):
            log(logging.WARNING, "telegram api not ok", event="tg_api", error=str(data), cmd=method)
        return data
    except Exception as e:
        log(logging.ERROR, "telegram api error", event="tg_api", cmd=method, error=str(e))
        return None

def send_message_html(chat_id: int, text: str, reply_to_message_id: Optional[int] = None,
                      disable_preview: bool = True, reply_markup: Optional[dict] = None):
    params = {"chat_id": chat_id, "text": text, "parse_mode": "HTML",
              "disable_web_page_preview": "true" if disable_preview else "false"}
    if reply_to_message_id: params["reply_to_message_id"] = reply_to_message_id
    if reply_markup: params["reply_markup"] = json.dumps(reply_markup, ensure_ascii=False)
    r = http_get("sendMessage", params=params)
    if r and r.get("ok"):
        log(logging.DEBUG, "message sent", event="send_message", chat_id=chat_id)
    return r

def answer_callback_query(cb_id: str, text: str = "", show_alert: bool = False):
    return http_get("answerCallbackQuery", params={
        "callback_query_id": cb_id, "text": text, "show_alert": "true" if show_alert else "false"
    })

# ====================== MySQL 连接/初始化 ======================
_DB = None

def _connect_mysql(dbname: Optional[str] = None):
    return pymysql.connect(
        host=MYSQL_HOST, port=MYSQL_PORT, user=MYSQL_USER, password=MYSQL_PASSWORD,
        database=dbname, charset="utf8mb4", autocommit=True, cursorclass=pymysql.cursors.Cursor,
    )

def get_conn():
    global _DB
    try:
        if _DB is None:
            try:
                _DB = _connect_mysql(MYSQL_DB)
            except pymysql.err.OperationalError as e:
                if e.args and e.args[0] == 1049:  # Unknown database
                    tmp = _connect_mysql("mysql")
                    with tmp.cursor() as c:
                        c.execute(f"CREATE DATABASE IF NOT EXISTS `{MYSQL_DB}` DEFAULT CHARSET utf8mb4 COLLATE utf8mb4_unicode_ci;")
                    tmp.close()
                    _DB = _connect_mysql(MYSQL_DB)
                else:
                    raise
        else:
            _DB.ping(reconnect=True)
    except Exception as e:
        log(logging.ERROR, "mysql connect error", event="mysql_connect", error=str(e))
        raise
    return _DB

def _exec(sql: str, args: tuple = ()):
    conn = get_conn()
    with conn.cursor() as c:
        c.execute(sql, args)
        return c

def _fetchone(sql: str, args: tuple = ()):
    with _exec(sql, args) as c:
        return c.fetchone()

def _fetchall(sql: str, args: tuple = ()):
    with _exec(sql, args) as c:
        return c.fetchall()

def init_db():
    log(logging.INFO, "init db...")
    _exec("""
    CREATE TABLE IF NOT EXISTS msg_counts (
        chat_id   BIGINT NOT NULL,
        user_id   BIGINT NOT NULL,
        username  VARCHAR(64) DEFAULT NULL,
        first_name VARCHAR(64) DEFAULT NULL,
        last_name  VARCHAR(64) DEFAULT NULL,
        day       CHAR(10) NOT NULL,
        cnt       INT NOT NULL DEFAULT 0,
        PRIMARY KEY (chat_id, user_id, day),
        KEY idx_day (chat_id, day),
        KEY idx_user (chat_id, user_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
    """)
    _exec("""
    CREATE TABLE IF NOT EXISTS scores (
        chat_id   BIGINT NOT NULL,
        user_id   BIGINT NOT NULL,
        username  VARCHAR(64) DEFAULT NULL,
        first_name VARCHAR(64) DEFAULT NULL,
        last_name  VARCHAR(64) DEFAULT NULL,
        points    INT NOT NULL DEFAULT 0,
        last_checkin CHAR(10) DEFAULT NULL,
        PRIMARY KEY (chat_id, user_id),
        KEY idx_points (chat_id, points)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
    """)
    _exec("""
    CREATE TABLE IF NOT EXISTS score_logs (
        id BIGINT PRIMARY KEY AUTO_INCREMENT,
        chat_id BIGINT, actor_id BIGINT, target_id BIGINT,
        delta INT, reason VARCHAR(64), ts VARCHAR(40)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
    """)
    _exec("""
    CREATE TABLE IF NOT EXISTS invites (
        chat_id BIGINT, invitee_id BIGINT, inviter_id BIGINT, ts VARCHAR(40),
        PRIMARY KEY (chat_id, invitee_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
    """)
    _exec("""
    CREATE TABLE IF NOT EXISTS award_runs (
        chat_id BIGINT, period_type VARCHAR(10), period_value VARCHAR(10), ts VARCHAR(40),
        PRIMARY KEY (chat_id, period_type, period_value)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
    """)
    _exec("""
    CREATE TABLE IF NOT EXISTS ads (
        chat_id BIGINT PRIMARY KEY,
        enabled TINYINT NOT NULL DEFAULT 1,
        content TEXT, updated_at VARCHAR(40)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
    """)
    _exec("""
    CREATE TABLE IF NOT EXISTS state (
        `key` VARCHAR(100) PRIMARY KEY, `val` TEXT
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
    """)
    _exec("""
    CREATE TABLE IF NOT EXISTS posted_news (
        chat_id BIGINT, category VARCHAR(16), link TEXT, ts VARCHAR(40),
        PRIMARY KEY (chat_id, category(8), link(255))
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
    """)
    # 兜底列
    col = _fetchone("""
        SELECT 1 FROM information_schema.COLUMNS
        WHERE TABLE_SCHEMA=%s AND TABLE_NAME='scores' AND COLUMN_NAME='last_checkin'
    """, (MYSQL_DB,))
    if not col:
        _exec("ALTER TABLE scores ADD COLUMN last_checkin CHAR(10) DEFAULT NULL")
        log(logging.INFO, "added column scores.last_checkin")

# ====================== 状态存取 ======================
def state_get(key: str) -> Optional[str]:
    row = _fetchone("SELECT val FROM state WHERE `key`=%s", (key,))
    return row[0] if row else None

def state_set(key: str, val: str):
    _exec(
        "INSERT INTO state(`key`,`val`) VALUES (%s,%s) ON DUPLICATE KEY UPDATE `val`=VALUES(`val`)",
        (key, val),
    )

def state_del(key: str):
    _exec("DELETE FROM state WHERE `key`=%s", (key,))

# ====================== 统计/积分 ======================
def _upsert_user_base(chat_id: int, frm: Dict):
    _exec(
        "INSERT INTO scores(chat_id,user_id,username,first_name,last_name,points,last_checkin) "
        "VALUES (%s,%s,%s,%s,%s,%s,%s) "
        "ON DUPLICATE KEY UPDATE username=VALUES(username), first_name=VALUES(first_name), last_name=VALUES(last_name)",
        (chat_id, frm.get("id"), (frm.get("username") or "")[:64], (frm.get("first_name") or "")[:64],
         (frm.get("last_name") or "")[:64], 0, None),
    )

def _add_points(chat_id: int, target_id: int, delta: int, actor_id: int, reason: str = ""):
    _exec(
        "INSERT INTO scores(chat_id,user_id,points) VALUES(%s,%s,%s) "
        "ON DUPLICATE KEY UPDATE points=points+VALUES(points)",
        (chat_id, target_id, delta),
    )
    _exec("INSERT INTO score_logs(chat_id,actor_id,target_id,delta,reason,ts) VALUES(%s,%s,%s,%s,%s,%s)",
          (chat_id, actor_id, target_id, delta, reason or "", utcnow().isoformat()))
    log(logging.INFO, "score changed", event="score", chat_id=chat_id, user_id=target_id, cmd=reason, count=delta)

def _get_points(chat_id: int, user_id: int) -> int:
    row = _fetchone("SELECT points FROM scores WHERE chat_id=%s AND user_id=%s", (chat_id, user_id))
    return int(row[0]) if row else 0

def _get_last_checkin(chat_id: int, user_id: int) -> str:
    row = _fetchone("SELECT last_checkin FROM scores WHERE chat_id=%s AND user_id=%s", (chat_id, user_id))
    return row[0] or "" if row else ""

def _set_last_checkin(chat_id: int, user_id: int, day: str):
    _exec("UPDATE scores SET last_checkin=%s WHERE chat_id=%s AND user_id=%s", (day, chat_id, user_id))

def inc_msg_count(chat_id: int, frm: Dict, day: str, inc: int = 1):
    _upsert_user_base(chat_id, frm)
    _exec(
        "INSERT INTO msg_counts(chat_id,user_id,username,first_name,last_name,day,cnt) "
        "VALUES (%s,%s,%s,%s,%s,%s,%s) "
        "ON DUPLICATE KEY UPDATE cnt=cnt+VALUES(cnt), username=VALUES(username), first_name=VALUES(first_name), last_name=VALUES(last_name)",
        (chat_id, frm.get("id"), (frm.get("username") or "")[:64], (frm.get("first_name") or "")[:64],
         (frm.get("last_name") or "")[:64], day, inc)
    )

def ensure_user_display(chat_id: int, uid: int, current_triplet: Tuple[str, str, str]) -> Tuple[str, str, str]:
    un, fn, ln = current_triplet
    if un or fn or ln: return un, fn, ln
    r = http_get("getChatMember", params={"chat_id": chat_id, "user_id": uid})
    user = ((r or {}).get("result") or {}).get("user") or {}
    un2 = user.get("username") or ""
    fn2 = user.get("first_name") or ""
    ln2 = user.get("last_name") or ""
    if un2 or fn2 or ln2:
        _exec("UPDATE scores SET username=%s, first_name=%s, last_name=%s WHERE chat_id=%s AND user_id=%s",
              (un2, fn2, ln2, chat_id, uid))
        return un2, fn2, ln2
    return un, fn, ln

def list_top_day(chat_id: int, day: str, limit: int = 10):
    return _fetchall(
        """SELECT user_id, MAX(username), MAX(first_name), MAX(last_name), SUM(cnt) AS c
           FROM msg_counts WHERE chat_id=%s AND day=%s
           GROUP BY user_id ORDER BY c DESC LIMIT %s""", (chat_id, day, limit)
    )

def list_top_month(chat_id: int, ym: str, limit: int = 10):
    return _fetchall(
        """SELECT user_id, MAX(username), MAX(first_name), MAX(last_name), SUM(cnt) AS c
           FROM msg_counts WHERE chat_id=%s AND day LIKE CONCAT(%s,'-%')
           GROUP BY user_id ORDER BY c DESC LIMIT %s""", (chat_id, ym, limit)
    )

def build_daily_report(chat_id: int, day: str) -> str:
    rows = list_top_day(chat_id, day, limit=10)
    total = _fetchone("SELECT SUM(cnt) FROM msg_counts WHERE chat_id=%s AND day=%s", (chat_id, day))[0] or 0
    people = _fetchone("SELECT COUNT(DISTINCT user_id) FROM msg_counts WHERE chat_id=%s AND day=%s", (chat_id, day))[0] or 0
    log(logging.INFO, "daily stats", event="stats_day", chat_id=chat_id, count=int(total))
    lines = [f"📊 <b>{day} 发言统计</b>", f"参与人数：<b>{people}</b>，总计条数：<b>{total}</b>", "<code>────────────────</code>"]
    if not rows:
        lines.append("暂无数据。"); return "\n".join(lines)
    for i, (uid, un, fn, ln, c) in enumerate(rows, 1):
        un, fn, ln = ensure_user_display(chat_id, uid, (un, fn, ln))
        lines.append(f"{i}. {safe_html(human_name(un, fn, ln))} — <b>{c}</b>")
    return "\n".join(lines)

def build_monthly_report(chat_id: int, ym: str) -> str:
    rows = list_top_month(chat_id, ym, limit=10)
    total = _fetchone("SELECT SUM(cnt) FROM msg_counts WHERE chat_id=%s AND day LIKE CONCAT(%s,'-%')", (chat_id, ym))[0] or 0
    people = _fetchone("SELECT COUNT(DISTINCT user_id) FROM msg_counts WHERE chat_id=%s AND day LIKE CONCAT(%s,'-%')", (chat_id, ym))[0] or 0
    log(logging.INFO, "monthly stats", event="stats_month", chat_id=chat_id, count=int(total))
    lines = [f"📈 <b>{ym} 月度发言统计</b>", f"参与人数：<b>{people}</b>，总计条数：<b>{total}</b>", "<code>────────────────</code>"]
    if not rows:
        lines.append("暂无数据。"); return "\n".join(lines)
    for i, (uid, un, fn, ln, c) in enumerate(rows, 1):
        un, fn, ln = ensure_user_display(chat_id, uid, (un, fn, ln))
        lines.append(f"{i}. {safe_html(human_name(un, fn, ln))} — <b>{c}</b>")
    return "\n".join(lines)

def _was_awarded(chat_id: int, period_type: str, value: str) -> bool:
    return _fetchone("SELECT 1 FROM award_runs WHERE chat_id=%s AND period_type=%s AND period_value=%s",
                     (chat_id, period_type, value)) is not None

def _mark_awarded(chat_id: int, period_type: str, value: str):
    _exec("INSERT IGNORE INTO award_runs(chat_id,period_type,period_value,ts) VALUES(%s,%s,%s,%s)",
          (chat_id, period_type, value, utcnow().isoformat()))

def award_top_speakers(chat_id: int, day: str = None, ym: str = None):
    if day:
        if _was_awarded(chat_id, "day", day): return
        rows = list_top_day(chat_id, day, limit=TOP_REWARD_SIZE)
        start_bonus = DAILY_TOP_REWARD_START; ptype, pval = "day", day
    else:
        if _was_awarded(chat_id, "month", ym): return
        rows = list_top_month(chat_id, ym, limit=TOP_REWARD_SIZE)
        start_bonus = MONTHLY_TOP_REWARD_START; ptype, pval = "month", ym
    if not rows: return
    bonus = start_bonus
    for (uid, un, fn, ln, c) in rows:
        _upsert_user_base(chat_id, {"id": uid, "username": un, "first_name": fn, "last_name": ln})
        _add_points(chat_id, uid, max(bonus, 0), uid, f"top_{ptype}_reward")
        bonus -= 1
    _mark_awarded(chat_id, ptype, pval)
    log(logging.INFO, "awarded top speakers", event="award", chat_id=chat_id, cmd=f"{ptype}:{pval}", count=len(rows))

# ====================== 广告 ======================
def ad_get(chat_id: int) -> Tuple[bool, str]:
    row = _fetchone("SELECT enabled, content FROM ads WHERE chat_id=%s", (chat_id,))
    if row: return (int(row[0]) == 1, row[1] or "")
    _exec("INSERT IGNORE INTO ads(chat_id,enabled,content,updated_at) VALUES(%s,%s,%s,%s)",
          (chat_id, 1 if AD_DEFAULT_ENABLED else 0, "", utcnow().isoformat()))
    return AD_DEFAULT_ENABLED, ""

def ad_set(chat_id: int, content: str):
    _exec("INSERT INTO ads(chat_id,enabled,content,updated_at) VALUES(%s,%s,%s,%s) "
          "ON DUPLICATE KEY UPDATE content=VALUES(content), updated_at=VALUES(updated_at)",
          (chat_id, 1 if AD_DEFAULT_ENABLED else 0, content, utcnow().isoformat()))
    log(logging.INFO, "ad updated", event="ad_set", chat_id=chat_id)

def ad_enable(chat_id: int, enabled: bool):
    _exec("INSERT INTO ads(chat_id,enabled,content,updated_at) VALUES(%s,%s,%s,%s) "
          "ON DUPLICATE KEY UPDATE enabled=VALUES(enabled), updated_at=VALUES(updated_at)",
          (chat_id, 1 if enabled else 0, "", utcnow().isoformat()))
    log(logging.INFO, "ad toggled", event="ad_toggle", chat_id=chat_id, cmd="enable" if enabled else "disable")

def ad_clear(chat_id: int):
    _exec("UPDATE ads SET content=%s, updated_at=%s WHERE chat_id=%s", ("", utcnow().isoformat(), chat_id))
    log(logging.INFO, "ad cleared", event="ad_clear", chat_id=chat_id)

# ====================== 新闻抓取 ======================
def clean_text(s: str) -> str:
    if not s: return ""
    soup = BeautifulSoup(s, "html.parser")
    return re.sub(r"\s+", " ", soup.get_text().strip())

def fetch_rss_list(urls: List[str], max_items: int) -> List[Dict]:
    items = []
    for u in urls:
        try:
            feed = feedparser.parse(u)
            for e in feed.entries[: max_items * 2]:
                title = clean_text(e.get("title"))
                link = e.get("link") or ""
                summary = clean_text(e.get("summary") or e.get("description"))
                if title and link:
                    items.append({"title": title, "link": link, "summary": summary})
        except Exception as e:
            log(logging.WARNING, "rss parse error", event="rss", error=f"{u} {e}")
    # 去重（按 link）
    seen, uniq = set(), []
    for it in items:
        if it["link"] in seen: continue
        seen.add(it["link"]); uniq.append(it)
        if len(uniq) >= max_items: break
    return uniq

def already_posted(chat_id: int, category: str, link: str) -> bool:
    return _fetchone("SELECT 1 FROM posted_news WHERE chat_id=%s AND category=%s AND link=%s",
                     (chat_id, category, link)) is not None

def mark_posted(chat_id: int, category: str, link: str):
    _exec("INSERT IGNORE INTO posted_news(chat_id,category,link,ts) VALUES(%s,%s,%s,%s)",
          (chat_id, category, link, utcnow().isoformat()))

def push_news_once(chat_id: int):
    order = ["finance", "sea", "war"]
    now_str = tz_now().strftime("%Y-%m-%d %H:%M")
    sent_any = False
    for cat in order:
        cname, feeds = CATEGORY_MAP.get(cat, (cat, []))
        items = fetch_rss_list(feeds, NEWS_ITEMS_PER_CAT)
        log(logging.INFO, "news fetched", event="news_fetch", chat_id=chat_id, category=cat, count=len(items))
        if not items: continue
        new_items = [it for it in items if not already_posted(chat_id, cat, it["link"])]
        if not new_items: continue
        lines = [f"🗞️ <b>{cname}</b> | {now_str}", "<code>────────────────</code>"]
        for i, it in enumerate(new_items, 1):
            lines.append(f"{i}. {safe_html(it['title'])}\n{it['link']}")
        enabled, content = ad_get(chat_id)
        if enabled and content.strip():
            lines.append("<code>────────────────</code>")
            lines.append(f"📣 <b>广告</b>\n{safe_html(content)}")
        send_message_html(chat_id, "\n".join(lines))
        for it in new_items:
            mark_posted(chat_id, cat, it["link"])
        sent_any = True
        log(logging.INFO, "news posted", event="news_post", chat_id=chat_id, category=cat, count=len(new_items))
    if not sent_any:
        send_message_html(chat_id, "🗞️ 暂无可用新闻（可能源不可达或暂无更新）。")
        log(logging.INFO, "news none", event="news_post", chat_id=chat_id, category="all", count=0)

# ====================== 权限/菜单 ======================
def ikb(text: str, data: str) -> dict:
    return {"text": text, "callback_data": data}

def list_chat_admin_ids(chat_id: int) -> set:
    key = f"admins:{chat_id}"
    now = int(time.time())
    cached = state_get(key)
    if cached:
        try:
            data = json.loads(cached)
            if now - int(data.get("ts", 0)) < 600:
                return set(data.get("ids", []))
        except Exception:
            pass
    ids = set()
    r = http_get("getChatAdministrators", params={"chat_id": chat_id})
    if r and r.get("ok"):
        for m in r["result"]:
            u = m.get("user") or {}
            if "id" in u: ids.add(u["id"])
    state_set(key, json.dumps({"ids": list(ids), "ts": now}))
    log(logging.DEBUG, "admins cached", event="admins", chat_id=chat_id, count=len(ids))
    return ids

def is_chat_admin(chat_id: int, uid: Optional[int]) -> bool:
    if not uid: return False
    if uid in ADMIN_USER_IDS: return True
    admins = list_chat_admin_ids(chat_id)
    if uid in admins: return True
    r = http_get("getChatMember", params={"chat_id": chat_id, "user_id": uid})
    try:
        status = ((r or {}).get("result") or {}).get("status", "")
        return status in ("administrator", "creator")
    except Exception:
        return False

def build_menu(is_admin_user: bool) -> dict:
    kb = [
        [ikb("✅ 签到", "ACT_CHECKIN")],
        [ikb("📌 我的积分", "ACT_SCORE"), ikb("🏆 积分榜Top10", "ACT_TOP10")],
        [ikb("📊 今日统计", "ACT_SD_TODAY"), ikb("📊 昨日统计", "ACT_SD_YESTERDAY")],
        [ikb("📈 本月统计", "ACT_SM_THIS"), ikb("📈 上月统计", "ACT_SM_LAST")],
        [ikb("🎁 兑换U", "ACT_REDEEM")],
        [ikb("🆘 帮助", "ACT_HELP")],
    ]
    if is_admin_user:
        kb.append([ikb("📣 广告显示", "ACT_AD_SHOW"), ikb("🟢 启用广告", "ACT_AD_ENABLE"), ikb("🔴 禁用广告", "ACT_AD_DISABLE")])
        kb.append([ikb("🧹 清空广告", "ACT_AD_CLEAR"), ikb("✍️ 设置广告", "ACT_AD_SET")])
        kb.append([ikb("🗞 立即推送新闻", "ACT_NEWS_NOW")])
    return {"inline_keyboard": kb}

def send_menu_for(chat_id: int, uid: int):
    send_message_html(chat_id, "请选择功能：", reply_markup=build_menu(is_chat_admin(chat_id, uid)))

# ====================== 命令/回调 ======================
def find_user_by_mention(chat_id: int, mention: str):
    u = mention.lstrip("@").strip().lower()
    return _fetchone(
        "SELECT user_id, username, first_name, last_name FROM scores WHERE chat_id=%s AND LOWER(username)=%s LIMIT 1",
        (chat_id, u)
    )

def target_user_from_msg(msg: Dict):
    chat_id = (msg.get("chat") or {}).get("id")
    if msg.get("reply_to_message"):
        t = msg["reply_to_message"].get("from") or {}
        return (chat_id, t.get("id"), t.get("username") or "", t.get("first_name") or "", t.get("last_name") or "")
    txt = (msg.get("text") or "").strip()
    parts = txt.split()
    for p in parts[1:]:
        if p.startswith("@") and len(p) > 1:
            row = find_user_by_mention(chat_id, p)
            if row:
                uid, un, fn, ln = row
                return (chat_id, uid, un, fn, ln)
    return (None, None, None, None, None)

def handle_admin_ad_command(msg: Dict) -> bool:
    chat_id = (msg.get("chat") or {}).get("id")
    frm = msg.get("from") or {}
    uid = frm.get("id")
    if not is_chat_admin(chat_id, uid):
        send_message_html(chat_id, "❌ 你没有权限执行该命令。")
        return True

    txt = (msg.get("text") or "").strip()
    if txt.startswith("/ad_help"):
        send_message_html(chat_id,
            "📢 <b>广告位命令</b>\n"
            "• /ad_set <文本...> — 设置/覆盖广告内容（或在菜单点“设置广告”后回复文本）\n"
            "• /ad_show — 查看当前广告与状态\n"
            "• /ad_clear — 清空广告内容\n"
            "• /ad_enable — 启用广告位\n"
            "• /ad_disable — 禁用广告位")
        return True
    if txt.startswith("/ad_set"):
        content = txt.split(" ", 1)
        if len(content) < 2 or not content[1].strip():
            send_message_html(chat_id, "用法：/ad_set <广告文本>")
            return True
        ad_set(chat_id, content[1].strip())
        send_message_html(chat_id, "✅ 广告内容已更新。")
        return True
    if txt.startswith("/ad_show"):
        enabled, content = ad_get(chat_id)
        st = "启用" if enabled else "禁用"
        send_message_html(chat_id, f"📣 当前状态：<b>{st}</b>\n内容：\n{safe_html(content) if content else '（空）'}")
        return True
    if txt.startswith("/ad_clear"):
        ad_clear(chat_id)
        send_message_html(chat_id, "✅ 已清空广告内容。")
        return True
    if txt.startswith("/ad_enable"):
        ad_enable(chat_id, True)
        send_message_html(chat_id, "✅ 已启用广告位。")
        return True
    if txt.startswith("/ad_disable"):
        ad_enable(chat_id, False)
        send_message_html(chat_id, "✅ 已禁用广告位。")
        return True
    return False

def handle_general_command(msg: Dict) -> bool:
    chat_id = (msg.get("chat") or {}).get("id")
    frm = msg.get("from") or {}
    uid = frm.get("id")
    txt = (msg.get("text") or "").strip()
    if not txt or not txt.startswith("/"):
        return False

    # 广告命令
    if txt.startswith("/ad_"):
        return handle_admin_ad_command(msg)

    parts = txt.split()
    cmd = parts[0].lower()
    log(logging.INFO, "command", event="cmd", chat_id=chat_id, user_id=uid, cmd=cmd)

    if cmd in ("/menu", "/start", "/help"):
        send_menu_for(chat_id, uid); return True

    if cmd == "/whoami":
        r = http_get("getChatMember", params={"chat_id": chat_id, "user_id": uid})
        status = ((r or {}).get("result") or {}).get("status", "unknown")
        send_message_html(chat_id, f"👤 <b>whoami</b>\nuser_id: <code>{uid}</code>\nstatus: <b>{status}</b>\nadmin: <b>{'YES' if is_chat_admin(chat_id, uid) else 'NO'}</b>")
        return True

    _upsert_user_base(chat_id, frm)

    if cmd == "/checkin":
        today = tz_now().strftime("%Y-%m-%d")
        last = _get_last_checkin(chat_id, uid)
        if last == today:
            send_message_html(chat_id, f"✅ 你今天已经签到过啦（{today}）。"); return True
        _add_points(chat_id, uid, SCORE_CHECKIN_POINTS, uid, "daily_checkin")
        _set_last_checkin(chat_id, uid, today)
        pts = _get_points(chat_id, uid)
        send_message_html(chat_id, f"🎉 签到成功 +{SCORE_CHECKIN_POINTS} 分！当前积分：<b>{pts}</b>")
        return True

    if cmd == "/score":
        pts = _get_points(chat_id, uid)
        send_message_html(chat_id, f"📌 你的当前积分：<b>{pts}</b>"); return True

    if cmd == "/score_top":
        limit = SCORE_TOP_LIMIT
        if len(parts) >= 2 and parts[1].isdigit():
            limit = max(1, min(50, int(parts[1])))
        rows = _fetchall(
            "SELECT user_id,username,first_name,last_name,points FROM scores WHERE chat_id=%s ORDER BY points DESC LIMIT %s",
            (chat_id, limit),
        )
        if not rows:
            send_message_html(chat_id, "暂无积分数据。"); return True
        lines = ["🏆 <b>积分榜</b>", "<code>────────────────</code>"]
        for i, (uid2, u, f, l, p) in enumerate(rows, 1):
            u, f, l = ensure_user_display(chat_id, uid2, (u, f, l))
            lines.append(f"{i}. {safe_html(human_name(u, f, l))} — <b>{p}</b> 分")
        send_message_html(chat_id, "\n".join(lines)); return True

    if cmd in ("/score_add", "/score_deduct"):
        if not is_chat_admin(chat_id, uid):
            send_message_html(chat_id, "❌ 你没有权限执行此命令。"); return True
        tgt_chat, tgt_id, un, fn, ln = target_user_from_msg(msg)
        if not tgt_id:
            send_message_html(chat_id, "请对目标成员的消息回复命令，或在命令后带 @username。示例：/score_add @user 5"); return True
        if len(parts) >= 2 and parts[-1].lstrip("-").isdigit():
            delta = int(parts[-1])
        else:
            send_message_html(chat_id, "请在命令末尾给出整数分值。例如：/score_deduct 3"); return True
        delta = -abs(delta) if cmd == "/score_deduct" else abs(delta)
        _upsert_user_base(chat_id, {"id": tgt_id, "username": un, "first_name": fn, "last_name": ln})
        _add_points(chat_id, tgt_id, delta, uid, cmd[1:])
        new_pts = _get_points(chat_id, tgt_id)
        sign = "+" if delta > 0 else ""
        send_message_html(chat_id, f"✅ 已为 {safe_html(human_name(un, fn, ln))} 变更积分：{sign}{delta}，当前积分 <b>{new_pts}</b>"); return True

    if cmd == "/invited_by":
        if len(parts) < 2 or not parts[1].startswith("@"):
            send_message_html(chat_id, "用法：/invited_by @邀请人（仅限新加入后首次绑定）"); return True
        exist = _fetchone("SELECT 1 FROM invites WHERE chat_id=%s AND invitee_id=%s", (chat_id, uid))
        if exist:
            send_message_html(chat_id, "你已绑定过邀请人，无法重复绑定。"); return True
        row = find_user_by_mention(chat_id, parts[1])
        if not row:
            send_message_html(chat_id, "未找到该邀请人，请让对方在群里先发一条消息再试。"); return True
        inviter_id, un, fn, ln = row
        _add_points(chat_id, inviter_id, 1, uid, "invite_bind")
        _exec("INSERT INTO invites(chat_id, invitee_id, inviter_id, ts) VALUES(%s,%s,%s,%s)",
              (chat_id, uid, inviter_id, utcnow().isoformat()))
        send_message_html(chat_id, f"绑定成功！已为 {safe_html(human_name(un, fn, ln))} 增加 1 分。"); return True

    if cmd == "/stats_day":
        day = (tz_now() - timedelta(days=1)).strftime("%Y-%m-%d")
        if len(parts) >= 2:
            p = parts[1].lower()
            day = tz_now().strftime("%Y-%m-%d") if p == "today" else ((tz_now() - timedelta(days=1)).strftime("%Y-%m-%d") if p == "yesterday" else parts[1])
        send_message_html(chat_id, build_daily_report(chat_id, day)); return True

    if cmd == "/stats_month":
        ym = tz_now().strftime("%Y-%m")
        if len(parts) >= 2:
            p = parts[1].lower()
            ym = tz_now().strftime("%Y-%m") if p == "this" else ((tz_now().replace(day=1) - timedelta(days=1)).strftime("%Y-%m") if p == "last" else parts[1])
        send_message_html(chat_id, build_monthly_report(chat_id, ym)); return True

    if cmd == "/news_now":
        if not is_chat_admin(chat_id, uid):
            send_message_html(chat_id, "❌ 你没有权限执行此命令。"); return True
        push_news_once(chat_id)
        nxt = tz_now() + timedelta(minutes=INTERVAL_MINUTES)
        state_set("next_news_at", nxt.isoformat())
        return True

    if cmd == "/redeem":
        pts = _get_points(chat_id, uid)
        max_u = pts // REDEEM_RATE
        if max_u < REDEEM_MIN_U:
            need = REDEEM_RATE * REDEEM_MIN_U
            send_message_html(chat_id, f"当前积分 {pts}，不足以兑换（至少需 {need} 分，即 {REDEEM_MIN_U} U）。"); return True
        target_u = max_u
        if len(parts) >= 2 and parts[1].isdigit():
            req_u = int(parts[1])
            if req_u < REDEEM_MIN_U:
                send_message_html(chat_id, f"单次兑换至少 {REDEEM_MIN_U} U。"); return True
            if req_u > max_u:
                send_message_html(chat_id, f"可兑上限 {max_u} U，你当前积分不足以兑换 {req_u} U。"); return True
            target_u = req_u
        deduct_pts = target_u * REDEEM_RATE
        _add_points(chat_id, uid, -deduct_pts, uid, f"redeem_to_U:{target_u}")
        remain = _get_points(chat_id, uid)
        send_message_html(chat_id, f"🎁 兑换成功：{target_u} U（已扣 {deduct_pts} 分）。当前剩余积分：<b>{remain}</b>。"); return True

    return False

# ---------- 回调（按钮） ----------
def handle_callback(cb: Dict):
    cb_id = cb.get("id")
    user = cb.get("from") or {}
    uid = user.get("id")
    msg = cb.get("message") or {}
    chat = msg.get("chat") or {}
    chat_id = chat.get("id")
    data = cb.get("data") or ""
    log(logging.INFO, "callback", event="cb", chat_id=chat_id, user_id=uid, cmd=data)

    try:
        if not chat_id or not uid or not data:
            answer_callback_query(cb_id); return
        _upsert_user_base(chat_id, {
            "id": uid, "username": user.get("username"),
            "first_name": user.get("first_name"), "last_name": user.get("last_name"),
        })
        admin = is_chat_admin(chat_id, uid)

        if data == "ACT_CHECKIN":
            today = tz_now().strftime("%Y-%m-%d")
            last = _get_last_checkin(chat_id, uid)
            if last == today: answer_callback_query(cb_id, "今天已签到")
            else:
                _add_points(chat_id, uid, SCORE_CHECKIN_POINTS, uid, "daily_checkin")
                _set_last_checkin(chat_id, uid, today)
                pts = _get_points(chat_id, uid)
                answer_callback_query(cb_id, f"签到成功 +{SCORE_CHECKIN_POINTS}，当前 {pts} 分")
            return

        if data == "ACT_SCORE":
            pts = _get_points(chat_id, uid)
            answer_callback_query(cb_id, f"当前积分：{pts}"); return

        if data == "ACT_TOP10":
            rows = _fetchall(
                "SELECT user_id,username,first_name,last_name,points FROM scores WHERE chat_id=%s ORDER BY points DESC LIMIT %s",
                (chat_id, SCORE_TOP_LIMIT),
            )
            if not rows: send_message_html(chat_id, "暂无积分数据。")
            else:
                lines = ["🏆 <b>积分榜</b>", "<code>────────────────</code>"]
                for i, (uid2, u, f, l, p) in enumerate(rows, 1):
                    u, f, l = ensure_user_display(chat_id, uid2, (u, f, l))
                    lines.append(f"{i}. {safe_html(human_name(u, f, l))} — <b>{p}</b> 分")
                send_message_html(chat_id, "\n".join(lines))
            answer_callback_query(cb_id); return

        if data in ("ACT_SD_TODAY", "ACT_SD_YESTERDAY"):
            day = tz_now().strftime("%Y-%m-%d") if data.endswith("TODAY") else (tz_now() - timedelta(days=1)).strftime("%Y-%m-%d")
            send_message_html(chat_id, build_daily_report(chat_id, day)); answer_callback_query(cb_id); return

        if data in ("ACT_SM_THIS", "ACT_SM_LAST"):
            ym = tz_now().strftime("%Y-%m") if data.endswith("THIS") else (tz_now().replace(day=1) - timedelta(days=1)).strftime("%Y-%m")
            send_message_html(chat_id, build_monthly_report(chat_id, ym)); answer_callback_query(cb_id); return

        if data == "ACT_REDEEM":
            pts = _get_points(chat_id, uid); max_u = pts // REDEEM_RATE
            if max_u < REDEEM_MIN_U:
                answer_callback_query(cb_id, f"积分不足，至少需 {REDEEM_RATE*REDEEM_MIN_U} 分", show_alert=True)
            else:
                deduct_pts = max_u * REDEEM_RATE
                _add_points(chat_id, uid, -deduct_pts, uid, f"redeem_to_U:{max_u}")
                remain = _get_points(chat_id, uid)
                send_message_html(chat_id, f"🎁 兑换成功：{max_u} U（已扣 {deduct_pts} 分）。当前剩余积分：<b>{remain}</b>。")
                answer_callback_query(cb_id, "兑换完成")
            return

        if data == "ACT_AD_SHOW":
            if not admin: answer_callback_query(cb_id, "无权限", show_alert=True); return
            enabled, content = ad_get(chat_id)
            st = "启用" if enabled else "禁用"
            send_message_html(chat_id, f"📣 当前状态：<b>{st}</b>\n内容：\n{safe_html(content) if content else '（空）'}")
            answer_callback_query(cb_id); return

        if data == "ACT_AD_ENABLE":
            if not admin: answer_callback_query(cb_id, "无权限", show_alert=True); return
            ad_enable(chat_id, True); answer_callback_query(cb_id, "已启用"); return

        if data == "ACT_AD_DISABLE":
            if not admin: answer_callback_query(cb_id, "无权限", show_alert=True); return
            ad_enable(chat_id, False); answer_callback_query(cb_id, "已禁用"); return

        if data == "ACT_AD_CLEAR":
            if not admin: answer_callback_query(cb_id, "无权限", show_alert=True); return
            ad_clear(chat_id); answer_callback_query(cb_id, "已清空"); return

        if data == "ACT_AD_SET":
            if not admin: answer_callback_query(cb_id, "无权限", show_alert=True); return
            key = f"pending:adset:{chat_id}:{uid}"
            state_set(key, "1")
            send_message_html(chat_id, "请在本条消息下<b>回复一条文本</b>作为新的广告内容。")
            answer_callback_query(cb_id, "请回复广告文本"); return

        if data == "ACT_HELP":
            send_menu_for(chat_id, uid); answer_callback_query(cb_id, "已刷新菜单"); return

        if data == "ACT_NEWS_NOW":
            if not admin: answer_callback_query(cb_id, "无权限", show_alert=True); return
            push_news_once(chat_id)
            nxt = tz_now() + timedelta(minutes=INTERVAL_MINUTES)
            state_set("next_news_at", nxt.isoformat())
            answer_callback_query(cb_id, "已推送新闻"); return

    except Exception as e:
        logger.exception("callback error")
        try: answer_callback_query(cb_id)
        except Exception: pass

# ====================== 长轮询 & 统计 ======================
def handle_new_members(msg: Dict):
    chat_id = (msg.get("chat") or {}).get("id")
    inv_link = msg.get("invite_link") or {}
    creator = (inv_link.get("creator") or {})
    inviter_id = creator.get("id")
    members = msg.get("new_chat_members") or []
    for m in members:
        invitee_id = m.get("id")
        if not invitee_id or (inviter_id and inviter_id == invitee_id): continue
        exists = _fetchone("SELECT 1 FROM invites WHERE chat_id=%s AND invitee_id=%s", (chat_id, invitee_id))
        if exists: continue
        if inviter_id:
            _upsert_user_base(chat_id, {"id": inviter_id, "username": creator.get("username"), "first_name": creator.get("first_name"), "last_name": creator.get("last_name")})
            _add_points(chat_id, inviter_id, 1, inviter_id, "invite_join")
            _exec("INSERT INTO invites(chat_id, invitee_id, inviter_id, ts) VALUES(%s,%s,%s,%s)",
                  (chat_id, invitee_id, inviter_id, utcnow().isoformat()))
            log(logging.INFO, "member joined by link", event="invite_join", chat_id=chat_id, user_id=invitee_id)

def process_updates_once():
    offset_key = "last_update_id"
    last_update_id = int(state_get(offset_key) or 0)

    resp = http_get("getUpdates", params={
        "offset": last_update_id + 1 if last_update_id else None,
        "timeout": POLL_TIMEOUT,
        "allowed_updates": json.dumps(["message", "callback_query"])
    }, timeout=max(POLL_TIMEOUT + 10, HTTP_TIMEOUT))
    if not resp or not resp.get("ok"):
        time.sleep(1); return

    results = resp.get("result", [])
    if results:
        log(logging.DEBUG, "updates polled", event="poll", count=len(results))

    for u in results:
        last_update_id = max(last_update_id, int(u.get("update_id", 0)))
        state_set(offset_key, str(last_update_id))

        if u.get("callback_query"):
            handle_callback(u["callback_query"]); continue

        msg = u.get("message") or {}
        chat = msg.get("chat") or {}
        chat_id = chat.get("id")
        if not chat_id: continue

        if msg.get("new_chat_members"):
            handle_new_members(msg); continue

        frm = msg.get("from") or {}
        if not frm or frm.get("is_bot"): continue

        text = (msg.get("text") or "").strip() if isinstance(msg.get("text"), str) else None

        key = f"pending:adset:{chat_id}:{frm.get('id')}"
        if state_get(key):
            if text and not text.startswith("/"):
                ad_set(chat_id, text); state_del(key)
                send_message_html(chat_id, "✅ 广告内容已更新。")
                continue

        if text and text.startswith("/"):
            try:
                if handle_general_command(msg): continue
            except Exception:
                logger.exception("command error")
                continue

        if STATS_ENABLED and text and len(text.strip()) >= MIN_MSG_CHARS:
            day = tz_now().strftime("%Y-%m-%d")
            inc_msg_count(chat_id, frm, day, inc=1)

# ====================== 调度 ======================
def gather_known_chats() -> List[int]:
    chats = set(NEWS_CHAT_IDS or [])
    for r in _fetchall("SELECT DISTINCT chat_id FROM msg_counts", ()): chats.add(int(r[0]))
    for r in _fetchall("SELECT DISTINCT chat_id FROM scores", ()): chats.add(int(r[0]))
    return sorted(chats)

def maybe_push_news():
    key = "next_news_at"
    nv = state_get(key)
    now = tz_now()
    if nv:
        try:
            next_at = datetime.fromisoformat(nv)
            if next_at.tzinfo is None: next_at = next_at.replace(tzinfo=LOCAL_TZ)
        except Exception:
            next_at = now - timedelta(minutes=1)
    else:
        next_at = now - timedelta(minutes=1)
    if now >= next_at:
        chats = NEWS_CHAT_IDS or gather_known_chats()
        log(logging.INFO, "news cycle", event="news_cycle", count=len(chats))
        for cid in chats:
            try:
                push_news_once(cid)
            except Exception:
                logger.exception("news push error")
        nxt = now + timedelta(minutes=INTERVAL_MINUTES)
        state_set(key, nxt.isoformat())
        log(logging.INFO, "next news schedule", event="news_schedule", cmd=nxt.isoformat())

def maybe_daily_report():
    h, m = parse_hhmm(STATS_DAILY_AT)
    now = tz_now()
    if now.hour != h or now.minute != m: return
    chats = STATS_CHAT_IDS or gather_known_chats()
    yesterday = (now - timedelta(days=1)).strftime("%Y-%m-%d")
    for cid in chats:
        run_key = f"daily_done:{cid}:{yesterday}"
        if state_get(run_key): continue
        try:
            send_message_html(cid, build_daily_report(cid, yesterday))
            award_top_speakers(cid, day=yesterday)
        except Exception:
            logger.exception("daily report error")
        state_set(run_key, "1")
    log(logging.INFO, "daily reports done", event="stats_daily", count=len(chats))

def maybe_monthly_report():
    h, m = parse_hhmm(STATS_MONTHLY_AT)
    now = tz_now()
    if not (now.day == 1 and now.hour == h and now.minute == m): return
    last_month = (now.replace(day=1) - timedelta(days=1)).strftime("%Y-%m")
    chats = STATS_CHAT_IDS or gather_known_chats()
    for cid in chats:
        run_key = f"monthly_done:{cid}:{last_month}"
        if state_get(run_key): continue
        try:
            send_message_html(cid, build_monthly_report(cid, last_month))
            award_top_speakers(cid, ym=last_month)
        except Exception:
            logger.exception("monthly report error")
        state_set(run_key, "1")
    log(logging.INFO, "monthly reports done", event="stats_monthly", cmd=last_month, count=len(chats))

def scheduler_step():
    maybe_push_news()
    maybe_daily_report()
    maybe_monthly_report()

# ====================== 启动 ======================
if __name__ == "__main__":
    print(f"[boot] starting bot... run={RUN_ID}")
    print(f"[boot] TZ={LOCAL_TZ_NAME}, MYSQL={MYSQL_USER}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}")
    try:
        get_conn(); init_db()
        log(logging.INFO, "boot ok", event="boot",
            cmd=f"{LOCAL_TZ_NAME} http={HTTP_TIMEOUT}s poll={POLL_TIMEOUT}s interval={INTERVAL_MINUTES}m")
    except Exception:
        logger.exception("boot error")
        sys.exit(1)

    while True:
        try:
            scheduler_step()
        except Exception:
            logger.exception("scheduler error")
        try:
            process_updates_once()
        except KeyboardInterrupt:
            print("bye"); break
        except Exception:
            logger.exception("updates loop error")
            time.sleep(2)

