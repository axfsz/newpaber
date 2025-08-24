#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Telegram 群机器人 - 新闻 / 统计 / 积分 / 广告(附加/定时) / 曝光台 / 入群欢迎面板 / 自定义新闻 / 招商按钮
数据层：MySQL（PyMySQL）

新增：
- 菜单尾部追加“招商”URL 按钮（从 .env 读取 BIZ_LINKS 或 BIZ_A/B_*）。
"""

import os
import re
import sys
import json
import html
import time
import uuid
import logging
from datetime import datetime, timedelta
from typing import List, Tuple, Optional, Dict

import requests
import feedparser
from bs4 import BeautifulSoup
from dateutil import tz
from dotenv import load_dotenv
import pymysql

# ========== 可选中文翻译 ==========
TRANSLATE_TO_ZH = os.getenv("TRANSLATE_TO_ZH", "1") == "1"
try:
    from deep_translator import GoogleTranslator
    _gt = GoogleTranslator(source="auto", target="zh-CN")
except Exception:
    _gt = None
    TRANSLATE_TO_ZH = False

# ========== ENV ==========
load_dotenv()

BOT_TOKEN = (os.getenv("BOT_TOKEN") or os.getenv("TELEGRAM_BOT_TOKEN") or "").strip()
if not BOT_TOKEN:
    raise SystemExit("请在 .env 中配置 BOT_TOKEN/TELEGRAM_BOT_TOKEN")
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
HTTP_TIMEOUT = int(os.getenv("HTTP_TIMEOUT", "65"))

# 新闻/统计
INTERVAL_MINUTES = int(os.getenv("INTERVAL_MINUTES", "60"))
NEWS_ITEMS_PER_CAT = int(os.getenv("NEWS_ITEMS_PER_CAT", "8"))
STATS_ENABLED = os.getenv("STATS_ENABLED", "1") == "1"
MIN_MSG_CHARS = int(os.getenv("MIN_MSG_CHARS", "3"))

# 管理员（也会认可群管/群主）
ADMIN_USER_IDS = {int(x) for x in re.split(r"[,\s]+", os.getenv("ADMIN_USER_IDS", "").strip()) if x.isdigit()}

# 积分 & 规则
SCORE_CHECKIN_POINTS = int(os.getenv("SCORE_CHECKIN_POINTS", "1"))
SCORE_TOP_LIMIT = int(os.getenv("SCORE_TOP_LIMIT", "10"))
TOP_REWARD_SIZE = int(os.getenv("TOP_REWARD_SIZE", "10"))
DAILY_TOP_REWARD_START = int(os.getenv("DAILY_TOP_REWARD_START", "10"))
MONTHLY_REWARD_RULE = os.getenv(
    "MONTHLY_REWARD_RULE",
    "[6000,4000,2000,1000,600,600,600,600,600,600]"
)
MONTHLY_REWARD_RULE = [int(x) for x in json.loads(MONTHLY_REWARD_RULE)][:10]

# 兑换：100 分 = 1U；且积分需要 ≥ REDEEM_MIN_POINTS 才能兑换
REDEEM_RATE = int(os.getenv("REDEEM_RATE", "100"))
REDEEM_MIN_POINTS = int(os.getenv("REDEEM_MIN_POINTS", "10000"))

# 邀请积分：邀请 +10，被邀请人退群 -10（自动识别）
INVITE_REWARD_POINTS = int(os.getenv("INVITE_REWARD_POINTS", "10"))

# 调度时间
STATS_DAILY_AT = os.getenv("STATS_DAILY_AT", "23:50")
STATS_MONTHLY_AT = os.getenv("STATS_MONTHLY_AT", "00:10")

# 目标群（可为空 -> 从数据库里自动扫描）
NEWS_CHAT_IDS = [int(x) for x in re.split(r"[,\s]+", os.getenv("NEWS_CHAT_IDS", "").strip()) if x.isdigit()]
STATS_CHAT_IDS = [int(x) for x in re.split(r"[,\s]+", os.getenv("STATS_CHAT_IDS", "").strip()) if x.isdigit()]

# 广告/曝光/欢迎
AD_DEFAULT_ENABLED = os.getenv("AD_DEFAULT_ENABLED", "1") == "1"
WELCOME_PANEL_ENABLED = os.getenv("WELCOME_PANEL_ENABLED", "1") == "1"

# 招商按钮（可两种写法：BIZ_LINKS 或 A/B 键）
BIZ_LINKS = os.getenv("BIZ_LINKS", "").strip()  # 形如：招商A|https://t.me/xxx;招商B|https://t.me/yyy
BIZ_A_LABEL = os.getenv("BIZ_A_LABEL", "招商A")
BIZ_A_URL   = os.getenv("BIZ_A_URL", "").strip()
BIZ_B_LABEL = os.getenv("BIZ_B_LABEL", "招商B")
BIZ_B_URL   = os.getenv("BIZ_B_URL", "").strip()

# 日志
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
LOG_JSON  = os.getenv("LOG_JSON", "0") == "1"
RUN_ID = os.getenv("RUN_ID") or uuid.uuid4().hex[:8]

# 新闻源
def _env_list(key: str, default: List[str]) -> List[str]:
    raw = os.getenv(key, "").strip()
    if not raw: return default
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

# ========== 日志 ==========
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
                for k in ("chat_id","user_id","cmd","event","category","count","error","news_id"):
                    if hasattr(record,k): payload[k] = getattr(record,k)
                return json.dumps(payload, ensure_ascii=False)
        h.setFormatter(JsonFormatter())
    else:
        h.setFormatter(logging.Formatter("%(asctime)s %(levelname)s [%(name)s] %(message)s","%Y-%m-%d %H:%M:%S"))
    logger.handlers.clear()
    logger.addHandler(h)
    return logger
logger = setup_logger()
def log(level, msg, **ctx):
    if LOG_JSON:
        rec = logger.makeRecord("newsbot", level, fn="", lno=0, msg=msg, args=(), exc_info=None)
        for k,v in ctx.items(): setattr(rec,k,v)
        logger.handle(rec)
    else:
        logger.log(level, f"{msg} | {json.dumps(ctx, ensure_ascii=False)}" if ctx else msg)

# ========== 工具 ==========
def tz_now() -> datetime:
    return datetime.now(tz=LOCAL_TZ)
def utcnow() -> datetime:
    return datetime.utcnow().replace(tzinfo=tz.UTC)
def parse_hhmm(s: str) -> Tuple[int, int]:
    m = re.match(r"^\s*(\d{1,2}):(\d{2})\s*$", s or "")
    if not m: return (0,0)
    h, mi = int(m.group(1)), int(m.group(2))
    return max(0,min(23,h)), max(0,min(59,mi))
def safe_html(s: str) -> str:
    return html.escape(s or "", quote=False)
def human_name(username: str, first: str, last: str) -> str:
    if username: return f"@{username}"
    full = f"{first or ''} {last or ''}".strip()
    return full or "（匿名）"

# ========== Telegram API ==========
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
    return http_get("sendMessage", params=params)

def send_media_group(chat_id: int, media: List[dict]):
    return http_get("sendMediaGroup", json_data={"chat_id": chat_id, "media": media})

def send_photo(chat_id: int, file_id: str, caption: str = ""):
    return http_get("sendPhoto", params={"chat_id": chat_id, "photo": file_id, "caption": caption, "parse_mode": "HTML"})

def send_video(chat_id: int, file_id: str, caption: str = ""):
    return http_get("sendVideo", params={"chat_id": chat_id, "video": file_id, "caption": caption, "parse_mode": "HTML"})

def answer_callback_query(cb_id: str, text: str = "", show_alert: bool = False):
    return http_get("answerCallbackQuery", params={
        "callback_query_id": cb_id, "text": text, "show_alert": "true" if show_alert else "false"
    })

# ========== MySQL ==========
_DB = None
def _connect_mysql(dbname: Optional[str] = None):
    return pymysql.connect(
        host=MYSQL_HOST, port=MYSQL_PORT, user=MYSQL_USER, password=MYSQL_PASSWORD,
        database=dbname, charset="utf8mb4", autocommit=True, cursorclass=pymysql.cursors.Cursor,
    )
def get_conn():
    global _DB
    if _DB is None:
        try:
            _DB = _connect_mysql(MYSQL_DB)
        except pymysql.err.OperationalError as e:
            if e.args and e.args[0] == 1049:
                tmp = _connect_mysql("mysql")
                with tmp.cursor() as c:
                    c.execute(f"CREATE DATABASE IF NOT EXISTS `{MYSQL_DB}` DEFAULT CHARSET utf8mb4 COLLATE utf8mb4_unicode_ci;")
                tmp.close()
                _DB = _connect_mysql(MYSQL_DB)
            else:
                log(logging.ERROR, "mysql connect error", event="mysql_connect", error=str(e))
                raise
    else:
        _DB.ping(reconnect=True)
    return _DB
def _exec(sql: str, args: tuple = ()):
    with get_conn().cursor() as c:
        c.execute(sql, args); return c
def _fetchone(sql: str, args: tuple = ()):
    with _exec(sql, args) as c: return c.fetchone()
def _fetchall(sql: str, args: tuple = ()):
    with _exec(sql, args) as c: return c.fetchall()
def _safe_alter(sql: str):
    try:
        _exec(sql)
    except Exception:
        pass

def init_db():
    _exec("""
    CREATE TABLE IF NOT EXISTS msg_counts (
        chat_id BIGINT NOT NULL, user_id BIGINT NOT NULL,
        username VARCHAR(64), first_name VARCHAR(64), last_name VARCHAR(64),
        day CHAR(10) NOT NULL, cnt INT NOT NULL DEFAULT 0,
        PRIMARY KEY (chat_id,user_id,day),
        KEY idx_day (chat_id,day), KEY idx_user (chat_id,user_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
    """)
    _exec("""
    CREATE TABLE IF NOT EXISTS scores (
        chat_id BIGINT NOT NULL, user_id BIGINT NOT NULL,
        username VARCHAR(64), first_name VARCHAR(64), last_name VARCHAR(64),
        points INT NOT NULL DEFAULT 0, last_checkin CHAR(10),
        is_bot TINYINT NOT NULL DEFAULT 0,
        PRIMARY KEY (chat_id,user_id), KEY idx_points (chat_id,points)
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
        content TEXT,
        mode ENUM('attach','schedule','disabled') DEFAULT 'attach',
        times VARCHAR(200) DEFAULT NULL,
        updated_at VARCHAR(40)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
    """)
    _safe_alter("ALTER TABLE ads ADD COLUMN mode ENUM('attach','schedule','disabled') DEFAULT 'attach'")
    _safe_alter("ALTER TABLE ads ADD COLUMN times VARCHAR(200) DEFAULT NULL")
    _exec("""
    CREATE TABLE IF NOT EXISTS state (`key` VARCHAR(100) PRIMARY KEY, `val` TEXT)
    ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
    """)
    _exec("""
    CREATE TABLE IF NOT EXISTS posted_news (
        chat_id BIGINT, category VARCHAR(16), link TEXT, ts VARCHAR(40),
        PRIMARY KEY (chat_id, category(8), link(255))
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
    """)
    _exec("""
    CREATE TABLE IF NOT EXISTS exposures (
        id BIGINT PRIMARY KEY AUTO_INCREMENT,
        chat_id BIGINT NOT NULL,
        title VARCHAR(200), content TEXT,
        media_type ENUM('none','photo','video') DEFAULT 'none',
        file_id VARCHAR(256),
        enabled TINYINT NOT NULL DEFAULT 1,
        created_at VARCHAR(40), updated_at VARCHAR(40),
        KEY idx_chat (chat_id, enabled)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
    """)
    _exec("""
    CREATE TABLE IF NOT EXISTS expose_settings (
        chat_id BIGINT PRIMARY KEY,
        enabled TINYINT NOT NULL DEFAULT 0,
        updated_at VARCHAR(40)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
    """)
    _exec("""
    CREATE TABLE IF NOT EXISTS custom_news (
        id BIGINT PRIMARY KEY AUTO_INCREMENT,
        chat_id BIGINT NOT NULL,
        title VARCHAR(200), content TEXT,
        media_type ENUM('none','photo','video') DEFAULT 'none',
        file_id VARCHAR(256),
        status ENUM('draft','published') DEFAULT 'draft',
        created_by BIGINT,
        created_at VARCHAR(40), updated_at VARCHAR(40),
        KEY idx_chat (chat_id, status, id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
    """)

# ========== 状态 ==========
def state_get(key: str) -> Optional[str]:
    row = _fetchone("SELECT val FROM state WHERE `key`=%s", (key,))
    return row[0] if row else None
def state_set(key: str, val: str):
    _exec("INSERT INTO state(`key`,`val`) VALUES(%s,%s) ON DUPLICATE KEY UPDATE `val`=VALUES(`val`)", (key, val))
def state_del(key: str):
    _exec("DELETE FROM state WHERE `key`=%s", (key,))

# ========== 统计/积分 ==========
def _upsert_user_base(chat_id: int, frm: Dict):
    _exec(
        "INSERT INTO scores(chat_id,user_id,username,first_name,last_name,points,last_checkin,is_bot) "
        "VALUES (%s,%s,%s,%s,%s,%s,%s,%s) "
        "ON DUPLICATE KEY UPDATE username=VALUES(username), first_name=VALUES(first_name), last_name=VALUES(last_name), is_bot=VALUES(is_bot)",
        (chat_id, frm.get("id"), (frm.get("username") or "")[:64], (frm.get("first_name") or "")[:64],
         (frm.get("last_name") or "")[:64], 0, None, 1 if frm.get("is_bot") else 0),
    )
def _add_points(chat_id: int, target_id: int, delta: int, actor_id: int, reason: str = ""):
    _exec("INSERT INTO scores(chat_id,user_id,points) VALUES(%s,%s,%s) ON DUPLICATE KEY UPDATE points=points+VALUES(points)",
          (chat_id, target_id, delta))
    _exec("INSERT INTO score_logs(chat_id,actor_id,target_id,delta,reason,ts) VALUES(%s,%s,%s,%s,%s,%s)",
          (chat_id, actor_id, target_id, delta, reason or "", utcnow().isoformat()))
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
    return ids

def ensure_user_display(chat_id: int, uid: int, triplet: Tuple[str,str,str]):
    un, fn, ln = triplet
    if un or fn or ln: return un, fn, ln
    r = http_get("getChatMember", params={"chat_id": chat_id, "user_id": uid})
    user = ((r or {}).get("result") or {}).get("user") or {}
    un2 = user.get("username") or ""; fn2 = user.get("first_name") or ""; ln2 = user.get("last_name") or ""
    if un2 or fn2 or ln2:
        _exec("UPDATE scores SET username=%s, first_name=%s, last_name=%s WHERE chat_id=%s AND user_id=%s",
              (un2, fn2, ln2, chat_id, uid))
        return un2, fn2, ln2
    return un, fn, ln

def list_top_day(chat_id: int, day: str, limit: int = 10):
    return _fetchall(
        """SELECT user_id, MAX(username), MAX(first_name), MAX(last_name), SUM(cnt) AS c
           FROM msg_counts WHERE chat_id=%s AND day=%s
           GROUP BY user_id ORDER BY c DESC LIMIT %s""",
        (chat_id, day, limit)
    )
def list_top_month(chat_id: int, ym: str, limit: int = 10):
    return _fetchall(
        """SELECT user_id, MAX(username), MAX(first_name), MAX(last_name), SUM(cnt) AS c
           FROM msg_counts WHERE chat_id=%s AND day LIKE CONCAT(%s,'-%')
           GROUP BY user_id ORDER BY c DESC LIMIT %s""",
        (chat_id, ym, limit)
    )

def eligible_member_count(chat_id: int) -> int:
    admin_ids = list_chat_admin_ids(chat_id)
    ids = _fetchall("SELECT user_id FROM scores WHERE chat_id=%s AND COALESCE(is_bot,0)=0", (chat_id,))
    return len([i[0] for i in ids if i[0] not in admin_ids])

# ========== 广告 ==========
def ad_get(chat_id: int):
    row = _fetchone("SELECT enabled, content, COALESCE(mode,'attach'), COALESCE(times,'') FROM ads WHERE chat_id=%s", (chat_id,))
    if row:
        en, ct, mode, times = int(row[0])==1, row[1] or "", row[2] or "attach", row[3] or ""
        return en, ct, mode, times
    _exec("INSERT IGNORE INTO ads(chat_id,enabled,content,mode,times,updated_at) VALUES(%s,%s,%s,%s,%s,%s)",
          (chat_id, 1 if AD_DEFAULT_ENABLED else 0, "", "attach", "", utcnow().isoformat()))
    return AD_DEFAULT_ENABLED, "", "attach", ""
def ad_set(chat_id: int, content: str):
    _exec("INSERT INTO ads(chat_id,enabled,content,updated_at) VALUES(%s,%s,%s,%s) "
          "ON DUPLICATE KEY UPDATE content=VALUES(content), updated_at=VALUES(updated_at)",
          (chat_id, 1 if AD_DEFAULT_ENABLED else 0, content, utcnow().isoformat()))
def ad_enable(chat_id: int, enabled: bool):
    _exec("INSERT INTO ads(chat_id,enabled,updated_at) VALUES(%s,%s,%s) "
          "ON DUPLICATE KEY UPDATE enabled=VALUES(enabled), updated_at=VALUES(updated_at)",
          (chat_id, 1 if enabled else 0, utcnow().isoformat()))
def ad_clear(chat_id: int):
    _exec("UPDATE ads SET content=%s, updated_at=%s WHERE chat_id=%s", ("", utcnow().isoformat(), chat_id))
def ad_set_mode(chat_id: int, mode: str):
    if mode not in ("attach","schedule","disabled"): return
    _exec("UPDATE ads SET mode=%s, enabled=%s, updated_at=%s WHERE chat_id=%s",
          (mode, 0 if mode=="disabled" else 1, utcnow().isoformat(), chat_id))
def _norm_times_str(times: str) -> str:
    lst = []
    for p in re.split(r"[,\s]+", times or ""):
        if not p: continue
        m = re.match(r"^(\d{1,2}):(\d{2})$", p)
        if not m: continue
        h,mi = int(m.group(1)), int(m.group(2))
        if 0<=h<=23 and 0<=mi<=59: lst.append(f"{h:02d}:{mi:02d}")
    lst = sorted(set(lst))
    return ",".join(lst)
def ad_set_times(chat_id: int, times: str):
    t = _norm_times_str(times)
    _exec("UPDATE ads SET times=%s, updated_at=%s WHERE chat_id=%s", (t, utcnow().isoformat(), chat_id))
    return t
def ad_send_now(chat_id: int):
    en, ct, mode, times = ad_get(chat_id)
    if not ct.strip():
        send_message_html(chat_id, "📣 广告内容为空，无法发送。"); return
    if not en:
        send_message_html(chat_id, "📣 广告当前处于禁用状态。"); return
    send_message_html(chat_id, "📣 <b>广告</b>\n" + safe_html(ct))

# ========== 报表 ==========
def build_daily_report(chat_id: int, day: str) -> str:
    rows = list_top_day(chat_id, day, limit=10)
    total = _fetchone("SELECT SUM(cnt) FROM msg_counts WHERE chat_id=%s AND day=%s", (chat_id, day))[0] or 0
    speakers = _fetchone("SELECT COUNT(DISTINCT user_id) FROM msg_counts WHERE chat_id=%s AND day=%s", (chat_id, day))[0] or 0
    members = eligible_member_count(chat_id)
    lines = [
        f"📊 <b>{day} 发言统计</b>",
        f"参与成员（剔除管理员/机器人）：<b>{members}</b>｜发言人数：<b>{speakers}</b>｜总条数：<b>{total}</b>",
        "<code>────────────────</code>"
    ]
    if not rows:
        lines.append("暂无数据。"); return "\n".join(lines)
    for i,(uid,un,fn,ln,c) in enumerate(rows,1):
        un,fn,ln = ensure_user_display(chat_id, uid, (un,fn,ln))
        lines.append(f"{i}. {safe_html(human_name(un,fn,ln))} — <b>{c}</b>")
    return "\n".join(lines)

def build_monthly_report(chat_id: int, ym: str) -> str:
    rows = list_top_month(chat_id, ym, limit=10)
    total = _fetchone("SELECT SUM(cnt) FROM msg_counts WHERE chat_id=%s AND day LIKE CONCAT(%s,'-%')", (chat_id, ym))[0] or 0
    speakers = _fetchone("SELECT COUNT(DISTINCT user_id) FROM msg_counts WHERE chat_id=%s AND day LIKE CONCAT(%s,'-%')", (chat_id, ym))[0] or 0
    members = eligible_member_count(chat_id)
    lines = [
        f"📈 <b>{ym} 月度发言统计</b>",
        f"参与成员（剔除管理员/机器人）：<b>{members}</b>｜发言人数：<b>{speakers}</b>｜总条数：<b>{total}</b>",
        "<code>────────────────</code>"
    ]
    if not rows:
        lines.append("暂无数据。"); return "\n".join(lines)
    for i,(uid,un,fn,ln,c) in enumerate(rows,1):
        un,fn,ln = ensure_user_display(chat_id, uid, (un,fn,ln))
        lines.append(f"{i}. {safe_html(human_name(un,fn,ln))} — <b>{c}</b>")
    return "\n".join(lines)

# ========== 曝光台 ==========
def expose_enabled(chat_id: int) -> bool:
    row = _fetchone("SELECT enabled FROM expose_settings WHERE chat_id=%s", (chat_id,))
    if not row:
        _exec("INSERT IGNORE INTO expose_settings(chat_id,enabled,updated_at) VALUES(%s,%s,%s)",
              (chat_id, 0, utcnow().isoformat()))
        return False
    return int(row[0])==1
def expose_set_enabled(chat_id: int, enabled: bool):
    _exec("INSERT INTO expose_settings(chat_id,enabled,updated_at) VALUES(%s,%s,%s) "
          "ON DUPLICATE KEY UPDATE enabled=VALUES(enabled), updated_at=VALUES(updated_at)",
          (chat_id, 1 if enabled else 0, utcnow().isoformat()))
def expose_add(chat_id: int, title: str, content: str, media_type: str, file_id: Optional[str]):
    _exec("INSERT INTO exposures(chat_id,title,content,media_type,file_id,enabled,created_at,updated_at) "
          "VALUES(%s,%s,%s,%s,%s,1,%s,%s)",
          (chat_id, title[:200] if title else None, content, media_type, file_id, utcnow().isoformat(), utcnow().isoformat()))
def expose_clear(chat_id: int):
    _exec("DELETE FROM exposures WHERE chat_id=%s", (chat_id,))
def expose_list(chat_id: int, limit: int = 10):
    return _fetchall("SELECT id,title,content,media_type,file_id FROM exposures WHERE chat_id=%s AND enabled=1 ORDER BY id DESC LIMIT %s",
                     (chat_id, limit))
def send_exposures(chat_id: int):
    if not expose_enabled(chat_id): return
    rows = expose_list(chat_id, 10)
    if not rows: return
    media, texts = [], []
    for _id,title,content,mtype,fid in rows:
        title = title or "曝光"
        caption = f"📌 <b>{safe_html(title)}</b>\n{safe_html(content or '')}".strip()
        if mtype in ("photo","video") and fid:
            media.append({"type": mtype, "media": fid, "caption": caption[:1024], "parse_mode": "HTML"})
        else:
            texts.append(f"• <b>{safe_html(title)}</b>\n{safe_html(content or '')}")
    if media: send_media_group(chat_id, media[:10])
    if texts: send_message_html(chat_id, "📌 <b>曝光台</b>\n" + "\n\n".join(texts))

# ========== 自定义新闻 ==========
def cnews_create(chat_id: int, uid: int, title: str, content: str, mtype: str, fid: Optional[str]) -> int:
    _exec("INSERT INTO custom_news(chat_id,title,content,media_type,file_id,status,created_by,created_at,updated_at) "
          "VALUES(%s,%s,%s,%s,%s,'draft',%s,%s,%s)",
          (chat_id, title[:200] if title else None, content, mtype, fid, uid, utcnow().isoformat(), utcnow().isoformat()))
    row = _fetchone("SELECT LAST_INSERT_ID()", ())
    return int(row[0])
def cnews_update(chat_id: int, nid: int, title: str, content: str, mtype: str, fid: Optional[str]):
    _exec("UPDATE custom_news SET title=%s, content=%s, media_type=%s, file_id=%s, updated_at=%s WHERE chat_id=%s AND id=%s",
          (title[:200] if title else None, content, mtype, fid, utcnow().isoformat(), chat_id, nid))
def cnews_get(chat_id: int, nid: int):
    return _fetchone("SELECT id,title,content,media_type,file_id,status FROM custom_news WHERE chat_id=%s AND id=%s",
                     (chat_id, nid))
def cnews_list(chat_id: int, status: str = "draft", limit: int = 10):
    return _fetchall("SELECT id,title,status FROM custom_news WHERE chat_id=%s AND status=%s ORDER BY id DESC LIMIT %s",
                     (chat_id, status, limit))
def cnews_delete(chat_id: int, nid: int):
    _exec("DELETE FROM custom_news WHERE chat_id=%s AND id=%s", (chat_id, nid))
def _cnews_caption(title: str, content: str, prefix: str = "📰 自定义新闻") -> str:
    t = f"{prefix}\n<b>{safe_html(title or '')}</b>"
    body = safe_html(content or "")
    return f"{t}\n{body}".strip()
def cnews_publish(chat_id: int, nid: int, preview: bool = False):
    row = cnews_get(chat_id, nid)
    if not row:
        send_message_html(chat_id, f"未找到自定义新闻 #{nid}"); return
    _id, title, content, mtype, fid, status = row
    cap = _cnews_caption(title, content, prefix=("🧪 预览" if preview else "📰 自定义新闻"))
    if mtype == "photo" and fid:
        send_photo(chat_id, fid, cap[:1024])
    elif mtype == "video" and fid:
        send_video(chat_id, fid, cap[:1024])
    else:
        send_message_html(chat_id, cap)
    if not preview:
        en, adct, mode, _ = ad_get(chat_id)
        if en and mode == "attach" and adct.strip():
            send_message_html(chat_id, "📣 <b>广告</b>\n" + safe_html(adct))
        send_exposures(chat_id)
        _exec("UPDATE custom_news SET status='published', updated_at=%s WHERE chat_id=%s AND id=%s",
              (utcnow().isoformat(), chat_id, nid))

# ========== 新闻抓取（含中文） ==========
def clean_text(s: str) -> str:
    if not s: return ""
    soup = BeautifulSoup(s, "html.parser")
    return re.sub(r"\s+", " ", soup.get_text().strip())
def _zh(s: str) -> str:
    if not s: return ""
    if not TRANSLATE_TO_ZH or _gt is None: return s
    try: return _gt.translate(s)
    except Exception: return s
def fetch_rss_list(urls: List[str], max_items: int) -> List[Dict]:
    items = []
    for u in urls:
        try:
            feed = feedparser.parse(u)
            for e in feed.entries[: max_items * 2]:
                title = clean_text(e.get("title"))
                link = e.get("link") or ""
                summary = clean_text(e.get("summary") or e.get("description"))
                if title and link: items.append({"title": title, "link": link, "summary": summary})
        except Exception as e:
            log(logging.WARNING, "rss parse error", event="rss", error=f"{u} {e}")
    seen, uniq = set(), []
    for it in items:
        if it["link"] in seen: continue
        seen.add(it["link"]); uniq.append(it)
        if len(uniq)>=max_items: break
    return uniq
def already_posted(chat_id: int, category: str, link: str) -> bool:
    return _fetchone("SELECT 1 FROM posted_news WHERE chat_id=%s AND category=%s AND link=%s",
                     (chat_id, category, link)) is not None
def mark_posted(chat_id: int, category: str, link: str):
    _exec("INSERT IGNORE INTO posted_news(chat_id,category,link,ts) VALUES(%s,%s,%s,%s)",
          (chat_id, category, link, utcnow().isoformat()))
def push_news_once(chat_id: int):
    order = ["finance","sea","war"]
    now_str = tz_now().strftime("%Y-%m-%d %H:%M")
    sent_any = False
    for cat in order:
        cname, feeds = CATEGORY_MAP.get(cat, (cat, []))
        items = fetch_rss_list(feeds, NEWS_ITEMS_PER_CAT)
        if not items: continue
        new_items = [it for it in items if not already_posted(chat_id, cat, it["link"])]
        if not new_items: continue
        lines = [f"🗞️ <b>{cname}</b> | {now_str}", "<code>────────────────</code>"]
        for i,it in enumerate(new_items,1):
            t = _zh(it['title']); s = _zh(it.get('summary') or "")
            if s: lines.append(f"{i}. {safe_html(t)}\n{safe_html(s)}\n{it['link']}")
            else: lines.append(f"{i}. {safe_html(t)}\n{it['link']}")
        en, content, mode, _times = ad_get(chat_id)
        if en and mode == "attach" and content.strip():
            lines.append("<code>────────────────</code>")
            lines.append(f"📣 <b>广告</b>\n{safe_html(content)}")
        send_message_html(chat_id, "\n".join(lines))
        send_exposures(chat_id)
        for it in new_items: mark_posted(chat_id, cat, it["link"])
        sent_any = True
    if not sent_any:
        send_message_html(chat_id, "🗞️ 暂无可用新闻（可能源不可达或暂无更新）。")

# ========== 菜单/帮助/规则 ==========
def ikb(text: str, data: str) -> dict:
    return {"text": text, "callback_data": data}
def urlb(text: str, url: str) -> dict:
    return {"text": text, "url": url}

def is_chat_admin(chat_id: int, uid: Optional[int]) -> bool:
    if not uid: return False
    if uid in ADMIN_USER_IDS: return True
    if uid in list_chat_admin_ids(chat_id): return True
    r = http_get("getChatMember", params={"chat_id": chat_id, "user_id": uid})
    try:
        status = ((r or {}).get("result") or {}).get("status", "")
        return status in ("administrator","creator")
    except Exception:
        return False

def get_biz_buttons() -> List[dict]:
    """读取 .env 里的招商链接，返回 URL 按钮列表"""
    btns: List[dict] = []
    raw = (BIZ_LINKS or "").strip()
    if raw:
        for item in raw.split(";"):
            item = item.strip()
            if not item: continue
            if "|" in item:
                label, link = item.split("|", 1)
            else:
                label, link = item, item
            label = (label or "").strip() or "招商"
            link = (link or "").strip()
            if not link: continue
            btns.append(urlb(label, link))
    else:
        if BIZ_A_URL: btns.append(urlb(BIZ_A_LABEL or "招商A", BIZ_A_URL))
        if BIZ_B_URL: btns.append(urlb(BIZ_B_LABEL or "招商B", BIZ_B_URL))
    return btns

def build_menu(is_admin_user: bool, chat_id: Optional[int]=None) -> dict:
    kb = [
        [ikb("✅ 签到","ACT_CHECKIN")],
        [ikb("📌 我的积分","ACT_SCORE"), ikb("🏆 积分榜Top10","ACT_TOP10")],
        [ikb("📊 今日统计","ACT_SD_TODAY"), ikb("📊 昨日统计","ACT_SD_YESTERDAY")],
        [ikb("📈 本月统计","ACT_SM_THIS"), ikb("📜 规则","ACT_RULES")],
        [ikb("🎁 兑换U","ACT_REDEEM")],
        [ikb("🆘 帮助","ACT_HELP")],
    ]
    if chat_id and expose_enabled(chat_id):
        kb.insert(3, [ikb("📌 曝光台", "ACT_EXP_SHOW")])
    if is_admin_user:
        kb.append([ikb("📰 自定义新闻","ACT_CNEWS_PANEL")])
        kb.append([ikb("📣 广告显示","ACT_AD_SHOW"), ikb("🟢 启用广告","ACT_AD_ENABLE"), ikb("🔴 禁用广告","ACT_AD_DISABLE")])
        kb.append([ikb("📎 设为附加模式","ACT_AD_MODE_ATTACH"), ikb("⏰ 设为定时模式","ACT_AD_MODE_SCHEDULE")])
        kb.append([ikb("🕒 设置时间点","ACT_AD_SET_TIMES"), ikb("📤 立即发送一次","ACT_AD_SEND_NOW")])
        kb.append([ikb("🧹 清空广告","ACT_AD_CLEAR"), ikb("✍️ 设置广告","ACT_AD_SET")])
        kb.append([ikb("🗞 立即推送新闻","ACT_NEWS_NOW")])
        kb.append([ikb("➕ 添加曝光","ACT_EXP_ADD"), ikb("🧹 清空曝光","ACT_EXP_CLEAR"),
                   ikb("🟢 开启曝光" if not expose_enabled(chat_id) else "🔴 关闭曝光","ACT_EXP_TOGGLE")])

    # —— 菜单尾部：招商按钮（URL 跳转）
    biz_btns = get_biz_buttons()
    if biz_btns:
        # 2~3 个一行排布
        row: List[dict] = []
        for b in biz_btns:
            row.append(b)
            if len(row) == 3:
                kb.append(row); row = []
        if row: kb.append(row)

    return {"inline_keyboard": kb}

def build_rules_text(chat_id: int) -> str:
    lines = [
        "📜 <b>群积分规则</b>",
        "<code>────────────────</code>",
        "🏆 <b>月度排名奖励</b>",
        "  1️⃣ 6000 分",
        "  2️⃣ 4000 分",
        "  3️⃣ 2000 分",
        "  4️⃣ 1000 分",
        "  5️⃣–🔟 各 600 分",
        "",
        f"🗓️ <b>每日签到</b>：每天 +{SCORE_CHECKIN_POINTS} 分",
        f"💬 <b>发言统计</b>：消息≥{MIN_MSG_CHARS} 字计入；支持日/月统计与奖励",
        f"🤝 <b>邀请加分</b>：成功邀请 +{INVITE_REWARD_POINTS} 分；被邀请人退群 -{INVITE_REWARD_POINTS} 分",
        f"💱 <b>兑换</b>：{REDEEM_RATE} 分 = 1 U；<b>满 {REDEEM_MIN_POINTS} 分</b>方可兑换",
        f"❌ <b>清零</b>：离群清零，或者兑换完清零.",
    ]
    en, _ct, mode, _times = ad_get(chat_id)
    if en and mode == "attach":
        lines.append("📣 <b>广告</b>：可能附在新闻或自定义新闻后（启用且为附加模式时显示）")
    if expose_enabled(chat_id):
        lines.append("📌 <b>曝光台</b>：群友可查看，管理员可添加图文/视频")
    return "\n".join(lines)

def send_menu_for(chat_id: int, uid: int):
    send_message_html(chat_id, "请选择功能：", reply_markup=build_menu(is_chat_admin(chat_id, uid), chat_id))

# ========== 自定义新闻面板 ==========
def cnews_panel(chat_id: int, uid: int):
    if not is_chat_admin(chat_id, uid):
        send_message_html(chat_id, "❌ 你没有权限操作自定义新闻。"); return
    kb = {"inline_keyboard":[
        [ikb("➕ 新建草稿","ACT_CNEWS_NEW"), ikb("🗂 草稿列表","ACT_CNEWS_LIST_D")],
        [ikb("📰 已发布","ACT_CNEWS_LIST_P")]
    ]}
    send_message_html(chat_id, "📰 <b>自定义新闻</b>\n• 新建：点击后回复文本（首行标题）+ 可选图/视频\n• 草稿列表：可预览/发布/编辑/删除\n• 已发布：查看已发列表", reply_markup=kb)
def cnews_list_message(chat_id: int, status: str):
    rows = cnews_list(chat_id, status=status, limit=10)
    if not rows:
        send_message_html(chat_id, "暂无记录。"); return
    lines = [f"📰 <b>自定义新闻 · {('草稿' if status=='draft' else '已发布')}</b>"]
    ik = []
    for (nid,title,st) in rows:
        lines.append(f"#{nid} — {safe_html(title or '(无标题)')}")
        if status == "draft":
            ik.append([ikb(f"🔍预览#{nid}", f"ACT_CNEWS_PRE:{nid}"),
                       ikb(f"📤发布#{nid}", f"ACT_CNEWS_PUB:{nid}"),
                       ikb(f"✏️编辑#{nid}", f"ACT_CNEWS_EDIT:{nid}"),
                       ikb(f"🗑删除#{nid}", f"ACT_CNEWS_DEL:{nid}")])
        else:
            ik.append([ikb(f"🗑删除#{nid}", f"ACT_CNEWS_DEL:{nid}")])
    send_message_html(chat_id, "\n".join(lines), reply_markup={"inline_keyboard":ik})

# ========== 邀请识别（自动绑定/加分 & 退群扣分） ==========
def _bind_invite_if_needed(chat_id: int, invitee: Dict, inviter: Optional[Dict]):
    """给邀请人加分（若尚未绑定）。inviter 可能为 None（链接无法识别时跳过）"""
    if not invitee or not invitee.get("id"): return
    invitee_id = invitee["id"]
    if inviter and inviter.get("id") and inviter["id"] != invitee_id:
        exists = _fetchone("SELECT 1 FROM invites WHERE chat_id=%s AND invitee_id=%s", (chat_id, invitee_id))
        if not exists:
            _exec("INSERT INTO invites(chat_id,invitee_id,inviter_id,ts) VALUES(%s,%s,%s,%s)",
                  (chat_id, invitee_id, inviter["id"], utcnow().isoformat()))
            _upsert_user_base(chat_id, inviter)
            _add_points(chat_id, inviter["id"], INVITE_REWARD_POINTS, inviter["id"], "invite_auto_join")

def handle_chat_member_update(obj: Dict):
    """处理 chat_member 更新，识别邀请人及退群"""
    chat = obj.get("chat") or {}; chat_id = chat.get("id")
    changer = obj.get("from") or {}               # 执行操作的管理员
    oldm = obj.get("old_chat_member") or {}
    newm = obj.get("new_chat_member") or {}
    invite_link = obj.get("invite_link") or {}    # 通过邀请链接加入时提供
    old_status = (oldm.get("status") or "").lower()
    new_status = (newm.get("status") or "").lower()
    target_user = (newm.get("user") or {})        # 被变更的成员

    if not chat_id or not target_user: return

    # 加入：left/kicked -> member/administrator/restricted
    if old_status in ("left","kicked") and new_status in ("member","administrator","restricted"):
        inviter = None
        # 1) 通过邀请链接加入：用 link 的创建者作为邀请人
        creator = (invite_link.get("creator") or {})
        if creator.get("id"):
            inviter = creator
        # 2) 管理员手动拉人：from 即邀请人
        elif changer.get("id") and changer.get("id") != target_user.get("id"):
            inviter = changer
        _upsert_user_base(chat_id, target_user)
        _bind_invite_if_needed(chat_id, target_user, inviter)
        return

    # 退群：member/restricted -> left/kicked
    if old_status in ("member","restricted") and new_status in ("left","kicked"):
        invitee_id = (oldm.get("user") or {}).get("id") or target_user.get("id")
        if not invitee_id: return
        row = _fetchone("SELECT inviter_id FROM invites WHERE chat_id=%s AND invitee_id=%s", (chat_id, invitee_id))
        if not row: return
        inviter_id = row[0]
        _add_points(chat_id, inviter_id, -INVITE_REWARD_POINTS, inviter_id, "invite_auto_leave")
        _exec("DELETE FROM invites WHERE chat_id=%s AND invitee_id=%s", (chat_id, invitee_id))

# ========== 查找用户 ==========
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
        if p.startswith("@") and len(p)>1:
            row = find_user_by_mention(chat_id, p)
            if row:
                uid, un, fn, ln = row
                return (chat_id, uid, un, fn, ln)
    return (None,None,None,None,None)

# ========== 命令（保留，按钮主用） ==========
def handle_admin_ad_command(msg: Dict) -> bool:
    chat_id = (msg.get("chat") or {}).get("id")
    frm = msg.get("from") or {}; uid = frm.get("id")
    if not is_chat_admin(chat_id, uid):
        send_message_html(chat_id, "❌ 你没有权限执行该命令。"); return True
    txt = (msg.get("text") or "").strip()
    if txt.startswith("/ad_help"):
        send_message_html(chat_id,
            "📢 <b>广告位命令</b>\n"
            "• /ad_set <文本...> —— 设置/覆盖广告内容\n"
            "• /ad_show —— 查看当前广告与状态\n"
            "• /ad_clear —— 清空广告内容\n"
            "• /ad_enable —— 启用广告位\n"
            "• /ad_disable —— 禁用广告位（隐藏，暂不发布）\n"
            "• /ad_mode_attach —— 设为附加到新闻模式\n"
            "• /ad_mode_schedule —— 设为定时发送模式\n"
            "• /ad_times HH:MM,HH:MM —— 设置每日时间点\n"
            "• /ad_send_now —— 立即发送一次")
        return True
    if txt.startswith("/ad_set"):
        parts = txt.split(" ",1)
        if len(parts)<2 or not parts[1].strip():
            send_message_html(chat_id,"用法：/ad_set <广告文本>"); return True
        ad_set(chat_id, parts[1].strip()); send_message_html(chat_id,"✅ 广告内容已更新。"); return True
    if txt.startswith("/ad_show"):
        en, ct, mode, times = ad_get(chat_id); st = "启用" if en else "禁用"
        send_message_html(chat_id, f"📣 当前：<b>{st}</b>  · 模式：<b>{mode}</b>\n🕒 时间点：{_norm_times_str(times) or '（未设置）'}\n内容：\n{safe_html(ct) if ct else '（空）'}"); return True
    if txt.startswith("/ad_clear"):
        ad_clear(chat_id); send_message_html(chat_id,"✅ 已清空广告内容。"); return True
    if txt.startswith("/ad_enable"):
        ad_enable(chat_id, True); send_message_html(chat_id,"✅ 已启用广告位。"); return True
    if txt.startswith("/ad_disable"):
        ad_enable(chat_id, False); send_message_html(chat_id,"✅ 已禁用广告位。"); return True
    if txt.startswith("/ad_mode_attach"):
        ad_set_mode(chat_id, "attach"); send_message_html(chat_id,"✅ 已设为附加模式。"); return True
    if txt.startswith("/ad_mode_schedule"):
        ad_set_mode(chat_id, "schedule"); send_message_html(chat_id,"✅ 已设为定时模式。"); return True
    if txt.startswith("/ad_times"):
        parts = txt.split(" ",1)
        if len(parts)<2: send_message_html(chat_id,"用法：/ad_times HH:MM,HH:MM"); return True
        t = ad_set_times(chat_id, parts[1])
        send_message_html(chat_id, f"✅ 时间点已设置：{t or '（空）'}"); return True
    if txt.startswith("/ad_send_now"):
        ad_send_now(chat_id); return True
    return False

def handle_admin_cnews_command(msg: Dict) -> bool:
    chat_id = (msg.get("chat") or {}).get("id")
    frm = msg.get("from") or {}; uid = frm.get("id")
    if not is_chat_admin(chat_id, uid):
        send_message_html(chat_id, "❌ 你没有权限。"); return True
    txt = (msg.get("text") or "").strip()
    if txt.startswith("/cnews_help"):
        send_message_html(chat_id,
            "📰 <b>自定义新闻命令</b>\n"
            "• /cnews_new —— 新建草稿\n"
            "• /cnews_list —— 草稿列表\n"
            "• /cnews_pub <id> —— 发布\n"
            "• /cnews_del <id> —— 删除\n"
            "• /cnews_edit <id> —— 编辑（随后回复新内容）")
        return True
    if txt.startswith("/cnews_new"):
        state_set(f"pending:cnewsnew:{chat_id}:{uid}","1")
        send_message_html(chat_id, "请在本条消息下<b>回复文本</b>（首行标题，其余正文），可附带图片/视频。")
        return True
    if txt.startswith("/cnews_list"):
        cnews_list_message(chat_id, "draft"); return True
    if txt.startswith("/cnews_pub"):
        parts = txt.split()
        if len(parts)<2 or not parts[1].isdigit(): send_message_html(chat_id,"用法：/cnews_pub <id>"); return True
        cnews_publish(chat_id, int(parts[1]), preview=False); return True
    if txt.startswith("/cnews_del"):
        parts = txt.split()
        if len(parts)<2 or not parts[1].isdigit(): send_message_html(chat_id,"用法：/cnews_del <id>"); return True
        cnews_delete(chat_id, int(parts[1])); send_message_html(chat_id,"✅ 已删除。"); return True
    if txt.startswith("/cnews_edit"):
        parts = txt.split()
        if len(parts)<2 or not parts[1].isdigit(): send_message_html(chat_id,"用法：/cnews_edit <id>"); return True
        nid = int(parts[1])
        state_set(f"pending:cnewsedit:{chat_id}:{uid}:{nid}","1")
        state_set(f"pending:cnewsedit:last:{chat_id}:{uid}", str(nid))
        send_message_html(chat_id,"请回复新文本（首行标题）+ 可选图/视频，用于覆盖该草稿。")
        return True
    return False

def handle_general_command(msg: Dict) -> bool:
    chat_id = (msg.get("chat") or {}).get("id")
    frm = msg.get("from") or {}; uid = frm.get("id")
    txt = (msg.get("text") or "").strip()
    if not txt or not txt.startswith("/"): return False

    if txt.startswith("/ad_"):
        return handle_admin_ad_command(msg)
    if txt.startswith("/cnews_"):
        return handle_admin_cnews_command(msg)

    parts = txt.split(); cmd = parts[0].lower()
    if cmd in ("/menu","/start","/help"):
        send_menu_for(chat_id, uid); return True

    if cmd == "/rules":
        send_message_html(chat_id, build_rules_text(chat_id)); return True

    _upsert_user_base(chat_id, frm)

    if cmd == "/checkin":
        today = tz_now().strftime("%Y-%m-%d")
        if _get_last_checkin(chat_id, uid) == today:
            send_message_html(chat_id, f"✅ 你今天已经签到过啦（{today}）。"); return True
        _add_points(chat_id, uid, SCORE_CHECKIN_POINTS, uid, "daily_checkin")
        _set_last_checkin(chat_id, uid, today)
        send_message_html(chat_id, f"🎉 签到成功 +{SCORE_CHECKIN_POINTS} 分！当前积分：<b>{_get_points(chat_id, uid)}</b>"); return True

    if cmd == "/score":
        send_message_html(chat_id, f"📌 你的当前积分：<b>{_get_points(chat_id, uid)}</b>"); return True

    if cmd == "/score_top":
        limit = SCORE_TOP_LIMIT
        if len(parts)>=2 and parts[1].isdigit(): limit = max(1,min(50,int(parts[1])))
        rows = _fetchall("SELECT user_id,username,first_name,last_name,points FROM scores WHERE chat_id=%s ORDER BY points DESC LIMIT %s",
                         (chat_id, limit))
        if not rows: send_message_html(chat_id,"暂无积分数据。"); return True
        lines = ["🏆 <b>积分榜</b>","<code>────────────────</code>"]
        for i,(uid2,u,f,l,p) in enumerate(rows,1):
            u,f,l = ensure_user_display(chat_id, uid2, (u,f,l))
            lines.append(f"{i}. {safe_html(human_name(u,f,l))} — <b>{p}</b> 分")
        send_message_html(chat_id,"\n".join(lines)); return True

    if cmd in ("/score_add","/score_deduct"):
        if not is_chat_admin(chat_id, uid):
            send_message_html(chat_id,"❌ 你没有权限执行此命令。"); return True
        tgt_chat,tgt_id,un,fn,ln = target_user_from_msg(msg)
        if not tgt_id:
            send_message_html(chat_id,"请对目标成员的消息回复命令，或在命令后带 @username。示例：/score_add @user 5"); return True
        if len(parts)>=2 and parts[-1].lstrip("-").isdigit(): delta = int(parts[-1])
        else: send_message_html(chat_id,"请在命令末尾给出整数分值。例如：/score_deduct 3"); return True
        delta = -abs(delta) if cmd=="/score_deduct" else abs(delta)
        _upsert_user_base(chat_id, {"id":tgt_id,"username":un,"first_name":fn,"last_name":ln})
        _add_points(chat_id, tgt_id, delta, uid, cmd[1:])
        send_message_html(chat_id, f"✅ 已为 {safe_html(human_name(un,fn,ln))} 变更积分：{'+' if delta>0 else ''}{delta}，当前积分 <b>{_get_points(chat_id,tgt_id)}</b>"); return True

    if cmd == "/stats_day":
        day = (tz_now()-timedelta(days=1)).strftime("%Y-%m-%d")
        if len(parts)>=2:
            p=parts[1].lower()
            day = tz_now().strftime("%Y-%m-%d") if p=="today" else ((tz_now()-timedelta(days=1)).strftime("%Y-%m-%d") if p=="yesterday" else parts[1])
        send_message_html(chat_id, build_daily_report(chat_id, day)); return True

    if cmd == "/stats_month":
        ym = tz_now().strftime("%Y-%m")
        if len(parts)>=2:
            p=parts[1].lower()
            ym = tz_now().strftime("%Y-%m") if p=="this" else ((tz_now().replace(day=1)-timedelta(days=1)).strftime("%Y-%m") if p=="last" else parts[1])
        send_message_html(chat_id, build_monthly_report(chat_id, ym)); return True

    if cmd == "/news_now":
        if not is_chat_admin(chat_id, uid):
            send_message_html(chat_id,"❌ 你没有权限执行此命令。"); return True
        push_news_once(chat_id)
        state_set("next_news_at", (tz_now()+timedelta(minutes=INTERVAL_MINUTES)).isoformat())
        return True

    if cmd == "/redeem":
        pts = _get_points(chat_id, uid)
        if pts < REDEEM_MIN_POINTS:
            send_message_html(chat_id, f"当前积分 <b>{pts}</b>，未达到兑换门槛（需 ≥ <b>{REDEEM_MIN_POINTS}</b>）。")
            return True
        max_u = pts // REDEEM_RATE
        target_u = max_u
        if len(parts)>=2 and parts[1].isdigit():
            req_u = int(parts[1])
            if req_u > max_u:
                send_message_html(chat_id, f"可兑上限 {max_u} U，你当前积分不足以兑换 {req_u} U。"); return True
            target_u = req_u
        deduct_pts = target_u * REDEEM_RATE
        _add_points(chat_id, uid, -deduct_pts, uid, f"redeem_to_U:{target_u}")
        send_message_html(chat_id, f"🎁 兑换成功：{target_u} U（已扣 {deduct_pts} 分）。当前剩余积分：<b>{_get_points(chat_id,uid)}</b>。")
        return True

    return False

# ---- 回调（按钮） ----
def handle_callback(cb: Dict):
    cb_id = cb.get("id"); user = cb.get("from") or {}; uid = user.get("id")
    msg = cb.get("message") or {}; chat = msg.get("chat") or {}; chat_id = chat.get("id")
    data = cb.get("data") or ""
    try:
        if not chat_id or not uid or not data: answer_callback_query(cb_id); return
        _upsert_user_base(chat_id, {"id":uid,"username":user.get("username"),"first_name":user.get("first_name"),"last_name":user.get("last_name"),"is_bot":user.get("is_bot")})
        admin = is_chat_admin(chat_id, uid)

        if data == "ACT_CHECKIN":
            today = tz_now().strftime("%Y-%m-%d")
            if _get_last_checkin(chat_id, uid) == today: answer_callback_query(cb_id, "今天已签到"); return
            _add_points(chat_id, uid, SCORE_CHECKIN_POINTS, uid, "daily_checkin")
            _set_last_checkin(chat_id, uid, today)
            answer_callback_query(cb_id, "签到成功"); return

        if data == "ACT_SCORE":
            answer_callback_query(cb_id, f"当前积分：{_get_points(chat_id, uid)}"); return

        if data == "ACT_TOP10":
            rows = _fetchall("SELECT user_id,username,first_name,last_name,points FROM scores WHERE chat_id=%s ORDER BY points DESC LIMIT %s",
                             (chat_id, SCORE_TOP_LIMIT))
            if not rows: send_message_html(chat_id,"暂无积分数据。")
            else:
                lines = ["🏆 <b>积分榜</b>", "<code>────────────────</code>"]
                for i,(uid2,u,f,l,p) in enumerate(rows,1):
                    u,f,l = ensure_user_display(chat_id, uid2, (u,f,l))
                    lines.append(f"{i}. {safe_html(human_name(u,f,l))} — <b>{p}</b> 分")
                send_message_html(chat_id,"\n".join(lines))
            answer_callback_query(cb_id); return

        if data in ("ACT_SD_TODAY","ACT_SD_YESTERDAY"):
            day = tz_now().strftime("%Y-%m-%d") if data.endswith("TODAY") else (tz_now()-timedelta(days=1)).strftime("%Y-%m-%d")
            send_message_html(chat_id, build_daily_report(chat_id, day)); answer_callback_query(cb_id); return

        if data == "ACT_SM_THIS":
            ym = tz_now().strftime("%Y-%m"); send_message_html(chat_id, build_monthly_report(chat_id, ym)); answer_callback_query(cb_id); return

        if data == "ACT_RULES":
            send_message_html(chat_id, build_rules_text(chat_id)); answer_callback_query(cb_id); return

        if data == "ACT_REDEEM":
            pts = _get_points(chat_id, uid)
            if pts < REDEEM_MIN_POINTS:
                answer_callback_query(cb_id, f"未达兑换门槛（≥{REDEEM_MIN_POINTS}）", show_alert=True); return
            max_u = pts // REDEEM_RATE
            if max_u <= 0: answer_callback_query(cb_id, "积分不足", show_alert=True); return
            deduct_pts = max_u * REDEEM_RATE
            _add_points(chat_id, uid, -deduct_pts, uid, f"redeem_to_U:{max_u}")
            send_message_html(chat_id, f"🎁 兑换成功：{max_u} U（已扣 {deduct_pts} 分）。当前剩余积分：<b>{_get_points(chat_id,uid)}</b>。")
            answer_callback_query(cb_id, "兑换完成"); return

        # 广告（按钮化）
        if data == "ACT_AD_SHOW":
            if not admin: answer_callback_query(cb_id,"无权限",show_alert=True); return
            en, ct, mode, times = ad_get(chat_id); st = "启用" if en else "禁用"
            send_message_html(chat_id, f"📣 当前：<b>{st}</b>  · 模式：<b>{mode}</b>\n🕒 时间点：{_norm_times_str(times) or '（未设置）'}\n内容：\n{safe_html(ct) if ct else '（空）'}"); answer_callback_query(cb_id); return
        if data == "ACT_AD_ENABLE":
            if not admin: answer_callback_query(cb_id,"无权限",show_alert=True); return
            ad_enable(chat_id, True); answer_callback_query(cb_id,"已启用"); return
        if data == "ACT_AD_DISABLE":
            if not admin: answer_callback_query(cb_id,"无权限",show_alert=True); return
            ad_enable(chat_id, False); answer_callback_query(cb_id,"已禁用"); return
        if data == "ACT_AD_CLEAR":
            if not admin: answer_callback_query(cb_id,"无权限",show_alert=True); return
            ad_clear(chat_id); answer_callback_query(cb_id,"已清空"); return
        if data == "ACT_AD_SET":
            if not admin: answer_callback_query(cb_id,"无权限",show_alert=True); return
            key = f"pending:adset:{chat_id}:{uid}"; state_set(key,"1")
            send_message_html(chat_id,"请在本条消息下<b>回复一条文本</b>作为新的广告内容。"); answer_callback_query(cb_id,"请回复广告文本"); return
        if data == "ACT_AD_MODE_ATTACH":
            if not admin: answer_callback_query(cb_id,"无权限",show_alert=True); return
            ad_set_mode(chat_id, "attach"); answer_callback_query(cb_id,"已设为附加模式"); return
        if data == "ACT_AD_MODE_SCHEDULE":
            if not admin: answer_callback_query(cb_id,"无权限",show_alert=True); return
            ad_set_mode(chat_id, "schedule"); answer_callback_query(cb_id,"已设为定时模式"); return
        if data == "ACT_AD_SET_TIMES":
            if not admin: answer_callback_query(cb_id,"无权限",show_alert=True); return
            key = f"pending:ad_times:{chat_id}:{uid}"; state_set(key,"1")
            send_message_html(chat_id,"请在本条消息下<b>回复</b>时间点，格式如：<code>09:00,12:30,20:00</code>"); answer_callback_query(cb_id); return
        if data == "ACT_AD_SEND_NOW":
            if not admin: answer_callback_query(cb_id,"无权限",show_alert=True); return
            ad_send_now(chat_id); answer_callback_query(cb_id,"已发送"); return

        # 曝光台
        if data == "ACT_EXP_SHOW":
            send_exposures(chat_id); answer_callback_query(cb_id); return
        if data == "ACT_EXP_ADD":
            if not admin: answer_callback_query(cb_id,"无权限",show_alert=True); return
            key = f"pending:exposeadd:{chat_id}:{uid}"; state_set(key,"1")
            send_message_html(chat_id,"请在本条消息下<b>回复</b>：文本（首行做标题）+ 可选图片/视频（说明写在媒体说明）。")
            answer_callback_query(cb_id,"等待你的曝光内容"); return
        if data == "ACT_EXP_CLEAR":
            if not admin: answer_callback_query(cb_id,"无权限",show_alert=True); return
            expose_clear(chat_id); answer_callback_query(cb_id,"已清空曝光"); return
        if data == "ACT_EXP_TOGGLE":
            if not admin: answer_callback_query(cb_id,"无权限",show_alert=True); return
            expose_set_enabled(chat_id, not expose_enabled(chat_id))
            send_menu_for(chat_id, uid); answer_callback_query(cb_id,"已切换曝光开关"); return

        # 自定义新闻
        if data == "ACT_CNEWS_PANEL":
            if not admin: answer_callback_query(cb_id,"无权限",show_alert=True); return
            cnews_panel(chat_id, uid); answer_callback_query(cb_id); return
        if data == "ACT_CNEWS_NEW":
            if not admin: answer_callback_query(cb_id,"无权限",show_alert=True); return
            state_set(f"pending:cnewsnew:{chat_id}:{uid}","1")
            send_message_html(chat_id,"请在本条消息下<b>回复文本</b>（首行标题），可附图/视频。")
            answer_callback_query(cb_id,"等待你的新闻内容"); return
        if data == "ACT_CNEWS_LIST_D":
            if not admin: answer_callback_query(cb_id,"无权限",show_alert=True); return
            cnews_list_message(chat_id, "draft"); answer_callback_query(cb_id); return
        if data == "ACT_CNEWS_LIST_P":
            if not admin: answer_callback_query(cb_id,"无权限",show_alert=True); return
            cnews_list_message(chat_id, "published"); answer_callback_query(cb_id); return
        if data.startswith("ACT_CNEWS_PRE:"):
            if not admin: answer_callback_query(cb_id,"无权限",show_alert=True); return
            nid = int(data.split(":")[1]); cnews_publish(chat_id, nid, preview=True); answer_callback_query(cb_id); return
        if data.startswith("ACT_CNEWS_PUB:"):
            if not admin: answer_callback_query(cb_id,"无权限",show_alert=True); return
            nid = int(data.split(":")[1]); cnews_publish(chat_id, nid, preview=False); answer_callback_query(cb_id,"已发布"); return
        if data.startswith("ACT_CNEWS_DEL:"):
            if not admin: answer_callback_query(cb_id,"无权限",show_alert=True); return
            nid = int(data.split(":")[1]); cnews_delete(chat_id, nid); send_message_html(chat_id,"✅ 已删除。"); answer_callback_query(cb_id); return
        if data.startswith("ACT_CNEWS_EDIT:"):
            if not admin: answer_callback_query(cb_id,"无权限",show_alert=True); return
            nid = int(data.split(":")[1])
            state_set(f"pending:cnewsedit:{chat_id}:{uid}:{nid}","1")
            state_set(f"pending:cnewsedit:last:{chat_id}:{uid}", str(nid))
            send_message_html(chat_id, f"请回复新的内容用于覆盖草稿 #{nid}（首行标题）+ 可选图/视频。")
            answer_callback_query(cb_id); return

        if data == "ACT_HELP":
            send_menu_for(chat_id, uid); answer_callback_query(cb_id,"已刷新菜单"); return

        if data == "ACT_NEWS_NOW":
            if not admin: answer_callback_query(cb_id,"无权限",show_alert=True); return
            push_news_once(chat_id); state_set("next_news_at",(tz_now()+timedelta(minutes=INTERVAL_MINUTES)).isoformat()); answer_callback_query(cb_id,"已推送新闻"); return

    except Exception:
        logger.exception("callback error")
        try: answer_callback_query(cb_id)
        except Exception: pass

# ========== 成员事件（消息型） ==========
def handle_new_members(msg: Dict):
    chat_id = (msg.get("chat") or {}).get("id")
    inviter = msg.get("from") or {}
    members = msg.get("new_chat_members") or []
    for m in members:
        _upsert_user_base(chat_id, m or {})
        # 管理员“拉人”场景：message.from 即邀请人
        if inviter and inviter.get("id") and inviter.get("id") != (m or {}).get("id"):
            _bind_invite_if_needed(chat_id, m, inviter)
    if WELCOME_PANEL_ENABLED and members:
        send_message_html(chat_id, build_rules_text(chat_id), reply_markup=build_menu(False, chat_id))

def handle_left_member(msg: Dict):
    chat_id = (msg.get("chat") or {}).get("id")
    left = msg.get("left_chat_member") or {}
    invitee_id = left.get("id")
    if not invitee_id: return
    row = _fetchone("SELECT inviter_id FROM invites WHERE chat_id=%s AND invitee_id=%s", (chat_id, invitee_id))
    if not row: return
    inviter_id = row[0]
    _add_points(chat_id, inviter_id, -INVITE_REWARD_POINTS, inviter_id, "invite_auto_leave")
    _exec("DELETE FROM invites WHERE chat_id=%s AND invitee_id=%s", (chat_id, invitee_id))

# ========== 轮询 ==========
def process_updates_once():
    offset_key = "last_update_id"
    last_update_id = int(state_get(offset_key) or 0)
    resp = http_get("getUpdates", params={
        "offset": last_update_id + 1 if last_update_id else None,
        "timeout": POLL_TIMEOUT,
        "allowed_updates": json.dumps(["message","callback_query","chat_member"])
    }, timeout=max(POLL_TIMEOUT+10, HTTP_TIMEOUT))
    if not resp or not resp.get("ok"): time.sleep(1); return

    for u in resp.get("result", []):
        last_update_id = max(last_update_id, int(u.get("update_id", 0)))
        state_set(offset_key, str(last_update_id))

        if u.get("callback_query"):
            handle_callback(u["callback_query"]); continue

        if u.get("chat_member"):
            handle_chat_member_update(u["chat_member"]); continue

        msg = u.get("message") or {}
        chat = msg.get("chat") or {}
        chat_id = chat.get("id")
        if not chat_id: continue

        if msg.get("new_chat_members"):
            handle_new_members(msg); continue

        if msg.get("left_chat_member"):
            handle_left_member(msg); continue

        frm = msg.get("from") or {}
        if not frm or frm.get("is_bot"): continue

        text = (msg.get("text") or "").strip() if isinstance(msg.get("text"), str) else None

        # —— 中文触发词：导航/菜单/帮助 ——
        if text and re.fullmatch(r"\s*(导航|菜单|帮助)\s*", text):
            send_menu_for(chat_id, frm.get("id"))
            continue

        # 广告待输入：内容
        key_ad = f"pending:adset:{chat_id}:{frm.get('id')}"
        if state_get(key_ad):
            if text and not text.startswith("/"):
                ad_set(chat_id, text); state_del(key_ad); send_message_html(chat_id,"✅ 广告内容已更新。")
                continue

        # 广告待输入：时间点
        key_times = f"pending:ad_times:{chat_id}:{frm.get('id')}"
        if state_get(key_times):
            if text and not text.startswith("/"):
                t = ad_set_times(chat_id, text)
                state_del(key_times)
                send_message_html(chat_id, f"✅ 时间点已设置：{t or '（空）'}")
                continue

        # 曝光待输入
        key_ex = f"pending:exposeadd:{chat_id}:{frm.get('id')}"
        if state_get(key_ex):
            title = None; content = None; mtype = "none"; fid = None
            if msg.get("caption"): content = msg.get("caption")
            if text and not content: content = text
            if content:
                parts = content.splitlines()
                title = parts[0][:200] if parts else "曝光"
            if msg.get("photo"):
                biggest = max(msg["photo"], key=lambda p: p.get("file_size",0))
                fid = biggest.get("file_id"); mtype = "photo"
            elif msg.get("video"):
                fid = msg["video"].get("file_id"); mtype = "video"
            expose_add(chat_id, title or "曝光", content or "", mtype, fid)
            state_del(key_ex); send_message_html(chat_id,"✅ 曝光已登记。"); continue

        # 自定义新闻：新建/编辑
        key_new = f"pending:cnewsnew:{chat_id}:{frm.get('id')}"
        if state_get(key_new):
            if frm.get("id") not in list_chat_admin_ids(chat_id) and frm.get("id") not in ADMIN_USER_IDS:
                state_del(key_new)
            else:
                content = msg.get("caption") or text or ""
                parts = (content or "").splitlines()
                title = (parts[0] if parts else "无标题").strip()
                body = "\n".join(parts[1:]).strip()
                mtype, fid = "none", None
                if msg.get("photo"):
                    biggest = max(msg["photo"], key=lambda p: p.get("file_size",0))
                    fid = biggest.get("file_id"); mtype = "photo"
                elif msg.get("video"):
                    fid = msg["video"].get("file_id"); mtype = "video"
                nid = cnews_create(chat_id, frm.get("id"), title, body, mtype, fid)
                state_del(key_new)
                send_message_html(chat_id, f"✅ 草稿已创建：#{nid} — {safe_html(title)}")
            continue

        edit_last_key = f"pending:cnewsedit:last:{chat_id}:{frm.get('id')}"
        last_nid = state_get(edit_last_key)
        if last_nid:
            edit_key = f"pending:cnewsedit:{chat_id}:{frm.get('id')}:{last_nid}"
            if state_get(edit_key):
                content = msg.get("caption") or text or ""
                parts = (content or "").splitlines()
                title = (parts[0] if parts else "无标题").strip()
                body = "\n".join(parts[1:]).strip()
                mtype, fid = "none", None
                if msg.get("photo"):
                    biggest = max(msg["photo"], key=lambda p: p.get("file_size",0))
                    fid = biggest.get("file_id"); mtype = "photo"
                elif msg.get("video"):
                    fid = msg["video"].get("file_id"); mtype = "video"
                cnews_update(chat_id, int(last_nid), title, body, mtype, fid)
                state_del(edit_key); state_del(edit_last_key)
                send_message_html(chat_id, f"✅ 草稿已更新：#{last_nid} — {safe_html(title)}")
                continue

        # 命令
        if text and text.startswith("/"):
            try:
                if handle_general_command(msg): continue
            except Exception:
                logger.exception("command error"); continue

        # 普通发言计数
        if STATS_ENABLED and text and len(text.strip()) >= MIN_MSG_CHARS:
            day = tz_now().strftime("%Y-%m-%d")
            inc_msg_count(chat_id, frm, day, inc=1)

# ========== 调度 ==========
def gather_known_chats() -> List[int]:
    chats = set(NEWS_CHAT_IDS or [])
    for r in _fetchall("SELECT DISTINCT chat_id FROM msg_counts", ()): chats.add(int(r[0]))
    for r in _fetchall("SELECT DISTINCT chat_id FROM scores", ()): chats.add(int(r[0]))
    for r in _fetchall("SELECT chat_id FROM ads", ()): chats.add(int(r[0]))
    return sorted(chats)

def maybe_push_news():
    key = "next_news_at"; nv = state_get(key); now = tz_now()
    if nv:
        try: next_at = datetime.fromisoformat(nv)
        except Exception: next_at = now - timedelta(minutes=1)
        if next_at.tzinfo is None: next_at = next_at.replace(tzinfo=LOCAL_TZ)
    else:
        next_at = now - timedelta(minutes=1)
    if now >= next_at:
        chats = NEWS_CHAT_IDS or gather_known_chats()
        for cid in chats:
            try: push_news_once(cid)
            except Exception: logger.exception("news push error")
        state_set(key, (now+timedelta(minutes=INTERVAL_MINUTES)).isoformat())

def maybe_daily_report():
    h,m = parse_hhmm(STATS_DAILY_AT); now = tz_now()
    if now.hour!=h or now.minute!=m: return
    chats = STATS_CHAT_IDS or gather_known_chats()
    yday = (now - timedelta(days=1)).strftime("%Y-%m-%d")
    for cid in chats:
        rk = f"daily_done:{cid}:{yday}"
        if state_get(rk): continue
        try:
            send_message_html(cid, build_daily_report(cid, yday))
            # 日度发言 TOP 奖励
            rows = list_top_day(cid, yday, limit=TOP_REWARD_SIZE)
            if rows:
                bonus = DAILY_TOP_REWARD_START
                for (uid, un, fn, ln, c) in rows:
                    _upsert_user_base(cid, {"id": uid, "username": un, "first_name": fn, "last_name": ln})
                    _add_points(cid, uid, max(bonus,0), uid, "top_day_reward")
                    bonus -= 1
        except Exception:
            logger.exception("daily report error")
        state_set(rk, "1")

def maybe_monthly_report():
    h,m = parse_hhmm(STATS_MONTHLY_AT); now = tz_now()
    if not (now.day==1 and now.hour==h and now.minute==m): return
    last_month = (now.replace(day=1)-timedelta(days=1)).strftime("%Y-%m")
    chats = STATS_CHAT_IDS or gather_known_chats()
    for cid in chats:
        rk = f"monthly_done:{cid}:{last_month}"
        if state_get(rk): continue
        try:
            send_message_html(cid, build_monthly_report(cid, last_month))
            # 月度奖励
            rows = list_top_month(cid, last_month, limit=10)
            if rows:
                for idx,(uid,un,fn,ln,c) in enumerate(rows,1):
                    reward = MONTHLY_REWARD_RULE[idx-1] if idx-1 < len(MONTHLY_REWARD_RULE) else 0
                    if reward>0:
                        _upsert_user_base(cid, {"id": uid, "username": un, "first_name": fn, "last_name": ln})
                        _add_points(cid, uid, reward, uid, "top_month_reward")
        except Exception:
            logger.exception("monthly report error")
        state_set(rk, "1")

def maybe_ad_schedule():
    """定时广告：到点发送（当天同一时间点仅发一次）"""
    now = tz_now()
    hhmm = now.strftime("%H:%M")
    today = now.strftime("%Y-%m-%d")
    rows = _fetchall("SELECT chat_id, enabled, COALESCE(mode,'attach'), COALESCE(times,''), COALESCE(content,'') FROM ads", ())
    for (cid, en, mode, times, content) in rows:
        if not en or mode != "schedule": continue
        if not content.strip(): continue
        tset = set((_norm_times_str(times) or "").split(",")) - {""}
        if hhmm not in tset: continue
        sent_key = f"ad_sent:{cid}:{today}:{hhmm}"
        if state_get(sent_key): continue
        try:
            send_message_html(cid, "📣 <b>广告</b>\n" + safe_html(content))
            state_set(sent_key, "1")
        except Exception:
            logger.exception("ad schedule send error", extra={"chat_id": cid})

def scheduler_step():
    maybe_push_news()
    maybe_daily_report()
    maybe_monthly_report()
    maybe_ad_schedule()

# ========== 启动 ==========
if __name__ == "__main__":
    print(f"[boot] starting bot... run={RUN_ID}")
    print(f"[boot] TZ={LOCAL_TZ_NAME}, MYSQL={MYSQL_USER}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}, zh={int(TRANSLATE_TO_ZH)}")
    try:
        get_conn(); init_db()
        log(logging.INFO, "boot ok", event="boot",
            cmd=f"{LOCAL_TZ_NAME} poll={POLL_TIMEOUT}s http={HTTP_TIMEOUT}s news_interval={INTERVAL_MINUTES}m")
    except Exception:
        logger.exception("boot error"); sys.exit(1)

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
            logger.exception("updates loop error"); time.sleep(2)
