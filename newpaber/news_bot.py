# -*- coding: utf-8 -*-
"""
Telegram 群机器人 - 新闻 / 统计 / 积分 / 广告 / 曝光台 / 自定义新闻 / 招商按钮 / 积分管理
"""

import os, re, sys, json, html, time, uuid, logging, requests, feedparser, pymysql
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from dateutil import tz
from dotenv import load_dotenv
from typing import List, Tuple, Optional, Dict

# ====================== ENV ======================
load_dotenv()
BOT_TOKEN = (os.getenv("BOT_TOKEN") or os.getenv("TELEGRAM_BOT_TOKEN") or "").strip()
if not BOT_TOKEN:
    raise SystemExit("配置 BOT_TOKEN 到 .env")
API_BASE = f"https://api.telegram.org/bot{BOT_TOKEN}"

LOCAL_TZ_NAME = os.getenv("LOCAL_TZ", "Asia/Shanghai")
LOCAL_TZ = tz.gettz(LOCAL_TZ_NAME)

MYSQL_HOST = os.getenv("MYSQL_HOST", "127.0.0.1")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3306"))
MYSQL_DB   = os.getenv("MYSQL_DB", "newsbot")
MYSQL_USER = os.getenv("MYSQL_USER", "root")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "")

POLL_TIMEOUT = int(os.getenv("POLL_TIMEOUT", "50"))
HTTP_TIMEOUT = int(os.getenv("HTTP_TIMEOUT", "65"))

INTERVAL_MINUTES = int(os.getenv("INTERVAL_MINUTES", "60"))
NEWS_ITEMS_PER_CAT = int(os.getenv("NEWS_ITEMS_PER_CAT", "8"))
NEWS_ENABLED_DEFAULT = os.getenv("NEWS_ENABLED_DEFAULT", "1") == "1"
NEWS_MEDIA = os.getenv("NEWS_MEDIA","0")=="1"
NEWS_MEDIA_LIMIT = int(os.getenv("NEWS_MEDIA_LIMIT","4"))
OG_FETCH_TIMEOUT = int(os.getenv("OG_FETCH_TIMEOUT","8"))

STATS_ENABLED = os.getenv("STATS_ENABLED","1")=="1"
MIN_MSG_CHARS = int(os.getenv("MIN_MSG_CHARS","3"))

WELCOME_PANEL_ENABLED = os.getenv("WELCOME_PANEL_ENABLED","1")=="1"
WELCOME_EPHEMERAL_SECONDS = int(os.getenv("WELCOME_EPHEMERAL_SECONDS","60"))
PANEL_EPHEMERAL_SECONDS = int(os.getenv("PANEL_EPHEMERAL_SECONDS","60"))
POPUP_EPHEMERAL_SECONDS = int(os.getenv("POPUP_EPHEMERAL_SECONDS","60"))

ADMIN_USER_IDS = {int(x) for x in re.split(r"[,\s]+", os.getenv("ADMIN_USER_IDS","").strip()) if x.isdigit()}

SCORE_CHECKIN_POINTS = int(os.getenv("SCORE_CHECKIN_POINTS","1"))
TOP_REWARD_SIZE = int(os.getenv("TOP_REWARD_SIZE","10"))
DAILY_TOP_REWARD_START = int(os.getenv("DAILY_TOP_REWARD_START","9"))
MONTHLY_REWARD_RULE = [int(x) for x in json.loads(os.getenv("MONTHLY_REWARD_RULE","[6000,4000,2000,1000,600,600,600,600,600,600]"))][:10]

REDEEM_RATE = int(os.getenv("REDEEM_RATE","100"))
REDEEM_MIN_POINTS = int(os.getenv("REDEEM_MIN_POINTS","10000"))
INVITE_REWARD_POINTS = int(os.getenv("INVITE_REWARD_POINTS","10"))

STATS_DAILY_AT = os.getenv("STATS_DAILY_AT","23:50")
STATS_MONTHLY_AT = os.getenv("STATS_MONTHLY_AT","00:10")
DAILY_BROADCAST_AT = os.getenv("DAILY_BROADCAST_AT","23:59")

NEWS_CHAT_IDS = [int(x) for x in re.split(r"[,\s]+", os.getenv("NEWS_CHAT_IDS","").strip()) if x.isdigit()]
STATS_CHAT_IDS = [int(x) for x in re.split(r"[,\s]+", os.getenv("STATS_CHAT_IDS","").strip()) if x.isdigit()]

AD_DEFAULT_ENABLED = os.getenv("AD_DEFAULT_ENABLED","1")=="1"

BIZ_LINKS = os.getenv("BIZ_LINKS","").strip()
BIZ_A_LABEL = os.getenv("BIZ_A_LABEL","招商A"); BIZ_A_URL = os.getenv("BIZ_A_URL","").strip()
BIZ_B_LABEL = os.getenv("BIZ_B_LABEL","招商B"); BIZ_B_URL = os.getenv("BIZ_B_URL","").strip()

LOG_LEVEL = os.getenv("LOG_LEVEL","INFO").upper()
RUN_ID = os.getenv("RUN_ID") or uuid.uuid4().hex[:8]

TRANSLATE_TO_ZH = os.getenv("TRANSLATE_TO_ZH","1")=="1"
try:
    from deep_translator import GoogleTranslator
    _gt = GoogleTranslator(source="auto", target="zh-CN")
except Exception:
    _gt = None
    TRANSLATE_TO_ZH = False

# ====================== 日志 ======================
logger = logging.getLogger("newsbot"); logger.setLevel(getattr(logging, LOG_LEVEL, logging.INFO))
h = logging.StreamHandler(sys.stdout)
h.setFormatter(logging.Formatter("%(asctime)s %(levelname)s [%(name)s] %(message)s","%Y-%m-%d %H:%M:%S"))
logger.handlers.clear(); logger.addHandler(h)
def log(level, msg, **ctx): logger.log(level, f"{msg} | {json.dumps(ctx,ensure_ascii=False)}" if ctx else msg)

# ====================== 工具 & Telegram ======================
def tz_now() -> datetime: return datetime.now(tz=LOCAL_TZ)
def utcnow() -> datetime: return datetime.utcnow().replace(tzinfo=tz.UTC)
def parse_hhmm(s:str)->Tuple[int,int]:
    m=re.match(r"^\s*(\d{1,2}):(\d{2})\s*$",s or ""); 
    if not m: return (0,0)
    return max(0,min(23,int(m.group(1)))), max(0,min(59,int(m.group(2))))
def safe_html(s:str)->str: return html.escape(s or "",quote=False)

def http_get(method:str, params=None, json_data=None, files=None, timeout:Optional[int]=None):
    url=f"{API_BASE}/{method}"; t=timeout or HTTP_TIMEOUT
    try:
        if json_data is not None:
            r=requests.post(url,json=json_data,timeout=t)
        elif files is not None:
            r=requests.post(url,data=params or {},files=files,timeout=t)
        else:
            r=requests.get(url,params=params or {},timeout=t)
        r.raise_for_status(); data=r.json()
        if not data.get("ok"): log(logging.WARNING,"telegram api not ok",event="tg_api",cmd=method,err=str(data))
        return data
    except Exception as e:
        log(logging.ERROR,"telegram api error",event="tg_api",cmd=method,error=str(e)); return None

def send_message_html(chat_id:int, text:str, reply_to_message_id:Optional[int]=None, disable_preview:bool=True, reply_markup:Optional[dict]=None):
    payload={"chat_id":chat_id,"text":text,"parse_mode":"HTML","disable_web_page_preview":True if disable_preview else False}
    if reply_to_message_id: payload["reply_to_message_id"]=reply_to_message_id
    if reply_markup: payload["reply_markup"]=json.dumps(reply_markup,ensure_ascii=False)
    use_post = bool(reply_markup) or len(text)>3500
    try:
        if use_post:
            r=requests.post(f"{API_BASE}/sendMessage",data=payload,timeout=HTTP_TIMEOUT); return r.json()
        else:
            return http_get("sendMessage", params=payload)
    except Exception as e:
        log(logging.ERROR,"telegram api error",event="tg_api",cmd="sendMessage",error=str(e)); return None

def edit_message_html(chat_id:int, message_id:int, text:str, disable_preview:bool=True, reply_markup:Optional[dict]=None):
    url=f"{API_BASE}/editMessageText"
    payload={"chat_id":chat_id,"message_id":message_id,"text":text,"parse_mode":"HTML","disable_web_page_preview":True if disable_preview else False}
    if reply_markup: payload["reply_markup"]=json.dumps(reply_markup,ensure_ascii=False)
    try:
        r=requests.post(url,data=payload,timeout=HTTP_TIMEOUT)
        try: data=r.json()
        except Exception: data={"ok":False,"description":r.text,"status_code":r.status_code}
        if not data.get("ok"):
            desc=(data.get("description") or "").lower()
            if "message is not modified" in desc:
                log(logging.INFO,"edit ignored: not modified",event="tg_api"); return {"ok":True,"result":None}
            log(logging.WARNING,"editMessageText failed",event="tg_api",desc=data.get("description",""))
        return data
    except Exception as e:
        log(logging.ERROR,"telegram api error",event="tg_api",cmd="editMessageText",error=str(e)); return None

def delete_message(chat_id:int, message_id:int): return http_get("deleteMessage", params={"chat_id":chat_id,"message_id":message_id})
def answer_callback_query(cb_id:str, text:str="", show_alert:bool=False):
    if not cb_id: return None
    try:
        payload={"callback_query_id":cb_id}; 
        if text: payload["text"]=text
        if show_alert: payload["show_alert"]=True
        r=requests.post(f"{API_BASE}/answerCallbackQuery",data=payload,timeout=min(5,HTTP_TIMEOUT))
        try: data=r.json()
        except Exception: data={"ok":False,"description":r.text}
        if not data.get("ok"):
            desc=(data.get("description") or "").lower()
            if "query is too old" in desc or "query id is invalid" in desc: 
                log(logging.INFO,"callback too old/invalid, ignored"); return None
            log(logging.WARNING,"answerCallbackQuery failed",event="tg_api",desc=data.get("description",""))
        return data
    except Exception as e:
        log(logging.WARNING,"answerCallbackQuery error",event="tg_api",error=str(e)); return None

# ====================== MySQL ======================
_DB=None
def _connect_mysql(dbname:Optional[str]=None):
    return pymysql.connect(host=MYSQL_HOST,port=MYSQL_PORT,user=MYSQL_USER,password=MYSQL_PASSWORD,
                           database=dbname,charset="utf8mb4",autocommit=True,cursorclass=pymysql.cursors.Cursor)
def get_conn():
    global _DB
    if _DB is None:
        try: _DB=_connect_mysql(MYSQL_DB)
        except pymysql.err.OperationalError as e:
            if e.args and e.args[0]==1049:
                tmp=_connect_mysql("mysql")
                with tmp.cursor() as c:
                    c.execute(f"CREATE DATABASE IF NOT EXISTS `{MYSQL_DB}` DEFAULT CHARSET utf8mb4 COLLATE utf8mb4_unicode_ci;")
                tmp.close(); _DB=_connect_mysql(MYSQL_DB)
            else:
                log(logging.ERROR,"mysql connect error",error=str(e)); raise
    else:
        _DB.ping(reconnect=True)
    return _DB
def _exec(sql:str,args:tuple=()): 
    with get_conn().cursor() as c: c.execute(sql,args); return c
def _fetchone(sql:str,args:tuple=()): 
    with _exec(sql,args) as c: return c.fetchone()
def _fetchall(sql:str,args:tuple=()): 
    with _exec(sql,args) as c: return c.fetchall()
def _safe_alter(sql:str):
    try: _exec(sql)
    except Exception: pass

def init_db():
    _exec("""CREATE TABLE IF NOT EXISTS msg_counts (
        chat_id BIGINT NOT NULL, user_id BIGINT NOT NULL,
        username VARCHAR(64), first_name VARCHAR(64), last_name VARCHAR(64),
        day CHAR(10) NOT NULL, cnt INT NOT NULL DEFAULT 0,
        PRIMARY KEY (chat_id,user_id,day),
        KEY idx_day (chat_id,day), KEY idx_user (chat_id,user_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;""")
    _exec("""CREATE TABLE IF NOT EXISTS scores (
        chat_id BIGINT NOT NULL, user_id BIGINT NOT NULL,
        username VARCHAR(64), first_name VARCHAR(64), last_name VARCHAR(64),
        points INT NOT NULL DEFAULT 0, last_checkin CHAR(10),
        is_bot TINYINT NOT NULL DEFAULT 0,
        PRIMARY KEY (chat_id,user_id), KEY idx_points (chat_id,points)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;""")
    _exec("""CREATE TABLE IF NOT EXISTS score_logs (
        id BIGINT PRIMARY KEY AUTO_INCREMENT,
        chat_id BIGINT, actor_id BIGINT, target_id BIGINT,
        delta INT, reason VARCHAR(64), ts VARCHAR(40)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;""")
    _exec("""CREATE TABLE IF NOT EXISTS invites (
        chat_id BIGINT, invitee_id BIGINT, inviter_id BIGINT, ts VARCHAR(40),
        PRIMARY KEY (chat_id, invitee_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;""")
    _exec("""CREATE TABLE IF NOT EXISTS award_runs (
        chat_id BIGINT, period_type VARCHAR(10), period_value VARCHAR(10), ts VARCHAR(40),
        PRIMARY KEY (chat_id, period_type, period_value)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;""")
    _exec("""CREATE TABLE IF NOT EXISTS ads (
        chat_id BIGINT PRIMARY KEY,
        enabled TINYINT NOT NULL DEFAULT 1,
        content TEXT,
        mode ENUM('attach','schedule','disabled') DEFAULT 'attach',
        times VARCHAR(200) DEFAULT NULL,
        media_type ENUM('none','photo','video') DEFAULT 'none',
        file_id VARCHAR(256),
        updated_at VARCHAR(40)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;""")
    _safe_alter("ALTER TABLE ads ADD COLUMN media_type ENUM('none','photo','video') DEFAULT 'none'")
    _safe_alter("ALTER TABLE ads ADD COLUMN file_id VARCHAR(256) NULL")
    _safe_alter("ALTER TABLE ads ADD COLUMN mode ENUM('attach','schedule','disabled') DEFAULT 'attach'")
    _safe_alter("ALTER TABLE ads ADD COLUMN times VARCHAR(200) DEFAULT NULL")
    _exec("""CREATE TABLE IF NOT EXISTS state (`key` VARCHAR(100) PRIMARY KEY, `val` TEXT)
             ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;""")
    _exec("""CREATE TABLE IF NOT EXISTS posted_news (
        chat_id BIGINT, category VARCHAR(16), link TEXT, ts VARCHAR(40),
        PRIMARY KEY (chat_id, category(8), link(255))
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;""")
    _exec("""CREATE TABLE IF NOT EXISTS exposures (
        id BIGINT PRIMARY KEY AUTO_INCREMENT,
        chat_id BIGINT NOT NULL,
        title VARCHAR(200), content TEXT,
        media_type ENUM('none','photo','video') DEFAULT 'none',
        file_id VARCHAR(256),
        enabled TINYINT NOT NULL DEFAULT 1,
        created_at VARCHAR(40), updated_at VARCHAR(40),
        KEY idx_chat (chat_id, enabled)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;""")
    _exec("""CREATE TABLE IF NOT EXISTS expose_settings (
        chat_id BIGINT PRIMARY KEY,
        enabled TINYINT NOT NULL DEFAULT 0,
        updated_at VARCHAR(40)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;""")
    _exec("""CREATE TABLE IF NOT EXISTS custom_news (
        id BIGINT PRIMARY KEY AUTO_INCREMENT,
        chat_id BIGINT NOT NULL,
        title VARCHAR(200), content TEXT,
        media_type ENUM('none','photo','video') DEFAULT 'none',
        file_id VARCHAR(256),
        status ENUM('draft','published') DEFAULT 'draft',
        created_by BIGINT,
        created_at VARCHAR(40), updated_at VARCHAR(40),
        KEY idx_chat (chat_id, status, id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;""")
    _exec("""CREATE TABLE IF NOT EXISTS redemptions (
        id BIGINT PRIMARY KEY AUTO_INCREMENT,
        chat_id BIGINT NOT NULL,
        user_id BIGINT NOT NULL,
        username VARCHAR(64), first_name VARCHAR(64), last_name VARCHAR(64),
        points_snapshot INT NOT NULL,
        u_amount INT NOT NULL,
        trc20_addr VARCHAR(128),
        status ENUM('pending','approved','rejected') DEFAULT 'pending',
        decided_by BIGINT NULL,
        created_at VARCHAR(40), decided_at VARCHAR(40)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;""")
    _exec("""CREATE TABLE IF NOT EXISTS ephemeral_msgs (
        chat_id BIGINT NOT NULL,
        message_id BIGINT NOT NULL,
        expire_at VARCHAR(40) NOT NULL,
        PRIMARY KEY(chat_id, message_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;""")

# ====================== 状态/工具 ======================
def state_get(key:str)->Optional[str]:
    row=_fetchone("SELECT val FROM state WHERE `key`=%s",(key,)); return row[0] if row else None
def state_set(key:str, val:str): _exec("INSERT INTO state(`key`,`val`) VALUES(%s,%s) ON DUPLICATE KEY UPDATE `val`=VALUES(`val`)",(key,val))
def state_del(key:str): _exec("DELETE FROM state WHERE `key`=%s",(key,))

def clear_pending_states(chat_id:int, uid:int):
    for k in [
        f"pending:redeemaddr:{chat_id}:{uid}",
        f"pending:set_ad_text:{chat_id}:{uid}",
        f"pending:set_ad_times:{chat_id}:{uid}",
        f"pending:set_ad_media:{chat_id}:{uid}",
        f"adtimebuilder:{chat_id}:{uid}",
        f"pending:score:mode:{chat_id}:{uid}",
    ]: 
        try: state_del(k)
        except Exception: pass

def news_enabled(chat_id:int)->bool:
    v=state_get(f"news_enabled:{chat_id}"); return (v=="1") if v is not None else NEWS_ENABLED_DEFAULT
def news_set_enabled(chat_id:int, enabled:bool): state_set(f"news_enabled:{chat_id}","1" if enabled else "0")

def add_ephemeral(chat_id:int, message_id:int, seconds:int):
    expire_at=(utcnow()+timedelta(seconds=max(5,seconds))).isoformat()
    _exec("INSERT IGNORE INTO ephemeral_msgs(chat_id,message_id,expire_at) VALUES(%s,%s,%s)",(chat_id,message_id,expire_at))
def send_ephemeral_html(chat_id:int, text:str, seconds:int, reply_markup:Optional[dict]=None, disable_preview:bool=True):
    hint=f"\n\n<i>（无操作{seconds}秒后关闭）</i>" if seconds and seconds>0 else ""
    r=send_message_html(chat_id, text+hint, disable_preview=disable_preview, reply_markup=reply_markup)
    try:
        mid=int(((r or {}).get("result") or {}).get("message_id") or 0)
        if mid and seconds>0: add_ephemeral(chat_id, mid, seconds)
    except Exception: pass
def maybe_ephemeral_gc():
    now=utcnow().isoformat()
    rows=_fetchall("SELECT chat_id,message_id FROM ephemeral_msgs WHERE expire_at<=%s",(now,))
    for cid,mid in rows:
        try: delete_message(cid,mid)
        except Exception: pass
    _exec("DELETE FROM ephemeral_msgs WHERE expire_at<=%s",(now,))

# ====================== 统计/积分基础 ======================
def _upsert_user_base(chat_id:int, frm:Dict):
    _exec("INSERT INTO scores(chat_id,user_id,username,first_name,last_name,points,last_checkin,is_bot) "
          "VALUES (%s,%s,%s,%s,%s,%s,%s,%s) "
          "ON DUPLICATE KEY UPDATE username=VALUES(username), first_name=VALUES(first_name), last_name=VALUES(last_name), is_bot=VALUES(is_bot)",
          (chat_id, frm.get("id"), (frm.get("username") or "")[:64], (frm.get("first_name") or "")[:64], (frm.get("last_name") or "")[:64], 0, None, 1 if frm.get("is_bot") else 0))
def _add_points(chat_id:int, target_id:int, delta:int, actor_id:int, reason:str=""):
    _exec("INSERT INTO scores(chat_id,user_id,points) VALUES(%s,%s,%s) ON DUPLICATE KEY UPDATE points=points+VALUES(points)", (chat_id, target_id, delta))
    _exec("INSERT INTO score_logs(chat_id,actor_id,target_id,delta,reason,ts) VALUES(%s,%s,%s,%s,%s,%s)", (chat_id, actor_id, target_id, delta, reason or "", utcnow().isoformat()))
def _get_points(chat_id:int, user_id:int)->int:
    row=_fetchone("SELECT points FROM scores WHERE chat_id=%s AND user_id=%s",(chat_id,user_id)); return int(row[0]) if row else 0
def _get_last_checkin(chat_id:int, user_id:int)->str:
    row=_fetchone("SELECT last_checkin FROM scores WHERE chat_id=%s AND user_id=%s",(chat_id,user_id)); return row[0] or "" if row else ""
def _set_last_checkin(chat_id:int, user_id:int, day:str): _exec("UPDATE scores SET last_checkin=%s WHERE chat_id=%s AND user_id=%s",(day,chat_id,user_id))
def inc_msg_count(chat_id:int, frm:Dict, day:str, inc:int=1):
    _upsert_user_base(chat_id, frm)
    _exec("INSERT INTO msg_counts(chat_id,user_id,username,first_name,last_name,day,cnt) "
          "VALUES (%s,%s,%s,%s,%s,%s,%s) "
          "ON DUPLICATE KEY UPDATE cnt=cnt+VALUES(cnt), username=VALUES(username), first_name=VALUES(first_name), last_name=VALUES(last_name)",
          (chat_id, frm.get("id"), (frm.get("username") or "")[:64], (frm.get("first_name") or "")[:64], (frm.get("last_name") or "")[:64], day, inc))

def list_chat_admin_ids(chat_id:int)->set:
    key=f"admins:{chat_id}"; now=int(time.time()); cached=state_get(key)
    if cached:
        try:
            data=json.loads(cached)
            if now-int(data.get("ts",0))<600: return set(data.get("ids",[]))
        except Exception: pass
    ids=set(); r=http_get("getChatAdministrators", params={"chat_id":chat_id})
    if r and r.get("ok"):
        for m in r["result"]:
            u=m.get("user") or {}
            if "id" in u: ids.add(u["id"])
    state_set(key, json.dumps({"ids":list(ids),"ts":now})); return ids

def ensure_user_display(chat_id:int, uid:int, triplet:Tuple[str,str,str]):
    un,fn,ln=triplet
    if un or fn or ln: return un,fn,ln
    r=http_get("getChatMember", params={"chat_id":chat_id,"user_id":uid})
    user=((r or {}).get("result") or {}).get("user") or {}
    un2=user.get("username") or ""; fn2=user.get("first_name") or ""; ln2=user.get("last_name") or ""
    if un2 or fn2 or ln2:
        _exec("UPDATE scores SET username=%s, first_name=%s, last_name=%s WHERE chat_id=%s AND user_id=%s",(un2,fn2,ln2,chat_id,uid))
        return un2,fn2,ln2
    return un,fn,ln

def _user_link(uid:Optional[int], username:Optional[str])->str:
    username=(username or "").strip()
    if username: return f"https://t.me/{username}"
    return f"tg://user?id={uid}" if uid else "tg://user"
def rank_display_link(chat_id:int, uid:int, un:str, fn:str, ln:str)->str:
    un,fn,ln=ensure_user_display(chat_id, uid, (un,fn,ln))
    full=f"{(fn or '').strip()} {(ln or '').strip()}".strip()
    label=full or (f"@{un}" if un else f"ID:{uid}")
    href=_user_link(uid, un); return f'<a href="{href}">{safe_html(label)}</a>'

# ====================== OG 抓图（新闻图文） ======================
def fetch_og_image(article_url:str)->Optional[str]:
    try:
        r=requests.get(article_url,timeout=OG_FETCH_TIMEOUT,headers={"User-Agent":"Mozilla/5.0"})
        if r.status_code!=200 or "text/html" not in (r.headers.get("Content-Type","")): return None
        soup=BeautifulSoup(r.text or "","html.parser")
        for sel,attr in (('meta[property="og:image"]','content'),('meta[name="twitter:image"]','content')):
            tag=soup.select_one(sel)
            if tag and tag.get(attr): return tag.get(attr)
    except Exception: return None
    return None

# ====================== 广告（略，保持你现有功能） ======================
def ad_get(chat_id:int):
    row=_fetchone("SELECT enabled, content, COALESCE(mode,'attach'), COALESCE(times,''), COALESCE(media_type,'none'), COALESCE(file_id,'') FROM ads WHERE chat_id=%s",(chat_id,))
    if row:
        en,ct,mode,times,mt,fid = int(row[0])==1, row[1] or "", row[2] or "attach", row[3] or "", row[4] or "none", row[5] or ""
        return en,ct,mode,times,mt,fid
    _exec("INSERT IGNORE INTO ads(chat_id,enabled,content,mode,times,media_type,file_id,updated_at) VALUES(%s,%s,%s,%s,%s,%s,%s,%s)",
          (chat_id,1 if AD_DEFAULT_ENABLED else 0,"","attach","", "none","", utcnow().isoformat()))
    return AD_DEFAULT_ENABLED,"","attach","", "none",""
def ad_set(chat_id:int, content:str):
    _exec("INSERT INTO ads(chat_id,enabled,content,updated_at) VALUES(%s,%s,%s,%s) "
          "ON DUPLICATE KEY UPDATE content=VALUES(content), updated_at=VALUES(updated_at)",
          (chat_id,1 if AD_DEFAULT_ENABLED else 0,content,utcnow().isoformat()))
def ad_enable(chat_id:int, enabled:bool):
    _exec("INSERT INTO ads(chat_id,enabled,updated_at) VALUES(%s,%s,%s) "
          "ON DUPLICATE KEY UPDATE enabled=VALUES(enabled), updated_at=VALUES(updated_at)",
          (chat_id,1 if enabled else 0,utcnow().isoformat()))
def ad_clear(chat_id:int):
    _exec("UPDATE ads SET content=%s, media_type='none', file_id='', updated_at=%s WHERE chat_id=%s",("", utcnow().isoformat(), chat_id))
def ad_set_mode(chat_id:int, mode:str):
    if mode not in ("attach","schedule","disabled"): return
    _exec("UPDATE ads SET mode=%s, enabled=%s, updated_at=%s WHERE chat_id=%s",(mode, 0 if mode=="disabled" else 1, utcnow().isoformat(), chat_id))
def _norm_times_str(times:str)->str:
    lst=[]
    for p in re.split(r"[,\s]+", times or ""):
        if not p: continue
        m=re.match(r"^(\d{1,2}):(\d{2})$",p); 
        if not m: continue
        h,mi=int(m.group(1)),int(m.group(2))
        if 0<=h<=23 and 0<=mi<=59: lst.append(f"{h:02d}:{mi:02d}")
    return ",".join(sorted(set(lst)))
def ad_set_times(chat_id:int, times:str):
    t=_norm_times_str(times)
    _exec("UPDATE ads SET times=%s, updated_at=%s WHERE chat_id=%s",(t, utcnow().isoformat(), chat_id)); return t
def ad_set_media(chat_id:int, media_type:str, file_id:str, content:str):
    if media_type not in ("photo","video"): return
    _exec("UPDATE ads SET media_type=%s, file_id=%s, content=%s, updated_at=%s WHERE chat_id=%s",(media_type,file_id,content or "", utcnow().isoformat(), chat_id))
def ad_send_now(chat_id:int, preview_only:bool=False):
    en,ct,mode,times,mt,fid=ad_get(chat_id)
    if not ct.strip() and (mt=="none" or not fid):
        send_message_html(chat_id,"📣 广告内容为空，无法发送。"); return
    if not en and not preview_only:
        send_message_html(chat_id,"📣 广告当前处于禁用状态。"); return
    if mt=="photo" and fid:
        http_get("sendPhoto", params={"chat_id":chat_id,"photo":fid,"caption":f"<b>广告</b>\n{safe_html(ct)}","parse_mode":"HTML"})
    elif mt=="video" and fid:
        http_get("sendVideo", params={"chat_id":chat_id,"video":fid,"caption":f"<b>广告</b>\n{safe_html(ct)}","parse_mode":"HTML"})
    else:
        send_message_html(chat_id,"📣 <b>广告</b>\n"+safe_html(ct))

# ======= 广告定时时间选择器（省略：保留你已有实现） =======
# ...（保留你之前的时间选择器函数实现，不在此处重复粘贴，为节省篇幅）
# >>> 注意：如果你直接覆盖文件，请保留上次我发给你的时间选择器函数们（_adtime_* 一整套）

# ====================== 报表/规则文案 ======================
_KEYCAP=["","1️⃣","2️⃣","3️⃣","4️⃣","5️⃣","6️⃣","7️⃣","8️⃣","9️⃣","🔟"]
def _keycap(n:int)->str: return _KEYCAP[n] if 1<=n<=10 else str(n)

def build_rules_text(chat_id:int)->str:
    m=MONTHLY_REWARD_RULE + [0]*max(0,10-len(MONTHLY_REWARD_RULE))
    daily_start=max(1,min(20,DAILY_TOP_REWARD_START))
    daily=[max(1,daily_start-i) for i in range(10)]
    lines=[]
    lines.append("🗂 <b>群积分规则</b>")
    lines.append("")
    lines.append("🏆 <b>月度排名奖励</b>")
    lines.append(f"  {_keycap(1)} {m[0]} 分")
    lines.append(f"  {_keycap(2)} {m[1]} 分")
    lines.append(f"  {_keycap(3)} {m[2]} 分")
    lines.append(f"  {_keycap(4)} {m[3]} 分")
    lines.append(f"  {_keycap(5)}–{_keycap(10)} 各 {m[4] or 600} 分")
    lines.append("")
    lines.append("🥇 <b>日度排名奖励</b>")
    for i in range(10): lines.append(f"  {_keycap(i+1)} {daily[i]} 分")
    lines.append("")
    lines.append("1. <b>加分条件</b>")
    lines.append(f"🗓️ 每日签到：每天 +{SCORE_CHECKIN_POINTS} 分")
    lines.append(f"📊 发言统计：消息≥{MIN_MSG_CHARS} 字计入；支持日/月统计与奖励")
    lines.append(f"🤝 邀请加分：成功邀请 +{INVITE_REWARD_POINTS} 分；被邀请人退群 -{INVITE_REWARD_POINTS} 分")
    lines.append(f"💱 兑换：{REDEEM_RATE} 分 = 1 U；<b>满 {REDEEM_MIN_POINTS} 分</b>方可兑换")
    lines.append("❌ 清零：离群清零，或者兑换完清零。")
    lines.append("")
    lines.append("2. <b>统计规则</b>：")
    lines.append("• 禁止刷屏（连续多次被举报取消其奖励）")
    lines.append("• 禁止自说自话")
    lines.append("• 贴纸及表情包不统计")
    return "\n".join(lines)

# ====================== 兑换 U（略，保留你已有实现） ======================
TRX_ADDR_RE=re.compile(r"^T[1-9A-HJ-NP-Za-km-z]{33}$")
def redeem_create(chat_id:int, uid:int, u_amount:int, addr:str):
    row=_fetchone("SELECT username,first_name,last_name,points FROM scores WHERE chat_id=%s AND user_id=%s",(chat_id,uid))
    username,fn,ln,pts=(row or ("","","",0))
    _exec("""INSERT INTO redemptions(chat_id,user_id,username,first_name,last_name,points_snapshot,u_amount,trc20_addr,status,created_at)
             VALUES(%s,%s,%s,%s,%s,%s,%s,%s,'pending',%s)""",(chat_id,uid,username,fn,ln,int(pts or 0),u_amount,addr,utcnow().isoformat()))
    rid=_fetchone("SELECT LAST_INSERT_ID()",())[0]; return int(rid)

def redeem_broadcast_success(chat_id:int, uid:int, u_amount:int):
    un,fn,ln=ensure_user_display(chat_id, uid, ("","",""))
    full=(f"{fn or ''} {ln or ''}").strip() or (f"@{un}" if un else f"ID:{uid}")
    send_message_html(chat_id, f"🎉 恭喜“{safe_html(full)}”兑换成功\n兑换金额：<b>{u_amount} U</b>")

# ====================== 邀请绑定/新人欢迎 ======================
def _bind_invite_if_needed(chat_id:int, new_member:Dict, inviter:Dict):
    try:
        invitee_id=(new_member or {}).get("id"); inviter_id=(inviter or {}).get("id")
        if not invitee_id or not inviter_id or invitee_id==inviter_id: return
        existed=_fetchone("SELECT 1 FROM invites WHERE chat_id=%s AND invitee_id=%s",(chat_id,invitee_id))
        if existed: return
        _exec("INSERT INTO invites(chat_id, invitee_id, inviter_id, ts) VALUES(%s,%s,%s,%s)",(chat_id,invitee_id,inviter_id,utcnow().isoformat()))
        _add_points(chat_id, inviter_id, INVITE_REWARD_POINTS, inviter_id, "invite_new_member")
    except Exception: logger.exception("bind_invite error", extra={"chat_id":chat_id})

def handle_new_members(msg:Dict):
    chat_id=(msg.get("chat") or {}).get("id"); inviter=msg.get("from") or {}; members=msg.get("new_chat_members") or []
    for m in members:
        _upsert_user_base(chat_id, m or {})
        if inviter and inviter.get("id") and inviter.get("id")!=(m or {}).get("id"):
            _bind_invite_if_needed(chat_id, m, inviter)
    if WELCOME_PANEL_ENABLED and members:
        send_ephemeral_html(chat_id, build_rules_text(chat_id), WELCOME_EPHEMERAL_SECONDS, reply_markup=build_menu(False, chat_id))

def handle_left_member(msg:Dict):
    chat_id=(msg.get("chat") or {}).get("id"); left=msg.get("left_chat_member") or {}
    invitee_id=left.get("id"); 
    if not invitee_id: return
    row=_fetchone("SELECT inviter_id FROM invites WHERE chat_id=%s AND invitee_id=%s",(chat_id,invitee_id))
    if not row: return
    inviter_id=row[0]; _add_points(chat_id, inviter_id, -INVITE_REWARD_POINTS, inviter_id, "invite_auto_leave")
    _exec("DELETE FROM invites WHERE chat_id=%s AND invitee_id=%s",(chat_id,invitee_id))

# ====================== 菜单/按钮 ======================
def ikb(text:str,data:str)->dict: return {"text":text,"callback_data":data}
def urlb(text:str,url:str)->dict: return {"text":text,"url":url}

def is_chat_admin(chat_id:int, uid:Optional[int])->bool:
    if not uid: return False
    if uid in ADMIN_USER_IDS: return True
    if uid in list_chat_admin_ids(chat_id): return True
    r=http_get("getChatMember", params={"chat_id":chat_id,"user_id":uid})
    try:
        status=((r or {}).get("result") or {}).get("status","")
        return status in ("administrator","creator")
    except Exception:
        return False

def get_biz_buttons()->List[dict]:
    btns=[]; raw=(BIZ_LINKS or "").strip()
    if raw:
        for item in raw.split(";"):
            if not item.strip(): continue
            label,link=(item.split("|",1)+[""])[:2]
            if link.strip(): btns.append(urlb(label.strip() or "招商", link.strip()))
    else:
        if BIZ_A_URL: btns.append(urlb(BIZ_A_LABEL or "招商A", BIZ_A_URL))
        if BIZ_B_URL: btns.append(urlb(BIZ_B_LABEL or "招商B", BIZ_B_URL))
    return btns

def build_menu(is_admin_user:bool, chat_id:Optional[int]=None)->dict:
    kb=[
        [ikb("✅ 签到","ACT_CHECKIN")],
        [ikb("📌 我的积分","ACT_SCORE"), ikb("🏆 积分榜Top10","ACT_TOP10")],
        [ikb("📊 今日统计","ACT_SD_TODAY"), ikb("📊 本月统计","ACT_SM_THIS")],
        [ikb("📜 规则","ACT_RULES")],
        [ikb("🎁 兑换U","ACT_REDEEM")],
        [ikb("🆘 帮助","ACT_HELP")],
    ]
    if chat_id is not None and is_admin_user:
        kb.append([ikb("🛠 积分管理","ACT_SCORE_MGR")])  # <<< 新增
        kb.append([ikb("📰 自定义新闻","ACT_CNEWS_PANEL")])
        kb.append([ikb("📣 广告显示","ACT_AD_SHOW"), ikb("🟢 启用广告","ACT_AD_ENABLE"), ikb("🔴 禁用广告","ACT_AD_DISABLE")])
        kb.append([ikb("📎 设为附加模式","ACT_AD_MODE_ATTACH"), ikb("⏰ 设为定时模式","ACT_AD_MODE_SCHEDULE")])
        kb.append([ikb("🕒 设置时间点","ACT_AD_SET_TIMES"), ikb("🖼 设置图文广告","ACT_AD_SET_MEDIA"), ikb("🔍 预览广告","ACT_AD_PREVIEW")])
        kb.append([ikb("🧹 清空广告","ACT_AD_CLEAR"), ikb("✍️ 设置广告文本","ACT_AD_SET")])
        kb.append([ikb("🗞 立即推送新闻","ACT_NEWS_NOW"),
                   ikb(("🔴 关闭新闻播报" if news_enabled(chat_id) else "🟢 开启新闻播报"), "ACT_NEWS_TOGGLE")])
    biz_btns=get_biz_buttons()
    if biz_btns:
        row=[]
        for b in biz_btns:
            row.append(b)
            if len(row)==3: kb.append(row); row=[]
        if row: kb.append(row)
    return {"inline_keyboard":kb}

def send_menu_for(chat_id:int, uid:int):
    send_ephemeral_html(chat_id, "请选择功能：", PANEL_EPHEMERAL_SECONDS, reply_markup=build_menu(is_chat_admin(chat_id, uid), chat_id))

# ====================== 报表（略，与上一版一致） ======================
def list_top_day(chat_id:int, day:str, limit:int=10):
    return _fetchall("""
        SELECT
            mc.user_id,
            COALESCE(NULLIF(s.username, ''), MAX(mc.username))       AS username,
            COALESCE(NULLIF(s.first_name, ''), MAX(mc.first_name))   AS first_name,
            COALESCE(NULLIF(s.last_name, ''),  MAX(mc.last_name))    AS last_name,
            SUM(mc.cnt)                                              AS c
        FROM msg_counts mc
        LEFT JOIN scores s
          ON s.chat_id = mc.chat_id AND s.user_id = mc.user_id
        WHERE mc.chat_id = %s AND mc.day = %s
        GROUP BY mc.user_id
        ORDER BY c DESC
        LIMIT %s
    """,(chat_id, day, limit))
def list_top_month(chat_id:int, ym:str, limit:int=10):
    return _fetchall("""
        SELECT
            mc.user_id,
            COALESCE(NULLIF(s.username, ''), MAX(mc.username))       AS username,
            COALESCE(NULLIF(s.first_name, ''), MAX(mc.first_name))   AS first_name,
            COALESCE(NULLIF(s.last_name, ''),  MAX(mc.last_name))    AS last_name,
            SUM(mc.cnt)                                              AS c
        FROM msg_counts mc
        LEFT JOIN scores s
          ON s.chat_id = mc.chat_id AND s.user_id = mc.user_id
        WHERE mc.chat_id = %s AND mc.day LIKE CONCAT(%s, '-%%')
        GROUP BY mc.user_id
        ORDER BY c DESC
        LIMIT %s
    """,(chat_id, ym, limit))
def list_score_top(chat_id:int, limit:int=10):
    return _fetchall("SELECT user_id, username, first_name, last_name, points FROM scores WHERE chat_id=%s ORDER BY points DESC LIMIT %s",(chat_id,limit))
def eligible_member_count(chat_id:int)->int:
    admin_ids=list_chat_admin_ids(chat_id)
    ids=_fetchall("SELECT user_id FROM scores WHERE chat_id=%s AND COALESCE(is_bot,0)=0",(chat_id,))
    return len([i[0] for i in ids if i[0] not in admin_ids])

def clean_text(s:str)->str:
    if not s: return ""
    soup=BeautifulSoup(s,"html.parser"); return re.sub(r"\s+"," ", soup.get_text().strip())
def _zh(s:str)->str:
    if not s: return ""
    if not TRANSLATE_TO_ZH or _gt is None: return s
    try: return _gt.translate(s)
    except Exception: return s

# ====================== 新闻（略，保留上一版 push_news_once 等函数） ======================
CATEGORY_MAP={
    "finance":("财经",["https://www.reuters.com/finance/rss"]),
    "sea":("东南亚",["https://www.straitstimes.com/news/world/asia/rss.xml"]),
    "war":("战争",["https://feeds.bbci.co.uk/news/world/rss.xml"]),
}
def fetch_rss_list(urls:List[str], max_items:int)->List[Dict]:
    items=[]
    for u in urls:
        try:
            feed=feedparser.parse(u)
            for e in feed.entries[:max_items*2]:
                title=clean_text(e.get("title")); link=e.get("link") or ""; summary=clean_text(e.get("summary") or e.get("description"))
                if title and link: items.append({"title":title,"link":link,"summary":summary})
        except Exception as e:
            log(logging.WARNING,"rss parse error",event="rss",error=f"{u} {e}")
    seen=set(); uniq=[]
    for it in items:
        if it["link"] in seen: continue
        seen.add(it["link"]); uniq.append(it)
        if len(uniq)>=max_items: break
    return uniq
def already_posted(chat_id:int, category:str, link:str)->bool:
    return _fetchone("SELECT 1 FROM posted_news WHERE chat_id=%s AND category=%s AND link=%s",(chat_id,category,link)) is not None
def mark_posted(chat_id:int, category:str, link:str):
    _exec("INSERT IGNORE INTO posted_news(chat_id,category,link,ts) VALUES(%s,%s,%s,%s)",(chat_id,category,link,utcnow().isoformat()))
def push_news_once(chat_id:int):
    if not news_enabled(chat_id): return
    order=["finance","sea","war"]; now_str=tz_now().strftime("%Y-%m-%d %H:%M")
    sent=False
    for cat in order:
        cname,feeds=CATEGORY_MAP.get(cat,(cat,[]))
        items=fetch_rss_list(feeds, NEWS_ITEMS_PER_CAT)
        if not items: continue
        new_items=[it for it in items if not already_posted(chat_id, cat, it["link"])]
        if not new_items: continue
        lines=[f"🗞️ <b>{cname}</b> | {now_str}"]
        for i,it in enumerate(new_items,1):
            t=_zh(it['title']); s=_zh(it.get('summary') or "")
            if s: lines.append(f"{i}. {safe_html(t)}\n{safe_html(s)}\n{it['link']}")
            else: lines.append(f"{i}. {safe_html(t)}\n{it['link']}")
        en,content,mode,_times,mt,fid=ad_get(chat_id)
        if en and mode=="attach" and content.strip(): lines.append("📣 <b>广告</b>\n"+safe_html(content))
        send_message_html(chat_id,"\n".join(lines))
        if en and mode=="attach" and mt!="none" and fid: ad_send_now(chat_id, preview_only=True)
        for it in new_items: mark_posted(chat_id, cat, it["link"])
        sent=True
    if not sent: send_message_html(chat_id,"🗞️ 暂无可用新闻。")

# ====================== 调度（略，与上一版一致） ======================
def gather_known_chats()->List[int]:
    chats=set(NEWS_CHAT_IDS or [])
    for r in _fetchall("SELECT DISTINCT chat_id FROM msg_counts",()): chats.add(int(r[0]))
    for r in _fetchall("SELECT DISTINCT chat_id FROM scores",()): chats.add(int(r[0]))
    for r in _fetchall("SELECT chat_id FROM ads",()): chats.add(int(r[0]))
    return sorted(chats)
def maybe_push_news():
    key="next_news_at"; nv=state_get(key); now=tz_now()
    if nv:
        try: next_at=datetime.fromisoformat(nv)
        except Exception: next_at=now - timedelta(minutes=1)
        if next_at.tzinfo is None: next_at=next_at.replace(tzinfo=LOCAL_TZ)
    else: next_at=now - timedelta(minutes=1)
    if now>=next_at:
        for cid in (NEWS_CHAT_IDS or gather_known_chats()):
            try: push_news_once(cid)
            except Exception: logger.exception("news push error", extra={"chat_id":cid})
        state_set(key,(now+timedelta(minutes=INTERVAL_MINUTES)).isoformat())
def maybe_daily_report():
    if not STATS_ENABLED: return
    h,m=parse_hhmm(STATS_DAILY_AT); now=tz_now()
    if now.hour!=h or now.minute!=m: return
    chats=STATS_CHAT_IDS or gather_known_chats()
    yday=(now - timedelta(days=1)).strftime("%Y-%m-%d")
    for cid in chats:
        rk=f"daily_done:{cid}:{yday}"
        if state_get(rk): continue
        try:
            send_message_html(cid, build_daily_report(cid, yday))
            rows=list_top_day(cid, yday, limit=TOP_REWARD_SIZE)
            if rows:
                bonus=DAILY_TOP_REWARD_START
                for (uid,un,fn,ln,c) in rows:
                    _upsert_user_base(cid, {"id":uid,"username":un,"first_name":fn,"last_name":ln})
                    _add_points(cid, uid, max(bonus,0), uid, "top_day_reward"); bonus-=1
        except Exception: logger.exception("daily report error", extra={"chat_id":cid})
        state_set(rk,"1")
def maybe_monthly_report():
    if not STATS_ENABLED: return
    h,m=parse_hhmm(STATS_MONTHLY_AT); now=tz_now()
    if not (now.day==1 and now.hour==h and now.minute==m): return
    last_month=(now.replace(day=1)-timedelta(days=1)).strftime("%Y-%m")
    chats=STATS_CHAT_IDS or gather_known_chats()
    for cid in chats:
        rk=f"monthly_done:{cid}:{last_month}"
        if state_get(rk): continue
        try:
            send_message_html(cid, build_monthly_report(cid, last_month))
            rows=list_top_month(cid, last_month, 10)
            if rows:
                for idx,(uid,un,fn,ln,c) in enumerate(rows,1):
                    reward=MONTHLY_REWARD_RULE[idx-1] if idx-1<len(MONTHLY_REWARD_RULE) else 0
                    if reward>0:
                        _upsert_user_base(cid, {"id":uid,"username":un,"first_name":fn,"last_name":ln})
                        _add_points(cid, uid, reward, uid, "top_month_reward")
        except Exception: logger.exception("monthly report error", extra={"chat_id":cid})
        state_set(rk,"1")
def maybe_daily_broadcast():
    h,m=parse_hhmm(DAILY_BROADCAST_AT); now=tz_now()
    if now.hour!=h or now.minute!=m: return
    day=now.strftime("%Y-%m-%d")
    chats=STATS_CHAT_IDS or gather_known_chats()
    for cid in chats:
        rk=f"daily_broadcast:{cid}:{day}"
        if state_get(rk): continue
        try: send_message_html(cid, build_day_broadcast(cid, day))
        except Exception: logger.exception("daily broadcast error", extra={"chat_id":cid})
        state_set(rk,"1")
def maybe_ephemeral_gc_wrap():
    maybe_ephemeral_gc()
def scheduler_step():
    maybe_push_news(); maybe_daily_report(); maybe_monthly_report(); maybe_daily_broadcast(); maybe_ephemeral_gc_wrap()

# ====================== 报表文本函数（复用） ======================
def build_daily_report(chat_id:int, day:str)->str:
    rows=list_top_day(chat_id, day, limit=10)
    total=_fetchone("SELECT SUM(cnt) FROM msg_counts WHERE chat_id=%s AND day=%s",(chat_id,day))[0] or 0
    speakers=_fetchone("SELECT COUNT(DISTINCT user_id) FROM msg_counts WHERE chat_id=%s AND day=%s",(chat_id,day))[0] or 0
    members=eligible_member_count(chat_id)
    lines=[f"📊 <b>{day} 发言统计</b>", f"参与成员（剔除管理员/机器人）：<b>{members}</b>｜发言人数：<b>{speakers}</b>｜总条数：<b>{total}</b>"]
    if not rows: lines.append("暂无数据。"); return "\n".join(lines)
    for i,(uid,un,fn,ln,c) in enumerate(rows,1):
        lines.append(f"{i}. {rank_display_link(chat_id, uid, un, fn, ln)} — <b>{c}</b>")
    return "\n".join(lines)
def build_monthly_report(chat_id:int, ym:str)->str:
    rows=list_top_month(chat_id, ym, limit=10)
    total=_fetchone("SELECT SUM(cnt) FROM msg_counts WHERE chat_id=%s AND day LIKE CONCAT(%s,'-%')",(chat_id,ym))[0] or 0
    speakers=_fetchone("SELECT COUNT(DISTINCT user_id) FROM msg_counts WHERE chat_id=%s AND day LIKE CONCAT(%s,'-%')",(chat_id,ym))[0] or 0
    members=eligible_member_count(chat_id)
    lines=[f"📈 <b>{ym} 月度发言统计</b>", f"参与成员（剔除管理员/机器人）：<b>{members}</b>｜发言人数：<b>{speakers}</b>｜总条数：<b>{total}</b>"]
    if not rows: lines.append("暂无数据。"); return "\n".join(lines)
    for i,(uid,un,fn,ln,c) in enumerate(rows,1):
        lines.append(f"{i}. {rank_display_link(chat_id, uid, un, fn, ln)} — <b>{c}</b>")
    return "\n".join(lines)
def build_day_broadcast(chat_id:int, day:str)->str:
    speakers=_fetchone("SELECT COUNT(DISTINCT user_id) FROM msg_counts WHERE chat_id=%s AND day=%s",(chat_id,day))[0] or 0
    lines=[f"🕛 <b>{day} 日终播报</b>", f"🧑‍🤝‍🧑 活跃人数：<b>{speakers}</b>"]
    rows_s=list_score_top(chat_id,10); lines.append("🏆 <b>积分榜 Top10</b>")
    if not rows_s: lines.append("（暂无积分数据）")
    else:
        for i,(uid,un,fn,ln,pts) in enumerate(rows_s,1):
            lines.append(f"{i}. {rank_display_link(chat_id, uid, un, fn, ln)} — <b>{pts}</b> 分")
    rows_m=list_top_day(chat_id, day,10); lines.append("💬 <b>发言 Top10</b>")
    if not rows_m: lines.append("（今日暂无发言数据）")
    else:
        for i,(uid,un,fn,ln,c) in enumerate(rows_m,1):
            lines.append(f"{i}. {rank_display_link(chat_id, uid, un, fn, ln)} — <b>{c}</b> 条")
    return "\n".join(lines)

# ====================== ==== 积分管理（新增） ======================
def find_user_by_username(chat_id:int, username:str)->Optional[Tuple[int,str,str,str]]:
    uname=(username or "").lstrip("@").strip()
    if not uname: return None
    # 先在 scores 表查
    row=_fetchone("SELECT user_id, username, first_name, last_name FROM scores WHERE chat_id=%s AND LOWER(username)=LOWER(%s) LIMIT 1",(chat_id, uname))
    if row: return (int(row[0]), row[1] or "", row[2] or "", row[3] or "")
    # 再在 msg_counts 最近记录里查
    row=_fetchone("""SELECT mc.user_id, mc.username, mc.first_name, mc.last_name 
                     FROM msg_counts mc 
                     WHERE mc.chat_id=%s AND LOWER(mc.username)=LOWER(%s) 
                     ORDER BY mc.day DESC LIMIT 1""",(chat_id, uname))
    if row: return (int(row[0]), row[1] or "", row[2] or "", row[3] or "")
    return None

def admin_adjust_points_by_uid(chat_id:int, admin_id:int, target_uid:int, delta:int, reason:str)->int:
    before=_get_points(chat_id, target_uid)
    _add_points(chat_id, target_uid, delta, admin_id, reason)
    after=_get_points(chat_id, target_uid)
    sign="+" if delta>=0 else ""
    send_message_html(chat_id, f"🛠 管理员已调整积分：\n目标：<a href=\"tg://user?id={target_uid}\">ID:{target_uid}</a>\n变动：<b>{sign}{delta}</b>\n当前：<b>{after}</b>", disable_preview=True)
    return after

def admin_adjust_points(chat_id:int, admin_id:int, username:str, delta:int, reason:str):
    found=find_user_by_username(chat_id, username)
    if not found:
        send_message_html(chat_id, "未找到该用户名的成员。\n小技巧：可以直接<b>回复目标成员的消息</b>并发送“/score_add 200”或“/score_sub 50”。")
        return
    uid,un,fn,ln=found
    before=_get_points(chat_id, uid)
    _add_points(chat_id, uid, delta, admin_id, reason)
    after=_get_points(chat_id, uid)
    name_link=rank_display_link(chat_id, uid, un, fn, ln)
    sign="+" if delta>=0 else ""
    send_message_html(chat_id, f"🛠 管理员已调整积分：\n目标：{name_link}\n变动：<b>{sign}{delta}</b>\n当前：<b>{after}</b>", disable_preview=True)

def parse_username_and_amount(text:str)->Tuple[Optional[str], Optional[int]]:
    # 支持：@name 200 / name 200 / @name +200 / @name -50
    m=re.search(r"(?:^|\s)@?([A-Za-z0-9_]{5,32})\s+([+-]?\d+)(?:\s|$)", text.strip())
    if not m: return None, None
    return m.group(1), int(m.group(2))

def build_score_mgr_kb()->dict:
    return {"inline_keyboard":[
        [ikb("➕ 加分（发送“@用户名 数值”或回复消息发“数值”）","ACT_SCORE_ADD")],
        [ikb("➖ 减分（发送“@用户名 数值”或回复消息发“数值”）","ACT_SCORE_SUB")],
        [ikb("❌ 退出","ACT_SCORE_CANCEL")]
    ]}

def open_score_mgr(chat_id:int, uid:int):
    if not is_chat_admin(chat_id, uid):
        send_ephemeral_html(chat_id,"仅管理员可使用积分管理。", POPUP_EPHEMERAL_SECONDS); return
    send_ephemeral_html(
        chat_id,
        "🛠 <b>积分管理</b>\n"
        "使用方法：\n"
        "1) 直接回复目标成员的一条消息，发送“/score_add 200”或“/score_sub 50”。\n"
        "2) 或点下面的“加分/减分”，然后发送“@用户名 200”。",
        PANEL_EPHEMERAL_SECONDS,
        reply_markup=build_score_mgr_kb()
    )

# ====================== 命令 ======================
HELP_TEXT=(
    "🧭 功能导航：\n"
    " /menu 打开菜单\n"
    " /checkin 签到\n"
    " /score 查看我的积分\n"
    " /top10 查看积分榜前十\n"
    " /rules 查看积分规则\n"
    " /redeem [U数量] 申请兑换\n"
    " /cancel 取消当前操作\n\n"
    "管理员：/score_add @username 200 、 /score_sub @username 50（也可回复消息）"
)

def do_checkin(chat_id:int, uid:int, frm:dict):
    today=tz_now().strftime("%Y-%m-%d")
    if _get_last_checkin(chat_id, uid)==today:
        send_message_html(chat_id, f"✅ 你今天已经签到过啦（{today}）。"); return
    _add_points(chat_id, uid, SCORE_CHECKIN_POINTS, uid, "daily_checkin")
    _set_last_checkin(chat_id, uid, today)
    un,fn,ln=ensure_user_display(chat_id, uid, (frm.get("username") or "", frm.get("first_name") or "", frm.get("last_name") or ""))
    full=(f"{fn or ''} {ln or ''}").strip() or (f"@{un}" if un else f"ID:{uid}")
    total=_get_points(chat_id, uid)
    send_message_html(chat_id, f"签到人：<b>{safe_html(full)}</b>\n签到成功：<b>积分+{SCORE_CHECKIN_POINTS}</b>\n总积分为：<b>{total}</b>")

def _next_update_offset()->int:
    v=state_get("tg_update_offset")
    try: return int(v)
    except Exception: return 0
def _set_update_offset(v:int): state_set("tg_update_offset", str(v))
def _handle_command(chat_id: int, uid: int, frm: dict, text: str, msg: Optional[dict] = None):
    parts = text.strip().split()
    cmd = parts[0].lower()

    # 退出/取消
    if cmd in ("/cancel","/stop","/exit","/esc") or parts[0] in ("取消","结束"):
        clear_pending_states(chat_id, uid)
        send_ephemeral_html(chat_id, "已取消当前操作。", POPUP_EPHEMERAL_SECONDS)
        return

    # 打开菜单
    if cmd in ("/start","/menu") or parts[0] in ("菜单","导航"):
        clear_pending_states(chat_id, uid)
        send_menu_for(chat_id, uid)
        return

    # 基本功能
    if cmd in ("/help", "帮助"):
        send_ephemeral_html(chat_id, HELP_TEXT, POPUP_EPHEMERAL_SECONDS); return
    if cmd in ("/rules", "规则"):
        send_ephemeral_html(chat_id, build_rules_text(chat_id), POPUP_EPHEMERAL_SECONDS); return
    if cmd in ("/checkin", "签到"):
        do_checkin(chat_id, uid, frm); return
    if cmd in ("/score", "/points", "我的积分"):
        pts = _get_points(chat_id, uid)
        send_ephemeral_html(chat_id, f"你的当前积分：<b>{pts}</b>", POPUP_EPHEMERAL_SECONDS); return
    if cmd in ("/top10", "积分榜"):
        rows = list_score_top(chat_id, 10)
        if not rows:
            send_ephemeral_html(chat_id, "暂无积分数据。", POPUP_EPHEMERAL_SECONDS); return
        lines = ["🏆 <b>积分榜 Top10</b>"]
        for i,(u,un,fn,ln,pts) in enumerate(rows, 1):
            lines.append(f"{i}. {rank_display_link(chat_id, u, un, fn, ln)} — <b>{pts}</b> 分")
        send_ephemeral_html(chat_id, "\n".join(lines), POPUP_EPHEMERAL_SECONDS); return

    # 兑换 U
    if cmd == "/redeem" or cmd == "兑换u":
        pts = _get_points(chat_id, uid)
        if pts < REDEEM_MIN_POINTS:
            send_ephemeral_html(chat_id, f"当前积分 <b>{pts}</b>，未达兑换门槛（≥{REDEEM_MIN_POINTS}）。", POPUP_EPHEMERAL_SECONDS); return
        max_u = pts // REDEEM_RATE
        target_u = max_u
        if len(parts)>=2 and parts[1].isdigit():
            req_u = int(parts[1])
            if req_u > max_u:
                send_ephemeral_html(chat_id, f"可兑上限 <b>{max_u}</b> U，你不足以兑换 {req_u} U。", POPUP_EPHEMERAL_SECONDS); return
            target_u = req_u
        state_set(f"pending:redeemaddr:{chat_id}:{uid}", str(target_u))
        send_ephemeral_html(chat_id, f"请回复 <b>TRC20</b> 地址（以 <code>T</code> 开头）。本次计划兑换：<b>{target_u} U</b>", POPUP_EPHEMERAL_SECONDS)
        return

    # ==== 管理员命令：/score_add /score_sub ====
    if cmd in ("/score_add", "/score_sub"):
        if not is_chat_admin(chat_id, uid):
            send_ephemeral_html(chat_id, "仅管理员可使用积分管理。", POPUP_EPHEMERAL_SECONDS); return
        mode = "add" if cmd == "/score_add" else "sub"

        # A) 如果是“回复某人”的消息：只需提供数值
        if msg and msg.get("reply_to_message"):
            m = re.search(r"([+-]?\d+)", text)
            if not m:
                send_ephemeral_html(chat_id, "请在命令后写上数值，例如：/score_add 200。", POPUP_EPHEMERAL_SECONDS); return
            amt = int(m.group(1))
            if mode == "sub" and amt > 0: amt = -amt
            target = (msg["reply_to_message"].get("from") or {}).get("id")
            if not target:
                send_ephemeral_html(chat_id, "未识别到被回复的目标用户。", POPUP_EPHEMERAL_SECONDS); return
            admin_adjust_points_by_uid(chat_id, uid, target, amt, f"admin_{mode}")
            return

        # B) 非回复：解析“@用户名 数值”
        uname, amt = parse_username_and_amount(text)
        if not uname or amt is None:
            send_ephemeral_html(
                chat_id,
                "用法：\n/score_add @username 200\n/score_sub @username 50\n或先<b>回复</b>目标消息后发：/score_add 200",
                POPUP_EPHEMERAL_SECONDS
            ); 
            return
        if mode == "sub" and amt > 0: amt = -amt
        admin_adjust_points(chat_id, uid, uname, amt, f"admin_{mode}")
        return

    # 兼容旧广告命令
    if cmd == "/adset" and is_chat_admin(chat_id, uid):
        state_set(f"pending:set_ad_text:{chat_id}:{uid}", "1")
        send_ephemeral_html(chat_id, "请发送广告文本（发送后立即保存）。", POPUP_EPHEMERAL_SECONDS); return
    if cmd == "/adtimes" and is_chat_admin(chat_id, uid):
        try:
            ad_timepicker_open(chat_id, uid)
        except NameError:
            send_ephemeral_html(chat_id, "（时间选择器函数未引入，本命令暂不可用）", POPUP_EPHEMERAL_SECONDS)
        return

    


def _handle_pending_inputs(msg: dict) -> bool:
    """返回 True 表示该消息已在此处消费"""
    chat_id = (msg.get("chat") or {}).get("id")
    frm = msg.get("from") or {}
    uid = frm.get("id")
    text = (msg.get("text") or "").strip()

    # 允许这些词在任何 pending 下直接穿透（交给 _handle_command）
    if text and (text.startswith("/") or text in ("菜单","导航","帮助","规则","签到","积分榜","我的积分")):
        if text.lower() in ("/cancel","/stop","/exit","/esc") or text in ("取消","结束"):
            clear_pending_states(chat_id, uid)
            send_ephemeral_html(chat_id, "已取消当前操作。", POPUP_EPHEMERAL_SECONDS)
            return True
        return False

    # 1) 兑U地址
    pend_key = f"pending:redeemaddr:{chat_id}:{uid}"
    plan = state_get(pend_key)
    if plan:
        if TRX_ADDR_RE.match(text):
            amt = int(plan)
            rid = redeem_create(chat_id, uid, amt, text)
            state_del(pend_key)
            kb = {"inline_keyboard":[[ikb("✅ 管理员批准", f"REDEEM_APPR:{rid}"), ikb("❌ 拒绝", f"REDEEM_REJ:{rid}")]]}
            send_message_html(chat_id, f"收到兑换申请 #{rid}\n申请人：<code>{uid}</code>\n金额：<b>{amt} U</b>\n地址：<code>{safe_html(text)}</code>\n（仅管理员可操作）", reply_markup=kb)
        else:
            send_ephemeral_html(chat_id, "地址格式不正确，请发送以 T 开头的 TRC20 地址，或发送 /cancel 退出。", POPUP_EPHEMERAL_SECONDS)
        return True

    # 2) 设置广告文本
    pend_key = f"pending:set_ad_text:{chat_id}:{uid}"
    if state_get(pend_key):
        if is_chat_admin(chat_id, uid):
            ad_set(chat_id, text)
            state_del(pend_key)
            send_ephemeral_html(chat_id, "广告文本已更新。", POPUP_EPHEMERAL_SECONDS)
        return True

    # 3) 设置广告时间（兼容手输）
    pend_key = f"pending:set_ad_times:{chat_id}:{uid}"
    if state_get(pend_key):
        if is_chat_admin(chat_id, uid):
            t = ad_set_times(chat_id, text)
            state_del(pend_key)
            send_ephemeral_html(chat_id, f"定时发送时间点已更新：{t}", POPUP_EPHEMERAL_SECONDS)
        return True

    # 4) 设置广告图文（等待媒体）
    pend_key = f"pending:set_ad_media:{chat_id}:{uid}"
    if state_get(pend_key):
        if is_chat_admin(chat_id, uid):
            cap = (msg.get("caption") or text or "").strip()
            if msg.get("photo"):
                fid = msg["photo"][-1]["file_id"]
                ad_set_media(chat_id, "photo", fid, cap)
                send_ephemeral_html(chat_id, "已保存图片广告。", POPUP_EPHEMERAL_SECONDS)
                state_del(pend_key)
                return True
            if msg.get("video"):
                fid = msg["video"]["file_id"]
                ad_set_media(chat_id, "video", fid, cap)
                send_ephemeral_html(chat_id, "已保存视频广告。", POPUP_EPHEMERAL_SECONDS)
                state_del(pend_key)
                return True
            send_ephemeral_html(chat_id, "请发送图片或视频作为广告素材（可带文案），或发送 /cancel 退出。", POPUP_EPHEMERAL_SECONDS)
        return True

    # 5) 积分管理“模式等待”：
    #    pending:score:mode:{chat_id}:{uid} = 'add' | 'sub'
    pend_key = f"pending:score:mode:{chat_id}:{uid}"
    mode = state_get(pend_key)
    if mode:
        # 先看是不是“回复某人”的消息，只要发一个数字就行
        if msg.get("reply_to_message"):
            m = re.search(r"([+-]?\d+)", text)
            if m:
                amt = int(m.group(1))
                if mode == "sub" and amt > 0: amt = -amt
                target = (msg["reply_to_message"].get("from") or {}).get("id")
                if target:
                    admin_adjust_points_by_uid(chat_id, uid, target, amt, f"admin_{mode}")
                    state_del(pend_key)
                    return True

        # 非回复：解析“@用户名 数值”
        uname, amt = parse_username_and_amount(text)
        if uname and amt is not None:
            if mode == "sub" and amt > 0: amt = -amt
            admin_adjust_points(chat_id, uid, uname, amt, f"admin_{mode}")
            state_del(pend_key)
            return True

        # 还没有解析成功——提示格式
        send_ephemeral_html(chat_id, "格式：@用户名 数值；或先<b>回复</b>目标消息再仅发送“数值”。（/cancel 退出）", POPUP_EPHEMERAL_SECONDS)
        return True

    return False


def process_updates_once():
    offset = _next_update_offset()
    params = {"timeout": POLL_TIMEOUT, "offset": offset + 1}
    data = http_get("getUpdates", params=params)
    if not data or not data.get("ok"):
        return
    for upd in data.get("result") or []:
        upd_id = upd.get("update_id", 0)
        try:
            if "message" in upd:
                msg = upd["message"]
                chat = msg.get("chat") or {}
                chat_id = chat.get("id")
                frm = msg.get("from") or {}
                uid = frm.get("id")

                # 新人 / 退群
                if msg.get("new_chat_members"): handle_new_members(msg)
                if msg.get("left_chat_member"): handle_left_member(msg)

                # 统计消息长度
                text = msg.get("text") or msg.get("caption") or ""
                if isinstance(text, str) and len(text.strip()) >= MIN_MSG_CHARS:
                    inc_msg_count(chat_id, frm, tz_now().strftime("%Y-%m-%d"), 1)

                # 先处理 pending
                if _handle_pending_inputs(msg):
                    pass
                else:
                    # 明确命令
                    if isinstance(text, str) and text.startswith("/"):
                       _handle_command(chat_id, uid, frm, text, msg=msg)

                    # 菜单文字触发
                    elif text in ("菜单","导航","帮助","规则","签到","积分榜","我的积分"):
                        _handle_command(chat_id, uid, frm, text, msg=msg)

            elif "callback_query" in upd:
                cb = upd["callback_query"]
                data_s = cb.get("data") or ""
                msg = cb.get("message") or {}
                chat_id = (msg.get("chat") or {}).get("id")
                frm = cb.get("from") or {}
                uid = frm.get("id")

                # 立刻回 ACK，避免回调过期报错
                answer_callback_query(cb.get("id"))

                # 普通用户功能
                if data_s == "ACT_CHECKIN":
                    do_checkin(chat_id, uid, frm)
                elif data_s == "ACT_SCORE":
                    pts = _get_points(chat_id, uid)
                    send_ephemeral_html(chat_id, f"你的当前积分：<b>{pts}</b>", POPUP_EPHEMERAL_SECONDS)
                elif data_s == "ACT_TOP10":
                    rows = list_score_top(chat_id, 10)
                    if not rows:
                        send_ephemeral_html(chat_id, "暂无积分数据。", POPUP_EPHEMERAL_SECONDS)
                    else:
                        lines = ["🏆 <b>积分榜 Top10</b>"]
                        for i,(u,un,fn,ln,pts) in enumerate(rows, 1):
                            lines.append(f"{i}. {rank_display_link(chat_id, u, un, fn, ln)} — <b>{pts}</b> 分")
                        send_ephemeral_html(chat_id, "\n".join(lines), POPUP_EPHEMERAL_SECONDS)
                elif data_s == "ACT_SD_TODAY":
                    d = tz_now().strftime("%Y-%m-%d")
                    send_ephemeral_html(chat_id, build_daily_report(chat_id, d), POPUP_EPHEMERAL_SECONDS, disable_preview=False)
                elif data_s == "ACT_SM_THIS":
                    ym = tz_now().strftime("%Y-%m")
                    send_ephemeral_html(chat_id, build_monthly_report(chat_id, ym), POPUP_EPHEMERAL_SECONDS, disable_preview=False)
                elif data_s == "ACT_RULES":
                    send_ephemeral_html(chat_id, build_rules_text(chat_id), POPUP_EPHEMERAL_SECONDS)
                elif data_s == "ACT_HELP":
                    send_ephemeral_html(chat_id, HELP_TEXT, POPUP_EPHEMERAL_SECONDS)
                elif data_s == "ACT_REDEEM":
                    _handle_command(chat_id, uid, frm, "/redeem", msg=None)


                # —— 管理功能 —— #
                elif data_s == "ACT_SCORE_MGR":
                    open_score_mgr(chat_id, uid)
                elif data_s == "ACT_SCORE_ADD":
                    if is_chat_admin(chat_id, uid):
                        state_set(f"pending:score:mode:{chat_id}:{uid}", "add")
                        send_ephemeral_html(chat_id, "请输入：@用户名 数值；或先<b>回复</b>目标消息后只发“数值”。（/cancel 退出）", POPUP_EPHEMERAL_SECONDS)
                elif data_s == "ACT_SCORE_SUB":
                    if is_chat_admin(chat_id, uid):
                        state_set(f"pending:score:mode:{chat_id}:{uid}", "sub")
                        send_ephemeral_html(chat_id, "请输入：@用户名 数值；或先<b>回复</b>目标消息后只发“数值”。（/cancel 退出）", POPUP_EPHEMERAL_SECONDS)
                elif data_s == "ACT_SCORE_CANCEL":
                    clear_pending_states(chat_id, uid)
                    send_ephemeral_html(chat_id, "已退出积分管理。", POPUP_EPHEMERAL_SECONDS)

                elif data_s == "ACT_NEWS_NOW":
                    push_news_once(chat_id)
                elif data_s == "ACT_NEWS_TOGGLE":
                    en = news_enabled(chat_id); news_set_enabled(chat_id, not en)
                    send_ephemeral_html(chat_id, f"新闻播报已{'开启' if not en else '关闭'}。", POPUP_EPHEMERAL_SECONDS)

                elif data_s == "ACT_AD_SHOW":
                    en, ct, mode, times, mt, fid = ad_get(chat_id)
                    info = [
                        f"状态：{'启用' if en else '禁用'}",
                        f"模式：{mode}",
                        f"时间：{times or '（未设置）'}",
                        f"媒体：{mt}{'✅' if fid else ''}",
                        f"文本：{('有' if ct.strip() else '空')}"
                    ]
                    send_ephemeral_html(chat_id, "📣 <b>广告概览</b>\n" + "\n".join(info), POPUP_EPHEMERAL_SECONDS)

                elif data_s == "ACT_AD_PREVIEW":
                    ad_send_now(chat_id, preview_only=True)
                elif data_s == "ACT_AD_ENABLE":
                    if is_chat_admin(chat_id, uid):
                        ad_enable(chat_id, True); send_ephemeral_html(chat_id, "广告已启用。", POPUP_EPHEMERAL_SECONDS)
                elif data_s == "ACT_AD_DISABLE":
                    if is_chat_admin(chat_id, uid):
                        ad_enable(chat_id, False); send_ephemeral_html(chat_id, "广告已禁用。", POPUP_EPHEMERAL_SECONDS)
                elif data_s == "ACT_AD_MODE_ATTACH":
                    if is_chat_admin(chat_id, uid):
                        ad_set_mode(chat_id, "attach"); send_ephemeral_html(chat_id, "广告模式：附加。", POPUP_EPHEMERAL_SECONDS)
                elif data_s == "ACT_AD_MODE_SCHEDULE":
                    if is_chat_admin(chat_id, uid):
                        ad_set_mode(chat_id, "schedule"); send_ephemeral_html(chat_id, "广告模式：定时。", POPUP_EPHEMERAL_SECONDS)
                elif data_s == "ACT_AD_CLEAR":
                    if is_chat_admin(chat_id, uid):
                        ad_clear(chat_id); send_ephemeral_html(chat_id, "广告已清空。", POPUP_EPHEMERAL_SECONDS)
                elif data_s == "ACT_AD_SET_TIMES":
                    if is_chat_admin(chat_id, uid):
                        try:
                            ad_timepicker_open(chat_id, uid)
                        except NameError:
                            send_ephemeral_html(chat_id, "（时间选择器函数未引入，本按钮暂不可用）", POPUP_EPHEMERAL_SECONDS)
                elif data_s == "ACT_AD_SET":
                    if is_chat_admin(chat_id, uid):
                        state_set(f"pending:set_ad_text:{chat_id}:{uid}", "1")
                        send_ephemeral_html(chat_id, "请发送广告文本（发送后立即保存）。", POPUP_EPHEMERAL_SECONDS)
                elif data_s == "ACT_AD_SET_MEDIA":
                    if is_chat_admin(chat_id, uid):
                        state_set(f"pending:set_ad_media:{chat_id}:{uid}", "1")
                        send_ephemeral_html(chat_id, "请发送图片或视频作为广告素材（可带文案）。", POPUP_EPHEMERAL_SECONDS)

                elif data_s.startswith("REDEEM_APPR:") or data_s.startswith("REDEEM_REJ:"):
                    rid = int(data_s.split(":",1)[1])
                    if is_chat_admin(chat_id, uid):
                        approve = data_s.startswith("REDEEM_APPR:")
                        admin_redeem_decide(chat_id, rid, approve=approve, admin_id=uid)
                    else:
                        send_ephemeral_html(chat_id, "仅管理员可操作。", POPUP_EPHEMERAL_SECONDS)

                # —— 时间选择器所有按钮 —— #
                elif data_s.startswith("AT_"):
                    try:
                        ad_timepicker_handle(chat_id, uid, (msg.get("message_id") or 0), data_s, cb.get("id"))
                    except NameError:
                        send_ephemeral_html(chat_id, "（时间选择器函数未引入，本按钮暂不可用）", POPUP_EPHEMERAL_SECONDS)

        except Exception as e:
            logger.exception("update handle error: %s", e)
        finally:
            if upd_id > offset:
                offset = upd_id
                _set_update_offset(offset)


# ------------------------------- 启动主循环 -------------------------------
def main():
    print(f"[boot] TZ={LOCAL_TZ_NAME}, MYSQL={MYSQL_USER}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}")
    try:
        get_conn(); init_db()
        logger.info("boot ok | %s", json.dumps(
            {"event":"boot","cmd":f"{LOCAL_TZ_NAME} poll={POLL_TIMEOUT}s http={HTTP_TIMEOUT}s news_interval={INTERVAL_MINUTES}m"},
            ensure_ascii=False))
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


if __name__ == "__main__":
    main()

