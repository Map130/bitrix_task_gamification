import json, os, time, logging
from datetime import datetime, timedelta
from typing import Dict, List, Tuple
from urllib.parse import urlencode
from zoneinfo import ZoneInfo

import pika, requests
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

load_dotenv()
WEBHOOK = os.getenv("BITRIX_WEBHOOK_URL","").rstrip("/")
RABBIT  = os.getenv("RABBITMQ_URL","amqp://guest:guest@rabbitmq:5672/%2F")
TZ_NAME = os.getenv("TZ_NAME","Asia/Almaty")
REFETCH = int(os.getenv("REFETCH_INTERVAL_SEC","300"))
BATCH_PAUSE = float(os.getenv("BATCH_PAUSE_SEC","0.2"))
HTTP_TIMEOUT = int(os.getenv("HTTP_TIMEOUT_SEC","60"))
RETRIES = int(os.getenv("RETRIES","4"))
BACKOFF = float(os.getenv("BACKOFF_BASE_SEC","0.6"))
QUEUE_USERS = os.getenv("QUEUE_USERS","bitrix.users.map")
QUEUE_TASKS = os.getenv("QUEUE_TASKS_EVENTS","bitrix.tasks.events")
MAX_CMDS = 50
TZ = ZoneInfo(TZ_NAME)

if not WEBHOOK:
    raise RuntimeError("BITRIX_WEBHOOK_URL is required")

def week_bounds_iso():
    now = datetime.now(TZ)
    mon = now - timedelta(days=now.weekday())
    mon = mon.replace(hour=0, minute=0, second=0, microsecond=0)
    sun = mon + timedelta(days=6, hours=23, minutes=59, seconds=59)
    return mon.isoformat(), sun.isoformat()

def _post(url, payload):
    last = None
    for i in range(1, RETRIES+1):
        try:
            r = requests.post(url, json=payload, timeout=HTTP_TIMEOUT)
            r.raise_for_status()
            data = r.json()
            if isinstance(data, dict) and "error" in data:
                error_message = data.get("error_description", data["error"])
                logging.error(f"Bitrix API error: {error_message}")
                raise RuntimeError(error_message)
            return data
        except Exception as e:
            last = e
            logging.warning(f"Request failed (attempt {i}/{RETRIES}): {e}")
            if i < RETRIES: time.sleep(BACKOFF*(2**(i-1)))
            else:
                logging.error(f"Request failed after {RETRIES} retries.")
                raise last

def rest(method, payload):
    return _post(f"{WEBHOOK}/{method}.json", payload)

def batch(cmd: Dict[str,str], halt=0):
    return _post(f"{WEBHOOK}/batch.json", {"cmd": cmd, "halt": halt})

def get_users_map() -> Dict[int,str]:
    out = {}
    start = 0
    while True:
        resp = rest("user.search", {"FILTER":{"ACTIVE":"Y"},"SELECT":["ID","NAME","LAST_NAME"],"start":start})
        for u in resp.get("result", []):
            uid = int(u["ID"]); name = (" ".join([u.get("NAME") or "", u.get("LAST_NAME") or ""]).strip()) or f"User {uid}"
            out[uid] = name
        if "next" in resp: start = resp["next"]
        else: break
    return out

def build_tasks_cmd(uid:int, dt_from:str, dt_to:str, start:int=0)->str:
    params = {
        "filter[RESPONSIBLE_ID]": uid,
        "filter[STATUS]": 5,
        "filter[>=CLOSED_DATE]": dt_from,
        "filter[<=CLOSED_DATE]": dt_to,
        "order[ID]":"asc",
        "start": start,
        "select[]":["ID"]
    }
    return "tasks.task.list?" + urlencode(params, doseq=True)

def fetch_closed(user_ids: List[int]) -> Dict[int, List[int]]:
    dt_from, dt_to = week_bounds_iso()
    remaining = {uid:0 for uid in user_ids}
    out = {uid:[] for uid in user_ids}
    while remaining:
        cmd = {}; mapping: List[Tuple[str,int]] = []
        for uid in list(remaining.keys())[:MAX_CMDS]:
            start = remaining[uid]; alias = f"u{uid}_s{start}"
            cmd[alias] = build_tasks_cmd(uid, dt_from, dt_to, start)
            mapping.append((alias, uid))
        data = batch(cmd)
        res = (data.get("result") or {}).get("result", {})
        nxt = (data.get("result") or {}).get("result_next", {})
        for alias, uid in mapping:
            page = res.get(alias) or {}
            tasks = page.get("tasks", []) or page.get("result", []) or []
            ids = []
            for t in tasks:
                tid = t.get("ID") or t.get("id")
                if tid is None: continue
                try: ids.append(int(tid))
                except: pass
            out[uid].extend(ids)
            if alias in nxt: remaining[uid] = nxt[alias]
            else: remaining.pop(uid, None)
        if remaining: time.sleep(BATCH_PAUSE)
    return out

def open_channel():
    params = pika.URLParameters(RABBIT)
    conn = pika.BlockingConnection(params); ch = conn.channel()
    ch.queue_declare(queue=QUEUE_USERS, durable=True)
    ch.queue_declare(queue=QUEUE_TASKS, durable=True)
    return conn, ch

import json, os, time, logging
from datetime import datetime, timedelta
from typing import Dict, List, Tuple
from urllib.parse import urlencode
from zoneinfo import ZoneInfo

import pika, requests
from dotenv import load_dotenv

# Импортируем наш новый модуль для работы с конфигом
from shared.config import get_config_value

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

load_dotenv()
# ... (остальные переменные)
# ... (код функций fetch_closed, rest, batch и т.д. остается без изменений)

def main():
    logging.info("Producer starting...")
    
    # Небольшая задержка перед стартом, чтобы config-server успел заполнить Redis
    time.sleep(5) 

    conn, ch = open_channel()
    logging.info("RabbitMQ connection and channel opened.")
    try:
        while True:
            # Получаем текущий режим работы
            mode = get_config_value("producer_mode", "none")

            if mode == "polling":
                logging.info("Running in 'polling' mode.")
                
                logging.info("Fetching users map...")
                users_map = get_users_map()
                ch.basic_publish("", QUEUE_USERS, json.dumps({str(k):v for k,v in users_map.items()}, ensure_ascii=False).encode(),
                                 pika.BasicProperties(content_type="application/json", delivery_mode=2))
                logging.info(f"Published {len(users_map)} users to queue '{QUEUE_USERS}'")

                logging.info("Fetching closed tasks for users...")
                tasks_map = fetch_closed(list(users_map.keys()))
                
                snapshot_payload = {
                    "type": "snapshot",
                    "data": {str(k): v for k, v in tasks_map.items()}
                }
                
                ch.basic_publish("", QUEUE_TASKS_EVENTS, json.dumps(snapshot_payload, ensure_ascii=False).encode(),
                                 pika.BasicProperties(content_type="application/json", delivery_mode=2))
                logging.info(f"Published task snapshot for {len(tasks_map)} users to queue '{QUEUE_TASKS_EVENTS}'")

                logging.info(f"Sleeping for {REFETCH} seconds...")
                time.sleep(REFETCH)
            else:
                logging.info(f"Standby mode. Current mode is '{mode}'. Checking again in 60 seconds.")
                time.sleep(60)

    except KeyboardInterrupt:
        logging.info("Producer stopped by user.")
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}", exc_info=True)
    finally:
        if conn and not conn.is_closed:
            conn.close()
            logging.info("RabbitMQ connection closed.")

if __name__ == "__main__":
    main()
