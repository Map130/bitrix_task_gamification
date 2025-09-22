from __future__ import annotations

import time
import requests
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo
from urllib.parse import urlencode

# Настройка логгера для модуля
logger = logging.getLogger(__name__)

class BitrixSimple:
    """Упрощённый клиент: два метода — пользователи и недельные задачи (batch)."""

    MAX_CMDS_PER_BATCH = 50

    def __init__(
        self,
        webhook_url: str,
        tz_name: str = "Asia/Almaty",
        date_field: str = "CREATED_DATE",  # можно: CHANGED_DATE / CLOSED_DATE / DEADLINE
        http_timeout: int = 60,
        retries: int = 4,
        backoff: float = 0.6,
    ) -> None:
        self.webhook = webhook_url.rstrip("/")
        self.tz = ZoneInfo(tz_name)
        self.date_field = date_field
        self.http_timeout = http_timeout
        self.retries = retries
        self.backoff = backoff
        logger.info(f"Клиент BitrixSimple инициализирован. Поле даты для задач: {self.date_field}")

    # ---------- public ----------

    def get_active_users(self) -> List[Dict]:
        """Вернёт [{ID:int, NAME:str}, ...] всех активных сотрудников."""
        users: List[Dict] = []
        start = 0
        page_num = 1
        logger.info("Начинаем получение активных пользователей...")
        while True:
            logger.debug(f"Запрос пользователей: страница {page_num}, start={start}")
            resp = self._rest("user.search", {
                "FILTER": {"ACTIVE": "Y"},
                "SELECT": ["ID", "NAME", "LAST_NAME", "SECOND_NAME"],
                "start": start
            })
            chunk = resp.get("result", [])
            if not chunk:
                logger.info("Получена пустая страница, завершаем сбор пользователей.")
                break
            
            logger.debug(f"Получено {len(chunk)} пользователей на странице {page_num}.")
            for u in chunk:
                uid = int(u["ID"])
                name = " ".join([u.get("NAME") or "", u.get("LAST_NAME") or ""]).strip() or (u.get("NAME") or f"User {uid}")
                users.append({"ID": uid, "NAME": name})
            
            if "next" in resp:
                start = resp["next"]
                page_num += 1
            else:
                break
        logger.info(f"Всего найдено {len(users)} активных пользователей.")
        return users

    def get_weekly_tasks_batch(self, users: List[Dict]) -> Dict[str, Dict]:
        """
        Сбор задач батчем за текущую неделю по self.date_field.
        """
        dt_from, dt_to = self._week_bounds_iso()
        logger.info(f"Начинаем сбор задач за период с {dt_from} по {dt_to}.")
        
        out: Dict[str, Dict] = {
            str(u["ID"]): {"name": u["NAME"], "tasks": []} for u in users
        }
        remaining: Dict[int, int] = {int(u["ID"]): 0 for u in users}
        
        batch_num = 1
        while remaining:
            logger.info(f"Формирование пакета №{batch_num}. Осталось обработать пользователей: {len(remaining)}")
            cmd = {}
            batch_map = []  # [(alias, user_id)]
            
            users_in_batch = list(remaining.keys())[: self.MAX_CMDS_PER_BATCH]
            for uid in users_in_batch:
                start = remaining[uid]
                alias = f"u{uid}_s{start}"
                cmd[alias] = self._build_tasks_list_cmd(uid, dt_from, dt_to, start)
                batch_map.append((alias, uid))
            logger.debug(f"Пакет №{batch_num} содержит {len(batch_map)} команд.")

            data = self._batch(cmd)
            result_data = data.get("result", {})
            
            result_map = result_data.get("result", {})
            result_list = result_data.get("result", [])
            next_map = result_data.get("result_next", {})
            logger.debug(f"Пакет №{batch_num} выполнен. Начинаем разбор результатов.")

            for i, (alias, uid) in enumerate(batch_map):
                page_data = None
                if isinstance(result_map, dict):
                    page_data = result_map.get(alias, [])
                elif isinstance(result_list, list) and i < len(result_list):
                    page_data = result_list[i]

                tasks = []
                if isinstance(page_data, dict):
                    tasks = page_data.get("tasks", [])
                elif isinstance(page_data, list):
                    tasks = page_data
                
                if not tasks and isinstance(page_data, dict) and "result" in page_data:
                    tasks = page_data.get("result", {}).get("tasks", [])

                if tasks:
                    logger.debug(f"Для пользователя {uid} найдено {len(tasks)} задач.")
                
                simplified = []
                for t in tasks:
                    _id = t.get("ID") or t.get("id")
                    _status = t.get("STATUS") or t.get("status")
                    if _id is None:
                        continue
                    try:
                        _id = int(_id)
                    except (ValueError, TypeError):
                        logger.warning(f"Не удалось преобразовать ID '{_id}' задачи в число. Пропускаем.")
                        pass
                    simplified.append({"ID": _id, "STATUS": str(_status) if _status is not None else None})
                out[str(uid)]["tasks"].extend(simplified)

                next_val = None
                if isinstance(next_map, dict):
                    next_val = next_map.get(alias)
                elif isinstance(next_map, list) and i < len(next_map):
                    next_val = next_map[i]

                if next_val is not None:
                    logger.debug(f"Для пользователя {uid} есть следующая страница задач (start={next_val}).")
                    remaining[uid] = next_val
                else:
                    remaining.pop(uid, None)
            
            batch_num += 1

        logger.info("Сбор всех задач завершен.")
        return out

    # ---------- internals ----------

    def _week_bounds_iso(self) -> Tuple[str, str]:
        now = datetime.now(self.tz)
        monday = now - timedelta(days=now.weekday())
        monday = monday.replace(hour=0, minute=0, second=0, microsecond=0)
        sunday = monday + timedelta(days=6, hours=23, minutes=59, seconds=59)
        return monday.isoformat(), sunday.isoformat()

    def _build_tasks_list_cmd(self, user_id: int, dt_from: str, dt_to: str, start: int) -> str:
        params = {
            "filter[RESPONSIBLE_ID]": user_id,
            f"filter[>={self.date_field}]": dt_from,
            f"filter[<={self.date_field}]": dt_to,
            "order[ID]": "asc",
            "start": start,
            "select[]": ["ID", "STATUS"],
        }
        return "tasks.task.list?" + urlencode(params, doseq=True)

    def _rest(self, method: str, payload: dict) -> dict:
        url = f"{self.webhook}/{method}.json"
        logger.debug(f"Выполнение REST-запроса к {method}...")
        return self._post_json(url, payload)

    def _batch(self, commands: Dict[str, str], halt: int = 0) -> dict:
        url = f"{self.webhook}/batch.json"
        logger.debug(f"Выполнение batch-запроса с {len(commands)} командами...")
        payload = {"cmd": commands, "halt": halt}
        return self._post_json(url, payload)

    def _post_json(self, url: str, payload: dict) -> dict:
        last_err = None
        for attempt in range(1, self.retries + 1):
            try:
                logger.debug(f"POST-запрос (попытка {attempt}/{self.retries}) на URL: {url.split('?')[0]}")
                r = requests.post(url, json=payload, timeout=self.http_timeout)
                r.raise_for_status()
                data = r.json()
                if isinstance(data, dict) and "error" in data:
                    err_msg = f"Ошибка API Bitrix24: {data.get('error_description') or data.get('error')}"
                    logger.error(err_msg)
                    raise RuntimeError(err_msg)
                logger.debug("Запрос успешно выполнен.")
                return data
            except Exception as e:
                logger.warning(f"Ошибка при выполнении запроса (попытка {attempt}): {e}")
                last_err = e
                if attempt < self.retries:
                    sleep_time = self.backoff * (2 ** (attempt - 1))
                    logger.warning(f"Пауза на {sleep_time:.2f} сек. перед повторной попыткой.")
                    time.sleep(sleep_time)
                else:
                    logger.error("Превышено количество попыток выполнения запроса.")
                    raise
        raise last_err
