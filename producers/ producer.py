import aiohttp
from collections import defaultdict
import asyncio
import math
from typing import Dict, Iterable, List, Optional, Tuple
from urllib.parse import urlencode, quote



class Producer:
    def __init__(self, mode:str, webhook_url:str=None, api_key:str=None, rabbitmq_url:str=None, redis_url:str=None, task_queue:str="tasks"):
        self.mode = mode
        self.webhook_url = webhook_url
        self.api_key = api_key
        self.rabbitmq_url = rabbitmq_url
        self.redis_url = redis_url
        self.task_queue = task_queue

    async def get_user_map(self):
        """
        Retrieves the user ID to user data mapping from the Redis cache.

        Raises:
            ValueError: If redis_url is not configured.
            Exception: "REDIS_USER_MAP_CACHE_IS_EMPTY" if the user map cache is empty.

        Returns:
            dict: A dictionary containing the user map.
        """
        if not self.redis_url:
            raise ValueError("REDIS_URL_NOT_CONFIGURED")

        try:
            import redis.asyncio as redis
        except ImportError:
            raise ImportError("REDIS_ASYNCIO_LIBRARY_NOT_INSTALLED")

        r = redis.from_url(self.redis_url, decode_responses=True)
        user_map = await r.hgetall("user_map")
        await r.close()

        if not user_map:
            raise ValueError("REDIS_USER_MAP_CACHE_IS_EMPTY")

        return user_map
    

class PollingProducer(Producer):
    """
    Две стратегии:
      1) by_total:   один запрос на пользователя -> читаем только `total` (STATUS=5, >=CLOSED_DATE)
      2) by_scanning: проходим все закрытые задачи пачками (select=ID, RESPONSIBLE_ID, CLOSED_DATE)
    Оба варианта уважают лимит 50 и минимизируют поля.
    """

    def __init__(self, webhook_url: str, mode: str = "polling"):
        # webhook_url вида: https://<portal>.bitrix24.ru/rest/<user>/<webhook>  (без метода на конце)
        self.webhook_url = webhook_url.rstrip("/")
        self.mode = mode

    # ===== ВНУТРЕННЯЯ ТРАНСПОРТНАЯ ОБВЯЗКА  ==================================

    async def _request_json(
        self,
        session: aiohttp.ClientSession,
        method: str,                          # например "tasks.task.list" или "batch"
        params: Dict,
        http_method: str = "POST",
        retries: int = 3,
        base_backoff: float = 0.6,
        timeout_s: float = 30.0,
    ) -> Dict:
        """
        Универсальный вызов REST Bitrix24 с ретраями на 429/5xx.
        """
        url = f"{self.webhook_url}/{method}.json"
        last_exc = None

        for attempt in range(1, retries + 1):
            try:
                kwargs = {"timeout": aiohttp.ClientTimeout(total=timeout_s)}
                if http_method.upper() == "GET":
                    async with session.get(url, params=params, **kwargs) as resp:
                        txt = await resp.text()
                        resp.raise_for_status()
                        return await resp.json()
                else:
                    # В Bitrix REST допустимы application/x-www-form-urlencoded
                    async with session.post(url, data=params, **kwargs) as resp:
                        txt = await resp.text()
                        resp.raise_for_status()
                        return await resp.json()

            except aiohttp.ClientResponseError as e:
                # 429/5xx — бэкофф с ретраями
                last_exc = e
                if e.status in (429, 500, 502, 503, 504) and attempt < retries:
                    await asyncio.sleep(base_backoff * (2 ** (attempt - 1)))
                    continue
                raise
            except Exception as e:
                last_exc = e
                if attempt < retries:
                    await asyncio.sleep(base_backoff * (2 ** (attempt - 1)))
                    continue
                raise

        if last_exc:
            raise last_exc

    # ======= 1) СТРАТЕГИЯ "ПО ПОЛЬЗОВАТЕЛЯМ ЧЕРЕЗ total" =====================

    async def get_totals_by_user_via_total(
        self,
        responsible_ids: Iterable[int],
        from_iso: str,
        to_iso: Optional[str] = None,
        *,
        batch_size: int = 50,                 # максимум 50 команд в batch
        delay_between_batches_ms: int = 200,  # пауза между batch (мс)
        retries: int = 3,
        base_backoff: float = 0.6,
        timeout_s: float = 30.0,
    ) -> Dict[str, int]:
        """
        Для каждого RESPONSIBLE_ID делает tasks.task.list с фильтром:
          STATUS=5, >=CLOSED_DATE=from_iso (и <=CLOSED_DATE при наличии to_iso)
        Снимает только total с первой страницы (без пагинации).
        Возвращает: { "<user_id>": total, ... }
        """
        # Подготовка батч-команд
        # Формат batch: cmd[alias] = 'tasks.task.list?filter[...]&select[]=ID'
        # Нам нужна минимальная выборка, т.к. читаем только total.
        totals: Dict[str, int] = {}

        async with aiohttp.ClientSession() as session:
            uids = list(map(int, responsible_ids))
            for i in range(0, len(uids), batch_size):
                chunk = uids[i:i + batch_size]
                cmd = {}
                for idx, uid in enumerate(chunk):
                    q = {
                        "filter[STATUS]": 5,
                        "filter[>=CLOSED_DATE]": from_iso,
                        "select[]": "ID",  # минимально
                    }
                    if to_iso:
                        q["filter[<=CLOSED_DATE]"] = to_iso
                    q["filter[RESPONSIBLE_ID]"] = uid
                    # start не указываем — первая страница (total уже присутствует)
                    query_str = "tasks.task.list?" + urlencode(q, doseq=True)
                    cmd_key = f"u{uid}"
                    cmd[cmd_key] = query_str

                params = {"cmd": cmd}
                data = await self._request_json(
                    session, "batch", params,
                    http_method="POST", retries=retries,
                    base_backoff=base_backoff, timeout_s=timeout_s,
                )

                # Парс батча
                # data['result']['result'] — словарь по ключам cmd; в каждом: {'tasks': [...], 'total': int, 'next': ...?}
                batch_result = data.get("result", {})
                per_cmd = batch_result.get("result", {}) or {}
                for k, v in per_cmd.items():
                    # v: {'tasks':[...], 'total': int, ...}
                    total = 0
                    if isinstance(v, dict):
                        total = int(v.get("total", 0))
                    # k = "u<uid>"
                    uid = k[1:]
                    totals[uid] = total

                # Пауза между батчами
                if i + batch_size < len(uids) and delay_between_batches_ms > 0:
                    await asyncio.sleep(delay_between_batches_ms / 1000.0)

        return totals

    # ======= 2) СТРАТЕГИЯ "СКАНИРОВАНИЕ ЗАДАЧ ПАКЕТАМИ (batch страниц)" ======

    async def get_totals_by_scanning_tasks(
        self,
        from_iso: str,
        to_iso: Optional[str] = None,
        *,
        pages_per_batch: int = 10,            # в одном batch сразу N страниц (N*50 задач максимум)
        delay_between_batches_ms: int = 200,
        retries: int = 3,
        base_backoff: float = 0.6,
        timeout_s: float = 30.0,
        include_task_ids: bool = False,       # если True — вернёт также id задач по пользователю
    ) -> Dict[str, int] | Tuple[Dict[str, int], Dict[str, List[str]]]:
        """
        Идём по всем задачам, закрытым с даты from_iso (и до to_iso, если задан).
        На каждой итерации батчим параллельно несколько страниц по 'start':
           start = base, base+50, base+100, ... (pages_per_batch штук)
        Останавливаемся, когда в сумме пришло 0 задач.

        Возвращает:
          - по умолчанию: { "<user_id>": total }
          - если include_task_ids=True: (totals, tasks_ids_by_user)
        """
        assert pages_per_batch >= 1 and pages_per_batch <= 50, "pages_per_batch должен быть в [1..50]"

        counts: Dict[str, int] = defaultdict(int)
        tasks_ids_by_user: Dict[str, List[str]] = defaultdict(list) if include_task_ids else {}

        base_start = 0  # смещение первой страницы в пачке

        async with aiohttp.ClientSession() as session:
            while True:
                # Собираем батч из pages_per_batch страниц
                cmd = {}
                for p in range(pages_per_batch):
                    start_offset = base_start + p * 50
                    q = {
                        "filter[STATUS]": 5,
                        "filter[>=CLOSED_DATE]": from_iso,
                        "order[CLOSED_DATE]": "ASC",
                        "select[]": "ID",
                        "select[]": "RESPONSIBLE_ID",
                        "select[]": "CLOSED_DATE",
                        "start": start_offset,
                    }
                    if to_iso:
                        q["filter[<=CLOSED_DATE]"] = to_iso
                    cmd[f"p{start_offset}"] = "tasks.task.list?" + urlencode(q, doseq=True)

                data = await self._request_json(
                    session, "batch", {"cmd": cmd},
                    http_method="POST", retries=retries,
                    base_backoff=base_backoff, timeout_s=timeout_s,
                )

                batch_result = data.get("result", {})
                per_cmd = batch_result.get("result", {}) or {}
                total_tasks_this_batch = 0

                # Обрабатываем результаты всех страниц из батча
                for k, v in per_cmd.items():
                    if not isinstance(v, dict):
                        continue
                    tasks = v.get("tasks", []) or []
                    total_tasks_this_batch += len(tasks)

                    for t in tasks:
                        uid = str(t.get("responsibleId"))
                        if not uid or uid == "None":
                            continue
                        counts[uid] += 1
                        if include_task_ids:
                            tid = str(t.get("id"))
                            if tid:
                                tasks_ids_by_user[uid].append(tid)

                # Если ничего не пришло — завершаем
                if total_tasks_this_batch == 0:
                    break

                # Сдвигаем окно на pages_per_batch страниц вперёд
                base_start += pages_per_batch * 50

                # Пауза между батчами
                if delay_between_batches_ms > 0:
                    await asyncio.sleep(delay_between_batches_ms / 1000.0)

        if include_task_ids:
            return dict(counts), {k: v for k, v in tasks_ids_by_user.items()}
        return dict(counts)

    # ===== ДОПОЛНИТЕЛЬНО: БЫСТРЫЙ "ЗАМЕР" ДЛЯ ВЫБОРА СТРАТЕГИИ ===============

    async def get_overall_total_completed(
        self,
        from_iso: str,
        to_iso: Optional[str] = None,
        *,
        retries: int = 3,
        base_backoff: float = 0.6,
        timeout_s: float = 30.0,
    ) -> int:
        """
        Быстрый вызов tasks.task.list (первая страница) -> берём общий total
        по STATUS=5 и >=CLOSED_DATE (и <=CLOSED_DATE при наличии).
        """
        async with aiohttp.ClientSession() as session:
            q = {
                "filter[STATUS]": 5,
                "filter[>=CLOSED_DATE]": from_iso,
                "select[]": "ID",
            }
            if to_iso:
                q["filter[<=CLOSED_DATE]"] = to_iso
            data = await self._request_json(
                session, "tasks.task.list", q,
                http_method="GET", retries=retries,
                base_backoff=base_backoff, timeout_s=timeout_s,
            )
            return int((data.get("result") or {}).get("total", 0))

    async def get_active_users_total(
        self,
        *,
        retries: int = 3,
        base_backoff: float = 0.6,
        timeout_s: float = 30.0,
    ) -> int:
        """
        Примерно оценить число активных пользователей (для выбора стратегии).
        Берём user.get с фильтром ACTIVE=true -> total.
        """
        async with aiohttp.ClientSession() as session:
            q = {"FILTER[ACTIVE]": "true", "SELECT[]": "ID"}
            data = await self._request_json(
                session, "user.get", q,
                http_method="GET", retries=retries,
                base_backoff=base_backoff, timeout_s=timeout_s,
            )
            return int((data.get("result") or {}).get("total", 0))
