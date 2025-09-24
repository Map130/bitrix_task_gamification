"""
Асинхронный продюсер для сбора данных из Bitrix24 в режиме 'polling'.

Новая архитектура:
1.  При старте сервис запрашивает всю конфигурацию у `config-server`.
2.  Инициализирует `ConfigManager` для получения динамических обновлений.
3.  Периодически (или по триггеру обновления конфига) проверяет,
    установлен ли `producer_mode` в `polling`.
4.  Если да, выполняет полный цикл сбора данных и отправки в RabbitMQ.
"""
import asyncio
import json
import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Tuple
from urllib.parse import urlencode
from zoneinfo import ZoneInfo

import aio_pika
import aiohttp

from shared.bootstrapper import fetch_config_from_server
from shared.config_manager import ConfigManager
from shared.utils import setup_logging

# --- Настройка ---
setup_logging()
logger = logging.getLogger(__name__)

# --- Глобальный объект конфигурации ---
CONFIG: Dict[str, Any] = {}


# --- Утилиты для работы с API Bitrix24 ---

def get_week_bounds_iso(tz_name: str) -> Tuple[str, str]:
    """Возвращает ISO-строки для начала и конца текущей недели."""
    tz = ZoneInfo(tz_name)
    now = datetime.now(tz)
    monday = now - timedelta(days=now.weekday())
    monday = monday.replace(hour=0, minute=0, second=0, microsecond=0)
    sunday = monday + timedelta(days=6, hours=23, minutes=59, seconds=59)
    return monday.isoformat(), sunday.isoformat()


async def make_api_request(session: aiohttp.ClientSession, url: str, payload: dict) -> dict:
    """Выполняет POST-запрос к API Bitrix24 с логикой повторных попыток."""
    http_retries = int(CONFIG.get("HTTP_RETRIES", 4))
    http_timeout = int(CONFIG.get("HTTP_TIMEOUT_SEC", 60))
    backoff_sec = float(CONFIG.get("HTTP_BACKOFF_SEC", 0.6))
    
    last_exception = None
    for i in range(1, http_retries + 1):
        try:
            async with session.post(url, json=payload, timeout=http_timeout) as response:
                response.raise_for_status()
                data = await response.json()
                if isinstance(data, dict) and "error" in data:
                    error_message = data.get("error_description", data["error"])
                    logger.error(f"Bitrix API error: {error_message}")
                    raise aiohttp.ClientResponseError(
                        response.request_info, response.history,
                        status=response.status, message=error_message
                    )
                return data
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            last_exception = e
            logger.warning(f"Request failed (attempt {i}/{http_retries}): {e}")
            if i < http_retries:
                await asyncio.sleep(backoff_sec * (2 ** (i - 1)))
            else:
                logger.error(f"Request failed after {http_retries} retries.")
                raise last_exception


async def rest(session: aiohttp.ClientSession, method: str, payload: dict) -> dict:
    """Выполняет один REST-запрос."""
    bitrix_webhook_url = CONFIG.get("BITRIX_WEBHOOK_URL", "").rstrip("/")
    if not bitrix_webhook_url:
        raise ValueError("BITRIX_WEBHOOK_URL is not configured.")
    url = f"{bitrix_webhook_url}/{method}.json"
    return await make_api_request(session, url, payload)


async def batch(session: aiohttp.ClientSession, cmd: Dict[str, str], halt: int = 0) -> dict:
    """Выполняет пакетный запрос."""
    bitrix_webhook_url = CONFIG.get("BITRIX_WEBHOOK_URL", "").rstrip("/")
    if not bitrix_webhook_url:
        raise ValueError("BITRIX_WEBHOOK_URL is not configured.")
    url = f"{bitrix_webhook_url}/batch.json"
    return await make_api_request(session, url, {"cmd": cmd, "halt": halt})


# --- Основная логика сбора данных ---

async def get_users_map(session: aiohttp.ClientSession) -> Dict[int, str]:
    """Получает полный список активных пользователей."""
    users_map = {}
    start = 0
    while True:
        resp = await rest(session, "user.search", {
            "FILTER": {"ACTIVE": "Y"},
            "SELECT": ["ID", "NAME", "LAST_NAME"],
            "start": start
        })
        result = resp.get("result", [])
        if not result:
            break

        for u in result:
            user_id = int(u["ID"])
            name = " ".join(filter(None, [u.get("NAME"), u.get("LAST_NAME")])).strip() or f"User {user_id}"
            users_map[user_id] = name

        if "next" in resp:
            start = resp["next"]
        else:
            break
    logger.info(f"Fetched {len(users_map)} active users.")
    return users_map


def build_tasks_cmd(user_id: int, dt_from: str, dt_to: str, start: int = 0) -> str:
    """Формирует строку команды для запроса задач пользователя."""
    params = {
        "filter[RESPONSIBLE_ID]": user_id,
        "filter[STATUS]": 5,  # Завершенные задачи
        "filter[>=CLOSED_DATE]": dt_from,
        "filter[<=CLOSED_DATE]": dt_to,
        "order[ID]": "asc",
        "start": start,
        "select[]": ["ID"]
    }
    return "tasks.task.list?" + urlencode(params, doseq=True)


async def fetch_closed_tasks(session: aiohttp.ClientSession, user_ids: List[int]) -> Dict[int, List[int]]:
    """Асинхронно собирает ID завершенных задач для списка пользователей."""
    tz_name = CONFIG.get("TZ_NAME", "UTC")
    dt_from, dt_to = get_week_bounds_iso(tz_name)
    remaining_users = {uid: 0 for uid in user_ids}
    all_tasks = {uid: [] for uid in user_ids}
    max_cmds_in_batch = int(CONFIG.get("MAX_CMDS_IN_BATCH", 50))
    batch_pause_sec = float(CONFIG.get("BATCH_PAUSE_SEC", 0.2))

    while remaining_users:
        cmd_batch = {}
        mapping: List[Tuple[str, int]] = []

        for user_id, start_from in list(remaining_users.items())[:max_cmds_in_batch]:
            alias = f"u{user_id}_s{start_from}"
            cmd_batch[alias] = build_tasks_cmd(user_id, dt_from, dt_to, start_from)
            mapping.append((alias, user_id))

        data = await batch(session, cmd_batch)
        result = (data.get("result") or {}).get("result", {})
        next_pages = (data.get("result") or {}).get("result_next", {})

        for alias, user_id in mapping:
            page = result.get(alias, {})
            tasks = page.get("tasks", []) or page.get("result", []) or []
            task_ids = [int(t["ID"]) for t in tasks if t.get("ID")]
            all_tasks[user_id].extend(task_ids)

            if alias in next_pages:
                remaining_users[user_id] = next_pages[alias]
            else:
                del remaining_users[user_id]

        if remaining_users:
            logger.info(f"{len(remaining_users)} users still have more tasks to fetch. Pausing...")
            await asyncio.sleep(batch_pause_sec)

    total_tasks = sum(len(tasks) for tasks in all_tasks.values())
    logger.info(f"Fetched a total of {total_tasks} closed tasks for {len(user_ids)} users.")
    return all_tasks


# --- Функции для работы с RabbitMQ ---

async def publish_message(channel: aio_pika.Channel, queue_name: str, message: dict):
    """Публикует сообщение в указанную очередь."""
    await channel.default_exchange.publish(
        aio_pika.Message(
            body=json.dumps(message).encode('utf-8'),
            content_type="application/json",
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
        ),
        routing_key=queue_name,
    )
    logger.info(f"Published message to '{queue_name}'.")


# --- Основной цикл продюсера ---

async def run_polling_cycle(session: aiohttp.ClientSession, channel: aio_pika.Channel):
    """Выполняет один полный цикл сбора и отправки данных."""
    logger.info("Starting new polling cycle...")
    queue_users = CONFIG.get("QUEUE_USERS", "bitrix.users.map")
    queue_tasks = CONFIG.get("QUEUE_TASKS_EVENTS", "bitrix.tasks.events")
    
    try:
        users = await get_users_map(session)
        if users:
            await publish_message(channel, queue_users, users)

        user_ids = list(users.keys())
        if user_ids:
            tasks = await fetch_closed_tasks(session, user_ids)
            await publish_message(channel, queue_tasks, {"type": "snapshot", "data": tasks})

        logger.info("Polling cycle finished successfully.")

    except (aiohttp.ClientError, ValueError) as e:
        logger.error(f"A critical error occurred during the polling cycle: {e}. Skipping this cycle.")
    except Exception as e:
        logger.critical(f"An unexpected error occurred in polling cycle: {e}", exc_info=True)


async def main():
    """Главная функция запуска сервиса."""
    logger.info("Producer service starting...")
    
    global CONFIG
    CONFIG = await fetch_config_from_server()

    rabbitmq_url = CONFIG.get("RABBITMQ_URL")
    redis_url = CONFIG.get("REDIS_URL")
    if not rabbitmq_url or not redis_url:
        raise RuntimeError("RABBITMQ_URL and REDIS_URL must be configured.")

    config_manager = ConfigManager(redis_url, rabbitmq_url)
    await config_manager.initialize()

    async with aiohttp.ClientSession() as session:
        connection = await aio_pika.connect_robust(rabbitmq_url)
        async with connection:
            channel = await connection.channel()
            queue_users = CONFIG.get("QUEUE_USERS", "bitrix.users.map")
            queue_tasks = CONFIG.get("QUEUE_TASKS_EVENTS", "bitrix.tasks.events")
            await channel.declare_queue(queue_users, durable=True)
            await channel.declare_queue(queue_tasks, durable=True)
            logger.info("RabbitMQ queues declared.")

            while True:
                try:
                    if config_manager.get_config("producer_mode") == "polling":
                        await run_polling_cycle(session, channel)
                    else:
                        logger.info("Producer is in standby mode (producer_mode is not 'polling').")
                    
                    refetch_interval = int(CONFIG.get("REFETCH_INTERVAL_SEC", 300))
                    try:
                        await asyncio.wait_for(
                            config_manager.wait_for_update(),
                            timeout=refetch_interval
                        )
                        logger.info("Configuration updated, re-evaluating polling cycle.")
                        continue
                    except asyncio.TimeoutError:
                        pass

                except Exception as e:
                    logger.critical(f"Critical error in main loop: {e}", exc_info=True)
                    await asyncio.sleep(30)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Producer service stopped by user.")
    except Exception as e:
        logger.critical(f"Producer service failed to start: {e}", exc_info=True)
