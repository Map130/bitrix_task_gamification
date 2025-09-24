"""
Асинхронный консьюмер для обработки событий из RabbitMQ.

Новая архитектура:
1.  При старте сервис запрашивает всю конфигурацию у `config-server`.
2.  Инициализирует `ConfigManager` для получения динамических обновлений.
3.  Подписывается на очереди и обрабатывает сообщения, используя полученную
    конфигурацию.
"""
import asyncio
import json
import logging
from typing import Any, Dict

import aio_pika
import aiohttp
from redis.asyncio import Redis

from shared.bootstrapper import fetch_config_from_server
from shared.config_manager import ConfigManager
from shared.utils import get_rabbitmq_connection, get_redis_connection, setup_logging

# --- Настройка ---
setup_logging()
logger = logging.getLogger(__name__)

# --- Глобальный объект конфигурации ---
CONFIG: Dict[str, Any] = {}

# --- Ключи Redis и другие константы ---
KEY_USERS_HASH = "cache:users:map"
KEY_TASKS_JSON = "cache:tasks:closed"
KEY_SEEN_PREFIX = "cache:users:seen:"
LOCK_TASKS_UPDATE = "lock:tasks_update"


# --- Логика обработки задач ---

async def get_task_details(session: aiohttp.ClientSession, task_id: str) -> Dict[str, Any] | None:
    """Асинхронно запрашивает детали задачи по её ID."""
    bitrix_webhook_url = CONFIG.get("BITRIX_WEBHOOK_URL")
    if not bitrix_webhook_url:
        logger.error("BITRIX_WEBHOOK_URL is not configured. Cannot fetch task details.")
        return None
    
    url = f"{bitrix_webhook_url.rstrip('/')}/tasks.task.get.json"
    payload = {"taskId": task_id, "select": ["ID", "RESPONSIBLE_ID", "STATUS"]}
    http_timeout = int(CONFIG.get("HTTP_TIMEOUT_SEC", 60))
    
    try:
        async with session.post(url, json=payload, timeout=http_timeout) as response:
            response.raise_for_status()
            data = await response.json()

            if "error" in data:
                err_msg = data.get('error_description', data['error'])
                logger.error(f"Bitrix API error for task {task_id}: {err_msg}")
                return None
            
            task_info = data.get("result", {}).get("task")
            if not task_info:
                logger.warning(f"Task {task_id} not found in Bitrix API response.")
                return None
                
            return task_info
    except aiohttp.ClientError as e:
        logger.error(f"HTTP request to Bitrix failed for task {task_id}: {e}")
    except Exception as e:
        logger.error(f"Unexpected error in get_task_details for task {task_id}: {e}", exc_info=True)
    return None


async def _update_tasks_in_redis(redis: Redis, user_id: str, task_id: int):
    """Атомарно обновляет список задач пользователя в Redis."""
    async with redis.lock(LOCK_TASKS_UPDATE, timeout=10):
        raw_data = await redis.get(KEY_TASKS_JSON)
        current_tasks = json.loads(raw_data) if raw_data else {}
        
        user_tasks = current_tasks.get(user_id, [])
        if task_id not in user_tasks:
            user_tasks.append(task_id)
            current_tasks[user_id] = user_tasks
            
            await redis.set(KEY_TASKS_JSON, json.dumps(current_tasks, ensure_ascii=False))
            logger.info(f"Appended task {task_id} for user {user_id}.")
        else:
            logger.info(f"Task {task_id} already exists for user {user_id}. No update needed.")


# --- Обработчики сообщений из RabbitMQ ---

async def handle_users_map(msg: aio_pika.IncomingMessage, redis: Redis):
    """Обрабатывает сообщение с картой пользователей."""
    async with msg.process():
        try:
            payload = json.loads(msg.body.decode())
            if not isinstance(payload, dict):
                logger.warning("Received non-dict payload in users queue, ignoring.")
                return

            mapping = {str(uid): str(name) for uid, name in payload.items()}
            if not mapping:
                logger.info("Received empty user map, nothing to update.")
                return

            await redis.hset(KEY_USERS_HASH, mapping=mapping)
            logger.info(f"Updated {len(mapping)} users in hash '{KEY_USERS_HASH}'.")

            inactive_days = int(CONFIG.get("INACTIVE_DAYS", 14))
            ttl = inactive_days * 24 * 3600
            pipe = redis.pipeline()
            for uid in mapping.keys():
                pipe.set(KEY_SEEN_PREFIX + uid, "1", ex=ttl)
            await pipe.execute()
            logger.info(f"Refreshed 'seen' status for {len(mapping)} users with TTL {ttl}s.")

        except json.JSONDecodeError:
            logger.error("Failed to decode JSON from users message body.")
        except Exception as e:
            logger.error(f"Error in handle_users_map: {e}", exc_info=True)


async def handle_task_snapshot(msg_data: Dict, redis: Redis):
    """Обрабатывает полный снимок задач."""
    await redis.set(KEY_TASKS_JSON, json.dumps(msg_data, ensure_ascii=False))
    logger.info(f"Stored weekly tasks snapshot in '{KEY_TASKS_JSON}'.")


async def handle_task_event(msg_data: Dict, redis: Redis):
    """Обрабатывает инкрементальное событие по задаче."""
    user_id = str(msg_data.get("user_id"))
    task_id = msg_data.get("task_id")

    if not user_id or not task_id:
        logger.warning(f"Invalid event message, missing 'user_id' or 'task_id': {msg_data}")
        return

    await _update_tasks_in_redis(redis, user_id, int(task_id))


async def handle_raw_task_event(msg_data: Dict, redis: Redis, session: aiohttp.ClientSession):
    """Обрабатывает 'сырое' событие, требующее обогащения."""
    task_id = str(msg_data.get("task_id"))
    if not task_id:
        logger.warning(f"Invalid raw_task_event, missing 'task_id': {msg_data}")
        return
    
    task_details = await get_task_details(session, task_id)
    if not task_details:
        return

    if str(task_details.get("status")) == '5':
        responsible_id = task_details.get("responsibleId")
        if not responsible_id:
            logger.warning(f"Task {task_id} is completed but has no responsible ID.")
            return
        
        await _update_tasks_in_redis(redis, str(responsible_id), int(task_id))
    else:
        logger.info(f"Task {task_id} status is '{task_details.get('status')}', not '5'. Ignoring.")


async def tasks_dispatcher(msg: aio_pika.IncomingMessage, redis: Redis, session: aiohttp.ClientSession):
    """Диспетчер сообщений о задачах."""
    async with msg.process():
        try:
            payload = json.loads(msg.body.decode())
            msg_type = payload.get("type")
            msg_data = payload.get("data")

            if not msg_type or not msg_data:
                logger.warning(f"Invalid message format: {payload}")
                return

            if msg_type == "snapshot":
                await handle_task_snapshot(msg_data, redis)
            elif msg_type == "event":
                await handle_task_event(msg_data, redis)
            elif msg_type == "raw_task_event":
                await handle_raw_task_event(msg_data, redis, session)
            else:
                logger.warning(f"Unknown message type '{msg_type}'. Ignoring.")

        except json.JSONDecodeError:
            logger.error("Failed to decode JSON from tasks message body.")
        except Exception as e:
            logger.error(f"Error in tasks_dispatcher: {e}", exc_info=True)


# --- Основная функция ---

async def main():
    """Главная функция запуска сервиса."""
    logger.info("Consumer service starting...")
    
    global CONFIG
    CONFIG = await fetch_config_from_server()

    rabbitmq_url = CONFIG.get("RABBITMQ_URL")
    redis_url = CONFIG.get("REDIS_URL")
    if not rabbitmq_url or not redis_url:
        raise RuntimeError("RABBITMQ_URL and REDIS_URL must be configured.")

    config_manager = ConfigManager(redis_url, rabbitmq_url)
    await config_manager.initialize()

    redis = await get_redis_connection(redis_url)
    
    async with aiohttp.ClientSession() as session:
        while True:
            try:
                connection = await get_rabbitmq_connection(rabbitmq_url)
                async with connection:
                    channel = await connection.channel()
                    prefetch = int(CONFIG.get("PREFETCH", 32))
                    await channel.set_qos(prefetch_count=prefetch)

                    queue_users = CONFIG.get("QUEUE_USERS", "bitrix.users.map")
                    queue_tasks = CONFIG.get("QUEUE_TASKS_EVENTS", "bitrix.tasks.events")
                    
                    users_queue = await channel.declare_queue(queue_users, durable=True)
                    tasks_queue = await channel.declare_queue(queue_tasks, durable=True)
                    logger.info("RabbitMQ queues declared. Waiting for messages.")
                    
                    await users_queue.consume(lambda msg: handle_users_map(msg, redis))
                    await tasks_queue.consume(lambda msg: tasks_dispatcher(msg, redis, session))

                    logger.info("Consumer is now running. Press Ctrl+C to stop.")
                    await asyncio.Future()

            except (aio_pika.exceptions.AMQPConnectionError, ConnectionError) as e:
                reconnect_delay = int(CONFIG.get("RECONNECT_DELAY_SEC", 10))
                logger.error(f"Connection error: {e}. Reconnecting in {reconnect_delay}s...")
                await asyncio.sleep(reconnect_delay)
            except Exception as e:
                reconnect_delay = int(CONFIG.get("RECONNECT_DELAY_SEC", 10))
                logger.critical(f"Critical error in main loop: {e}", exc_info=True)
                await asyncio.sleep(reconnect_delay)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Consumer service stopped by user.")
    except Exception as e:
        logger.critical(f"Consumer service failed to start: {e}", exc_info=True)
