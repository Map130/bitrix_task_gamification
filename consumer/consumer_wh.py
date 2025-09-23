import asyncio
import json
import os
import logging
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import aio_pika
from redis.asyncio import Redis
from dotenv import load_dotenv

# --- Настройка логирования ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Загрузка конфигурации ---
load_dotenv()

RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@rabbitmq:5672/%2F")
REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")
QUEUE_TASK_EVENTS = os.getenv("QUEUE_TASK_EVENTS", "bitrix.tasks.events")
PREFETCH = int(os.getenv("PREFETCH", "32"))

# Настройки для сброса кэша
TZ_NAME = os.getenv("TZ_NAME", "Asia/Almaty")
TZ = ZoneInfo(TZ_NAME)
WEEKLY_RESET_DOW = int(os.getenv("WEEKLY_RESET_DOW", "1"))  # 0=Mon..6=Sun
WEEKLY_RESET_TIME = os.getenv("WEEKLY_RESET_TIME", "03:00")

# Ключи Redis
KEY_TASKS_WEEKLY = "gamification:tasks:weekly"

# --- Утилиты для времени ---
def _next_reset(now: datetime) -> datetime:
    """Вычисляет следующее время сброса кэша."""
    hh, mm = map(int, WEEKLY_RESET_TIME.split(":"))
    delta = (WEEKLY_RESET_DOW - now.weekday() + 7) % 7
    cand = (now + timedelta(days=delta)).replace(hour=hh, minute=mm, second=0, microsecond=0)
    return cand if cand > now else cand + timedelta(days=7)

def _ttl_until_next(now: datetime) -> int:
    """Возвращает TTL в секундах до следующего сброса."""
    return max(1, int((_next_reset(now) - now).total_seconds()))

# --- Логика обработки сообщений ---
async def handle_task_event(msg: aio_pika.IncomingMessage, redis: Redis):
    """Обрабатывает событие о новой завершенной задаче."""
    async with msg.process():
        try:
            payload = json.loads(msg.body.decode())
            user_id = str(payload.get("user_id"))
            task_id = str(payload.get("task_id"))

            if not user_id or not task_id:
                logging.warning(f"Invalid payload in task event: {payload}")
                return

            now = datetime.now(TZ)
            ttl = _ttl_until_next(now)

            # Используем HINCRBY для атомарного увеличения счетчика задач пользователя
            # и устанавливаем TTL для всего хэша при первом обновлении
            pipe = redis.pipeline()
            pipe.hincrby(KEY_TASKS_WEEKLY, user_id, 1)
            pipe.expire(KEY_TASKS_WEEKLY, ttl)
            await pipe.execute()

            logging.info(f"Incremented task count for user {user_id}. New TTL for '{KEY_TASKS_WEEKLY}' is {ttl}s.")

        except json.JSONDecodeError:
            logging.error("Failed to decode JSON from task event message.")
        except Exception as e:
            logging.error(f"An error occurred in handle_task_event: {e}", exc_info=True)

# --- Основной цикл ---
async def main():
    logging.info("Webhook Consumer starting...")
    redis = Redis.from_url(REDIS_URL, decode_responses=True)
    
    try:
        connection = await aio_pika.connect_robust(RABBITMQ_URL)
        async with connection:
            channel = await connection.channel()
            await channel.set_qos(prefetch_count=PREFETCH)
            
            queue = await channel.declare_queue(QUEUE_TASK_EVENTS, durable=True)
            
            logging.info(f"Waiting for messages on queue '{QUEUE_TASK_EVENTS}'.")
            
            async with queue.iterator() as queue_iter:
                async for message in queue_iter:
                    await handle_task_event(message, redis)
    except Exception as e:
        logging.error(f"An unhandled exception occurred in consumer main loop: {e}", exc_info=True)
    finally:
        if redis:
            await redis.aclose()
        logging.info("Webhook Consumer stopped.")

if __name__ == "__main__":
    asyncio.run(main())
