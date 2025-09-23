import asyncio, json, os, logging
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import aio_pika
from redis.asyncio import Redis
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

load_dotenv()

RABBITMQ_URL = os.getenv("RABBITMQ_URL","amqp://guest:guest@rabbitmq:5672/%2F")
REDIS_URL    = os.getenv("REDIS_URL","redis://redis:6379/0")
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import aio_pika
from redis.asyncio import Redis
from dotenv import load_dotenv

load_dotenv()

RABBITMQ_URL = os.getenv("RABBITMQ_URL","amqp://guest:guest@rabbitmq:5672/")
REDIS_URL    = os.getenv("REDIS_URL","redis://redis:6379/0")
QUEUE_USERS  = os.getenv("QUEUE_USERS","bitrix.users.map")
QUEUE_TASKS  = os.getenv("QUEUE_TASKS","bitrix.tasks.closed.week")
PREFETCH     = int(os.getenv("PREFETCH","32"))

TZ_NAME      = os.getenv("TZ_NAME","Asia/Almaty")
TZ           = ZoneInfo(TZ_NAME)
WEEKLY_RESET_DOW  = int(os.getenv("WEEKLY_RESET_DOW","1"))
WEEKLY_RESET_TIME = os.getenv("WEEKLY_RESET_TIME","03:00")
INACTIVE_DAYS     = int(os.getenv("INACTIVE_DAYS","14"))

KEY_USERS_HASH  = "cache:users:map"
KEY_TASKS_JSON  = "cache:tasks:closed"
KEY_SEEN_PREFIX = "cache:users:seen:"

def _next_reset(now: datetime) -> datetime:
    hh, mm = map(int, WEEKLY_RESET_TIME.split(":"))
    delta = (WEEKLY_RESET_DOW - now.weekday()) % 7
    cand = (now + timedelta(days=delta)).replace(hour=hh, minute=mm, second=0, microsecond=0)
    return cand if cand > now else cand + timedelta(days=7)

def _ttl_until_next(now: datetime) -> int:
    return max(1, int((_next_reset(now) - now).total_seconds()))

async def _cleanup_users_hash(redis: Redis):
    cursor = 0; to_del = []
    while True:
        cursor, chunk = await redis.hscan(KEY_USERS_HASH, cursor=cursor, count=500)
        for uid, _ in chunk.items():
            if not await redis.exists(KEY_SEEN_PREFIX + uid):
                to_del.append(uid)
        if cursor == 0: break
    if to_del:
        deleted_count = await redis.hdel(KEY_USERS_HASH, *to_del)
        logging.info(f"Cleaned up {deleted_count} inactive users from hash '{KEY_USERS_HASH}'.")
    else:
        logging.info("No inactive users to clean up.")

async def handle_users(msg: aio_pika.IncomingMessage, redis: Redis):
    async with msg.process():
        try:
            payload = json.loads(msg.body.decode())
            if not isinstance(payload, dict):
                logging.warning("Received non-dict payload in users queue, ignoring.")
                return

            mapping = {str(uid): str(name) for uid, name in payload.items()}
            if not mapping:
                logging.info("Received empty user map, nothing to update.")
                return

            await redis.hset(KEY_USERS_HASH, mapping=mapping)
            logging.info(f"Updated {len(mapping)} users in hash '{KEY_USERS_HASH}'.")

            ttl = INACTIVE_DAYS * 24 * 3600
            pipe = redis.pipeline()
            for uid in mapping.keys():
                pipe.set(KEY_SEEN_PREFIX + uid, "1", ex=ttl)
            await pipe.execute()
            logging.info(f"Refreshed seen status for {len(mapping)} users with TTL {ttl}s.")

            await _cleanup_users_hash(redis)
        except json.JSONDecodeError:
            logging.error("Failed to decode JSON from users message body.")
        except Exception as e:
            logging.error(f"An error occurred in handle_users: {e}", exc_info=True)

async def handle_tasks(msg: aio_pika.IncomingMessage, redis: Redis):
    async with msg.process():
        try:
            now = datetime.now(TZ)
            ttl = _ttl_until_next(now)
            await redis.set(KEY_TASKS_JSON, msg.body, ex=ttl)
            logging.info(f"Stored weekly tasks in '{KEY_TASKS_JSON}' with TTL {ttl}s.")
        except Exception as e:
            logging.error(f"An error occurred in handle_tasks: {e}", exc_info=True)

async def main():
    logging.info("Consumer starting...")
    redis = Redis.from_url(REDIS_URL, decode_responses=True)
    logging.info("Redis client initialized.")
    conn = await aio_pika.connect_robust(RABBITMQ_URL)
    logging.info("RabbitMQ connection established.")
    ch = await conn.channel()
    await ch.set_qos(prefetch_count=PREFETCH)
    logging.info(f"QoS prefetch count set to {PREFETCH}.")

    q_users = await ch.declare_queue(QUEUE_USERS, durable=True)
    q_tasks = await ch.declare_queue(QUEUE_TASKS, durable=True)
    logging.info(f"Declared queues: '{QUEUE_USERS}' and '{QUEUE_TASKS}'.")

    await q_users.consume(lambda m: handle_users(m, redis))
    await q_tasks.consume(lambda m: handle_tasks(m, redis))
    logging.info("Started consuming messages. Waiting for data...")

    try:
        await asyncio.Future()
    except asyncio.CancelledError:
        logging.info("Consumer shutting down.")
    finally:
        if conn: await conn.close(); logging.info("RabbitMQ connection closed.")
        if redis: await redis.aclose(); logging.info("Redis connection closed.")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Consumer stopped by user.")
