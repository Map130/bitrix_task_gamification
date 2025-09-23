import os
import logging
import redis
from redis.asyncio import Redis as AsyncRedis

# Ключи и URL из переменных окружения
REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")
CONFIG_REDIS_KEY = "app_configs"

def get_config_value(key: str, default: str | None = None) -> str | None:
    """Получает значение конкретного ключа конфигурации из Redis (СИНХРОННАЯ ВЕРСИЯ)."""
    try:
        redis_client = redis.from_url(REDIS_URL, decode_responses=True)
        value = redis_client.hget(CONFIG_REDIS_KEY, key)
        redis_client.close()
        return value if value else default
    except redis.exceptions.ConnectionError as e:
        logging.error(f"Could not connect to Redis to get config: {e}")
        return default

async def get_config_value_async(key: str, default: str | None = None) -> str | None:
    """Получает значение конкретного ключа конфигурации из Redis (АСИНХРОННАЯ ВЕРСИЯ)."""
    try:
        redis_client = AsyncRedis.from_url(REDIS_URL, decode_responses=True)
        value = await redis_client.hget(CONFIG_REDIS_KEY, key)
        await redis_client.close()
        return value if value else default
    except redis.exceptions.ConnectionError as e:
        logging.error(f"Could not connect to Redis to get config: {e}")
        return default
