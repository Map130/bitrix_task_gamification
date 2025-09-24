"""
Общие утилиты для микросервисов.
"""
import logging
import asyncio
import aio_pika
from redis.asyncio import Redis

def setup_logging():
    """Настраивает базовую конфигурацию логирования для сервиса."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

async def get_rabbitmq_connection(rabbitmq_url: str, retries: int = 5, delay: int = 5) -> aio_pika.Connection:
    """
    Устанавливает асинхронное соединение с RabbitMQ с логикой повторных попыток.
    """
    logger = logging.getLogger(__name__)
    for attempt in range(retries):
        try:
            connection = await aio_pika.connect_robust(rabbitmq_url)
            logger.info("Successfully connected to RabbitMQ.")
            return connection
        except (aio_pika.exceptions.AMQPConnectionError, ConnectionError) as e:
            logger.warning(
                "RabbitMQ connection failed (attempt %d/%d): %s. Retrying in %ds...",
                attempt + 1, retries, e, delay
            )
            await asyncio.sleep(delay)
    raise ConnectionError(f"Failed to connect to RabbitMQ after {retries} attempts.")

async def get_redis_connection(redis_url: str, retries: int = 5, delay: int = 5) -> Redis:
    """
    Устанавливает асинхронное соединение с Redis с логикой повторных попыток.
    """
    logger = logging.getLogger(__name__)
    for attempt in range(retries):
        try:
            redis = Redis.from_url(redis_url, decode_responses=True)
            await redis.ping()
            logger.info("Successfully connected to Redis.")
            return redis
        except Exception as e:
            logger.warning(
                "Redis connection failed (attempt %d/%d): %s. Retrying in %ds...",
                attempt + 1, retries, e, delay
            )
            await asyncio.sleep(delay)
    raise ConnectionError(f"Failed to connect to Redis after {retries} attempts.")
