import os
import json
import logging
import asyncio
from typing import List, Dict, Any
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from redis.asyncio import Redis
import aio_pika
from sqlalchemy import (
    Table, Column, String, Boolean, MetaData,
    insert, select, update, text
)
from sqlalchemy.ext.asyncio import create_async_engine

from encryption import encryption_service, InvalidToken

# --- Настройка ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Конфигурация из переменных окружения ---
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql+asyncpg://app_user:app_pass@postgres/app_db")
REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")
RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@rabbitmq:5672/%2F")

# Ключи и имена
CONFIG_CACHE_KEY = "config:cache"  # Ключ для кэша в Redis (единый JSON-объект)
CONFIG_UPDATES_EXCHANGE = "config.updates"

# --- SQLAlchemy объекты ---
engine = create_async_engine(DATABASE_URL, echo=False)
metadata = MetaData()
configurations_table = Table(
    'configurations', metadata,
    Column('key', String, primary_key=True),
    Column('value', String, nullable=False),
    Column('is_secret', Boolean, server_default=text('false'), nullable=False)
)

# --- Pydantic модели ---
class ConfigItemIn(BaseModel):
    key: str
    value: str
    is_secret: bool = False

class ConfigItemOut(BaseModel):
    key: str
    value: str
    is_secret: bool

# --- Глобальные переменные для управления состоянием ---
app_state: Dict[str, Any] = {}

# --- Функции жизненного цикла FastAPI ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    """Управляет ресурсами (БД, Redis, RabbitMQ) во время работы приложения."""
    logger.info("Config Server starting up...")
    
    if not encryption_service:
        logger.critical("Encryption service is not available. Shutting down.")
        raise RuntimeError("Cannot start without CONFIG_ENCRYPTION_KEY.")

    # Инициализация соединений
    app_state["redis"] = Redis.from_url(REDIS_URL, decode_responses=True)
    
    try:
        # Принудительное пересоздание таблицы для обеспечения корректной схемы
        async with engine.begin() as conn:
            await conn.run_sync(metadata.drop_all)
            await conn.run_sync(metadata.create_all)
        logger.info("Database tables dropped and recreated.")

        # Подключение к RabbitMQ
        rmq_connection = await aio_pika.connect_robust(RABBITMQ_URL)
        rmq_channel = await rmq_connection.channel()
        rmq_exchange = await rmq_channel.declare_exchange(
            CONFIG_UPDATES_EXCHANGE, aio_pika.ExchangeType.FANOUT, durable=True
        )
        app_state["rabbitmq_connection"] = rmq_connection
        app_state["rabbitmq_exchange"] = rmq_exchange
        logger.info("Connections to DB, Redis, and RabbitMQ established.")

        # Заполнение начальными данными и синхронизация с кэшем
        await seed_initial_data()
        await sync_db_to_redis_cache()

    except Exception as e:
        logger.critical(f"Startup failed: {e}", exc_info=True)
        raise

    yield  # Приложение работает

    logger.info("Config Server shutting down...")
    if "redis" in app_state:
        await app_state["redis"].close()
    if "rabbitmq_connection" in app_state:
        await app_state["rabbitmq_connection"].close()
    logger.info("Connections closed.")

# --- Инициализация FastAPI ---
app = FastAPI(
    title="Config Server",
    version="2.0.0",
    description="Manages application configuration with encryption, storing it in PostgreSQL, caching in Redis, and notifying services via RabbitMQ.",
    lifespan=lifespan
)

@app.get("/healthz", status_code=200, tags=["System"])
async def health_check():
    """Provides a simple health check endpoint."""
    return {"status": "ok"}


# --- API Endpoints ---

@app.get("/api/v1/configs/all", response_model=Dict[str, Any], tags=["Configuration"])
async def get_all_configs():
    """
    Retrieves all configurations from the Redis cache.
    This is the primary endpoint for services to fetch their configuration.
    """
    redis: Redis = app_state["redis"]
    cached_configs = await redis.get(CONFIG_CACHE_KEY)
    if not cached_configs:
        logger.warning("Config cache is empty. Services may not be able to start.")
        return {}
    return json.loads(cached_configs)

@app.post("/api/v1/configs", response_model=ConfigItemOut, status_code=201, tags=["Configuration"])
async def create_or_update_config(item: ConfigItemIn):
    """
    Creates a new configuration item or updates an existing one.
    Secrets are encrypted before being stored in the database.
    """
    async with engine.begin() as conn:
        # Check if the key already exists
        exists_result = await conn.execute(
            select(configurations_table).where(configurations_table.c.key == item.key)
        )
        existing_item = exists_result.first()

        value_to_store = item.value
        if item.is_secret:
            value_to_store = encryption_service.encrypt(item.value)

        if existing_item:
            # Update existing item
            stmt = (
                update(configurations_table)
                .where(configurations_table.c.key == item.key)
                .values(value=value_to_store, is_secret=item.is_secret)
            )
        else:
            # Insert new item
            stmt = insert(configurations_table).values(
                key=item.key, value=value_to_store, is_secret=item.is_secret
            )
        
        await conn.execute(stmt)

    # Update the cache and notify services
    await sync_db_to_redis_cache()
    await publish_update_notification()

    return ConfigItemOut(key=item.key, value=item.value, is_secret=item.is_secret)

@app.get("/api/v1/configs/{key}", response_model=ConfigItemOut, tags=["Configuration"])
async def get_config_by_key(key: str):
    """
    Retrieves a specific configuration item by its key.
    The value is decrypted if it is a secret.
    """
    redis: Redis = app_state["redis"]
    cached_configs_str = await redis.get(CONFIG_CACHE_KEY)
    if not cached_configs_str:
        raise HTTPException(status_code=404, detail="Configuration cache is empty.")
    
    cached_configs = json.loads(cached_configs_str)
    if key not in cached_configs:
        raise HTTPException(status_code=404, detail=f"Configuration key '{key}' not found.")

    # To determine if it's a secret, we need to check the DB
    async with engine.connect() as conn:
        result = await conn.execute(
            select(configurations_table.c.is_secret).where(configurations_table.c.key == key)
        )
        db_item = result.first()
        if not db_item:
             raise HTTPException(status_code=404, detail=f"Configuration key '{key}' not found in database.")

    return ConfigItemOut(
        key=key,
        value=cached_configs[key],
        is_secret=db_item.is_secret
    )


# --- Вспомогательные функции ---

async def seed_initial_data():
    """Seed the database with initial configuration data."""
    initial_configs = {
        "BITRIX_WEBHOOK_URL": ("your_bitrix_webhook_url_here", True),
        "RABBITMQ_QUEUE_TASKS": ("tasks_queue", False),
        "RABBITMQ_QUEUE_RESULTS": ("results_queue", False),
        "API_KEY": ("your_api_key_here", True),
        "RABBITMQ_URL": (os.getenv("RABBITMQ_URL", "amqp://guest:guest@rabbitmq:5672/%2F"), False),
        "REDIS_URL": (os.getenv("REDIS_URL", "redis://redis:6379/0"), False),
    }
    async with engine.begin() as conn:
        for key, (value, is_secret) in initial_configs.items():
            # Check if the key already exists
            exists_result = await conn.execute(
                select(configurations_table).where(configurations_table.c.key == key)
            )
            if exists_result.first() is None:
                encrypted_value = value
                if is_secret:
                    encrypted_value = encryption_service.encrypt(value)
                
                await conn.execute(
                    insert(configurations_table).values(
                        key=key, value=encrypted_value, is_secret=is_secret
                    )
                )
    logger.info("Initial data seeding complete.")

async def sync_db_to_redis_cache():
    """
    Загружает все конфиги из БД, дешифрует секреты и сохраняет всё
    в виде единого JSON-объекта в Redis.
    """
    redis: Redis = app_state["redis"]
    decrypted_configs = {}
    
    async with engine.connect() as conn:
        result = await conn.execute(select(configurations_table))
        for row in result.all():
            value = row.value
            if row.is_secret:
                try:
                    value = encryption_service.decrypt(row.value)
                except InvalidToken:
                    logger.error(f"Failed to decrypt secret for key '{row.key}'. Caching raw value.")
            decrypted_configs[row.key] = value

    if decrypted_configs:
        await redis.set(CONFIG_CACHE_KEY, json.dumps(decrypted_configs))
        logger.info(f"Synced {len(decrypted_configs)} config items from DB to Redis cache.")
    else:
        # Если конфигов нет, очищаем кэш
        await redis.delete(CONFIG_CACHE_KEY)
        logger.info("No configs in DB. Cleared Redis cache.")

async def publish_update_notification():
    """Отправляет оповещение об обновлении конфига в RabbitMQ."""
    exchange: aio_pika.abc.AbstractExchange = app_state["rabbitmq_exchange"]
    message = aio_pika.Message(body=json.dumps({"event": "config_updated"}).encode())
    await exchange.publish(message, routing_key="")
    logger.info(f"Published update notification to exchange '{CONFIG_UPDATES_EXCHANGE}'.")
