import os
import json
import logging
import asyncio
from typing import List, Dict, Any

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from redis.asyncio import Redis
import aio_pika
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy import Table, Column, String, MetaData, insert, select, update

# --- Настройка логирования ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Загрузка конфигурации из переменных окружения ---
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql+asyncpg://app_user:app_pass@postgres/app_db")
REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")
RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@rabbitmq:5672/%2F")
CONFIG_REDIS_KEY = "app_configs"
CONFIG_UPDATE_EXCHANGE = "config.updates"
DEFAULT_PRODUCER_MODE = "polling"

# --- SQLAlchemy объекты ---
engine = create_async_engine(DATABASE_URL)
metadata = MetaData()
configurations_table = Table(
    'configurations', metadata,
    Column('key', String, primary_key=True),
    Column('value', String, nullable=False)
)

# --- FastAPI приложение ---
app = FastAPI(
    title="Config Server",
    version="1.0.0",
    description="Manages application configuration, storing it in PostgreSQL, caching in Redis, and notifying services via RabbitMQ."
)

# --- Pydantic модели ---
class ConfigItem(BaseModel):
    key: str
    value: str

# --- Глобальные переменные для соединений ---
redis_conn: Redis
rmq_channel: aio_pika.abc.AbstractChannel
rmq_exchange: aio_pika.abc.AbstractExchange

# --- Функции жизненного цикла (startup/shutdown) ---
@app.on_event("startup")
async def startup():
    global redis_conn, rmq_channel, rmq_exchange
    logging.info("Config Server starting up...")

    # Инициализация соединений
    redis_conn = Redis.from_url(REDIS_URL, decode_responses=True)
    
    try:
        # Проверка и создание таблицы в БД
        async with engine.begin() as conn:
            await conn.run_sync(metadata.create_all)

        # Подключение к RabbitMQ
        rmq_connection = await aio_pika.connect_robust(RABBITMQ_URL)
        rmq_channel = await rmq_connection.channel()
        rmq_exchange = await rmq_channel.declare_exchange(
            CONFIG_UPDATE_EXCHANGE, aio_pika.ExchangeType.FANOUT, durable=True
        )
        logging.info("Connections to DB, Redis, and RabbitMQ established.")

        # Загрузка конфига из БД в Redis и установка дефолтного значения
        await load_config_from_db_to_redis(set_default=True)

    except Exception as e:
        logging.error(f"Startup failed: {e}", exc_info=True)
        # В реальном приложении здесь может быть более сложная логика отката или падения
        raise

@app.on_event("shutdown")
async def shutdown():
    logging.info("Config Server shutting down...")
    if 'redis_conn' in globals() and redis_conn:
        await redis_conn.close()
    # aio_pika закроет соединения автоматически
    logging.info("Connections closed.")


# --- Вспомогательные функции ---
async def load_config_from_db_to_redis(set_default: bool = False):
    """Загружает все конфиги из БД в Redis. Если set_default=True, устанавливает producer_mode, если его нет."""
    async with engine.connect() as conn:
        result = await conn.execute(select(configurations_table))
        configs = {row.key: row.value for row in result.all()}

        # Установка значения по умолчанию, если его нет в базе
        if set_default and 'producer_mode' not in configs:
            configs['producer_mode'] = DEFAULT_PRODUCER_MODE
            stmt = insert(configurations_table).values(key='producer_mode', value=DEFAULT_PRODUCER_MODE)
            await conn.execute(stmt)
            await conn.commit()
            logging.info(f"Default 'producer_mode' set to '{DEFAULT_PRODUCER_MODE}' in DB.")

    if configs:
        await redis_conn.hset(CONFIG_REDIS_KEY, mapping=configs)
        logging.info(f"Loaded {len(configs)} config items from DB to Redis cache '{CONFIG_REDIS_KEY}'.")
    else:
        logging.info("No configs found in DB.")

async def publish_update_notification():
    """Отправляет оповещение об обновлении конфига в RabbitMQ."""
    message = aio_pika.Message(body=json.dumps({"event": "config_updated"}).encode())
    await rmq_exchange.publish(message, routing_key="")
    logging.info(f"Published update notification to exchange '{CONFIG_UPDATE_EXCHANGE}'.")


# --- API Эндпоинты ---
@app.get("/healthz")
async def healthz():
    return {"status": "ok"}

@app.get("/configs", response_model=List[ConfigItem])
async def get_all_configs():
    """Возвращает все конфигурации из кэша Redis."""
    if not redis_conn:
        raise HTTPException(status_code=503, detail="Redis connection not available.")
    
    configs = await redis_conn.hgetall(CONFIG_REDIS_KEY)
    return [{"key": k, "value": v} for k, v in configs.items()]

@app.post("/configs", response_model=ConfigItem)
async def update_config(item: ConfigItem):
    """Обновляет или создает один параметр конфигурации."""
    async with engine.connect() as conn:
        # Проверяем, существует ли ключ
        stmt_select = select(configurations_table).where(configurations_table.c.key == item.key)
        existing = await conn.execute(stmt_select)
        
        if existing.first():
            stmt_update = update(configurations_table).where(configurations_table.c.key == item.key).values(value=item.value)
            await conn.execute(stmt_update)
        else:
            stmt_insert = insert(configurations_table).values(key=item.key, value=item.value)
            await conn.execute(stmt_insert)
        
        await conn.commit()

    # Обновляем кэш в Redis
    await redis_conn.hset(CONFIG_REDIS_KEY, item.key, item.value)
    logging.info(f"Updated config '{item.key}' to '{item.value}' in DB and Redis.")

    # Отправляем оповещение
    await publish_update_notification()

    return item
