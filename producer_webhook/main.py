"""
Асинхронный вебхук-продюсер для обработки событий из Bitrix24.

Новая архитектура:
1.  При старте сервис запрашивает всю конфигурацию у `config-server`.
2.  Инициализирует `ConfigManager` для получения динамических обновлений.
3.  Принимает вебхуки, валидирует их и отправляет в RabbitMQ, если
    `producer_mode` установлен в `webhook`.
"""
import asyncio
import json
import logging
from contextlib import asynccontextmanager
from typing import Any, Dict

import aio_pika
from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel, Field

from shared.bootstrapper import fetch_config_from_server
from shared.config_manager import ConfigManager
from shared.utils import setup_logging

# --- Настройка ---
setup_logging()
logger = logging.getLogger(__name__)

# --- Глобальные переменные ---
CONFIG: Dict[str, Any] = {}
app_state: Dict[str, Any] = {}


# --- Модели данных Pydantic ---
class WebhookAuth(BaseModel):
    application_token: str

class WebhookData(BaseModel):
    fields: dict = Field(alias="FIELDS")

class WebhookPayload(BaseModel):
    event: str
    data: WebhookData
    ts: int
    auth: WebhookAuth


# --- Управление жизненным циклом FastAPI ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Управляет ресурсами (конфигурация, RabbitMQ) во время работы приложения.
    """
    global CONFIG, app_state
    logger.info("Webhook service starting up...")

    # 1. Загрузка конфигурации
    CONFIG = await fetch_config_from_server()
    
    rabbitmq_url = CONFIG.get("RABBITMQ_URL")
    redis_url = CONFIG.get("REDIS_URL")
    if not rabbitmq_url or not redis_url:
        raise RuntimeError("RABBITMQ_URL and REDIS_URL must be configured.")

    # 2. Инициализация ConfigManager
    config_manager = ConfigManager(redis_url, rabbitmq_url)
    await config_manager.initialize()
    app_state["config_manager"] = config_manager
    
    # 3. Установка соединения с RabbitMQ
    try:
        connection = await aio_pika.connect_robust(rabbitmq_url)
        channel = await connection.channel()
        queue_tasks = CONFIG.get("QUEUE_TASKS_EVENTS", "bitrix.tasks.events")
        await channel.declare_queue(queue_tasks, durable=True)
        
        app_state["rabbitmq_connection"] = connection
        app_state["rabbitmq_channel"] = channel
        logger.info("Successfully connected to RabbitMQ and declared queue.")
    except Exception as e:
        logger.critical(f"Failed to connect to RabbitMQ on startup: {e}", exc_info=True)
        raise
        
    yield
    
    logger.info("Webhook service shutting down...")
    if "rabbitmq_connection" in app_state:
        await app_state["rabbitmq_connection"].close()
        logger.info("RabbitMQ connection closed.")


# --- Инициализация FastAPI ---
app = FastAPI(
    title="Bitrix Webhook Producer (Async)",
    description="Receives webhooks, validates them, and sends a raw event to RabbitMQ.",
    version="2.0.0",
    lifespan=lifespan
)


# --- Основная логика ---
async def publish_raw_event(task_id: int, event_ts: int):
    """Асинхронно публикует 'сырое' событие в RabbitMQ."""
    channel = app_state.get("rabbitmq_channel")
    if not channel or channel.is_closed:
        logger.error("RabbitMQ channel is not available. Cannot publish message.")
        return

    message_body = {
        "type": "raw_task_event",
        "data": {"task_id": task_id, "event_ts": event_ts}
    }
    
    try:
        queue_tasks = CONFIG.get("QUEUE_TASKS_EVENTS", "bitrix.tasks.events")
        await channel.default_exchange.publish(
            aio_pika.Message(
                body=json.dumps(message_body).encode('utf-8'),
                content_type="application/json",
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            ),
            routing_key=queue_tasks,
        )
        logger.info(f"Published raw_task_event for task_id: {task_id}")
    except Exception as e:
        logger.error(f"Failed to publish message for task {task_id}: {e}", exc_info=True)


@app.post("/webhook", status_code=202)
async def handle_webhook(request: Request):
    """
    Принимает вебхуки от Bitrix24, валидирует и асинхронно отправляет в очередь.
    """
    config_manager: ConfigManager = app_state["config_manager"]
    if config_manager.get_config("producer_mode") != "webhook":
        logger.info(f"Ignoring webhook, producer_mode is '{config_manager.get_config('producer_mode')}'.")
        return {"status": "ignored_by_mode"}

    try:
        payload_data = await request.json()
        payload = WebhookPayload.model_validate(payload_data)
    except Exception as e:
        logger.error(f"Failed to parse or validate incoming webhook payload: {e}")
        raise HTTPException(status_code=400, detail=f"Invalid payload: {e}")

    bitrix_token = CONFIG.get("BITRIX_OUTGOING_TOKEN")
    if not bitrix_token or payload.auth.application_token != bitrix_token:
        logger.warning("Invalid webhook token received.")
        raise HTTPException(status_code=403, detail="Forbidden: Invalid token.")

    if payload.event != 'ONTASKUPDATE':
        logger.info(f"Ignoring event '{payload.event}'.")
        return {"status": "event_ignored"}

    try:
        task_id = int(payload.data.fields['ID'])
    except (KeyError, ValueError):
        logger.warning(f"Webhook payload does not contain a valid task ID. Payload: {payload_data}")
        raise HTTPException(status_code=422, detail="Unprocessable Entity: Missing or invalid task ID.")

    asyncio.create_task(publish_raw_event(task_id, payload.ts))
    
    return {"status": "accepted"}


@app.get("/healthz", status_code=200)
async def healthz():
    """Проверка состояния сервиса."""
    channel = app_state.get("rabbitmq_channel")
    is_healthy = channel and not channel.is_closed
    if is_healthy:
        return {"status": "ok", "rabbitmq_connection": "ok"}
    else:
        raise HTTPException(status_code=503, detail="Service Unavailable: RabbitMQ connection is down.")
