import json
import logging
from typing import Any, Dict

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from redis.asyncio import Redis

from shared.bootstrapper import fetch_config_from_server
from shared.utils import get_redis_connection, setup_logging

# --- Настройка ---
setup_logging()
logger = logging.getLogger(__name__)

# --- Глобальные объекты ---
CONFIG: Dict[str, Any] = {}
redis: Redis | None = None

app = FastAPI(title="Bitrix Weekly API", version="1.0.0")

# Настройка CORS должна происходить при инициализации, а не в startup
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Временно разрешаем все, пока не решим проблему с динамической конфигурацией
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
async def startup():
    """
    При старте:
    1. Загружает конфигурацию с config-server.
    2. Настраивает CORS.
    3. Инициализирует подключение к Redis.
    """
    global CONFIG, redis, app

    logger.info("API starting up...")
    CONFIG = await fetch_config_from_server()

    # Настройка CORS после получения конфигурации
    # cors_origins = CONFIG.get("API_CORS", "*")
    # app.add_middleware(
    #     CORSMiddleware,
    #     allow_origins=[cors_origins] if cors_origins != "*" else ["*"],
    #     allow_credentials=True,
    #     allow_methods=["*"],
    #     allow_headers=["*"],
    # )
    # logger.info(f"CORS configured for origins: {cors_origins}")

    redis_url = CONFIG.get("REDIS_URL")
    if not redis_url:
        raise RuntimeError("REDIS_URL not found in configuration.")
    
    redis = await get_redis_connection(redis_url)
    logger.info("Redis client initialized.")


@app.on_event("shutdown")
async def shutdown():
    """При завершении работы закрывает соединение с Redis."""
    logger.info("API shutting down...")
    if redis:
        await redis.aclose()
        logger.info("Redis connection closed.")


@app.get("/healthz")
async def healthz(request: Request):
    """Эндпоинт для проверки работоспособности сервиса."""
    logger.info(f"Health check requested from {request.client.host}")
    return {"ok": True}


@app.get("/users")
async def users(request: Request) -> Dict[str, str]:
    """Возвращает всех пользователей из кэша Redis."""
    if not redis:
        raise HTTPException(status_code=503, detail="Redis not available")
    
    key_users_hash = CONFIG.get("KEY_USERS_HASH", "cache:users:map")
    logger.info(f"Request for /users from {request.client.host}")
    
    data = await redis.hgetall(key_users_hash)
    logger.info(f"Found {len(data)} users in cache.")
    return data or {}


@app.get("/tasks/week")
async def tasks_week(request: Request):
    """Возвращает еженедельный снимок задач из кэша Redis."""
    if not redis:
        raise HTTPException(status_code=503, detail="Redis not available")

    key_tasks_json = CONFIG.get("KEY_TASKS_JSON", "cache:tasks:closed")
    logger.info(f"Request for /tasks/week from {request.client.host}")
    
    raw = await redis.get(key_tasks_json)
    if not raw:
        logger.warning("Weekly tasks cache is empty.")
        return {}
        
    try:
        data = json.loads(raw)
        logger.info(f"Returning cached tasks for {len(data)} users.")
        return data
    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse cached tasks JSON: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Cache payload parse error")
