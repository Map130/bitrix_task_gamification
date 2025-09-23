import os, json, logging
from typing import Dict
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from redis.asyncio import Redis
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

load_dotenv()
REDIS_URL = os.getenv("REDIS_URL","redis://redis:6379/0")
PORT = int(os.getenv("PORT","8080"))
CORS = os.getenv("API_CORS","*")

app = FastAPI(title="Bitrix Weekly API", version="1.0.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=[CORS] if CORS != "*" else ["*"],
    allow_credentials=True, allow_methods=["*"], allow_headers=["*"]
)

redis: Redis | None = None
KEY_USERS_HASH = "cache:users:map"
KEY_TASKS_JSON = "cache:tasks:closed"

@app.on_event("startup")
async def startup():
    global redis
    logging.info("API starting up...")
    redis = Redis.from_url(REDIS_URL, decode_responses=True)
    logging.info("Redis client initialized.")

@app.on_event("shutdown")
async def shutdown():
    logging.info("API shutting down...")
    if redis:
        await redis.aclose()
        logging.info("Redis connection closed.")

@app.get("/healthz")
async def healthz(request: Request):
    logging.info(f"Health check requested from {request.client.host}")
    return {"ok": True}

@app.get("/users")
async def users(request: Request) -> Dict[str,str]:
    assert redis
    logging.info(f"Request for /users from {request.client.host}")
    data = await redis.hgetall(KEY_USERS_HASH)
    logging.info(f"Found {len(data)} users in cache.")
    return data or {}

@app.get("/tasks/week")
async def tasks_week(request: Request):
    assert redis
    logging.info(f"Request for /tasks/week from {request.client.host}")
    raw = await redis.get(KEY_TASKS_JSON)
    if not raw:
        logging.warning("Weekly tasks cache is empty.")
        return {}
    try:
        data = json.loads(raw)
        logging.info(f"Returning cached tasks for {len(data)} users.")
        return data
    except Exception as e:
        logging.error(f"Failed to parse cached tasks JSON: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Cache payload parse error")
