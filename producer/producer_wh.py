import os
import logging
import json
from typing import Dict

import pika
import requests
from fastapi import FastAPI, Request, HTTPException, BackgroundTasks
from dotenv import load_dotenv

# --- Настройка логирования ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Загрузка конфигурации ---
load_dotenv()
WEBHOOK_URL = os.getenv("BITRIX_WEBHOOK_URL", "").rstrip("/")
RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@rabbitmq:5672/%2F")
QUEUE_TASK_EVENTS = os.getenv("QUEUE_TASK_EVENTS", "bitrix.tasks.events")
PRODUCER_PORT = int(os.getenv("PRODUCER_WH_PORT", "8000"))
HTTP_TIMEOUT = int(os.getenv("HTTP_TIMEOUT_SEC", "60"))

if not WEBHOOK_URL:
    raise RuntimeError("BITRIX_WEBHOOK_URL is required")

# --- RabbitMQ соединение ---
def get_rabbitmq_channel():
    params = pika.URLParameters(RABBITMQ_URL)
    connection = pika.BlockingConnection(params)
    channel = connection.channel()
    channel.queue_declare(queue=QUEUE_TASK_EVENTS, durable=True)
    return connection, channel

# --- Bitrix24 API ---
def bitrix_rest_call(method: str, payload: dict = None) -> Dict:
    """Выполняет вызов к REST API Bitrix24."""
    url = f"{WEBHOOK_URL}/{method}.json"
    try:
        r = requests.post(url, json=payload or {}, timeout=HTTP_TIMEOUT)
        r.raise_for_status()
        data = r.json()
        if "error" in data:
            logging.error(f"Bitrix API error: {data.get('error_description', data['error'])}")
            return {}
        return data.get("result", {})
    except requests.RequestException as e:
        logging.error(f"Request to Bitrix24 failed: {e}")
        return {}

def get_task_details(task_id: int) -> Dict:
    """Получает детали задачи по ее ID."""
    return bitrix_rest_call("tasks.task.get", {"taskId": task_id, "select": ["ID", "RESPONSIBLE_ID", "STATUS"]})

def publish_task_event(task_data: Dict):
    """Публикует событие о задаче в RabbitMQ."""
    conn, ch = None, None
    try:
        conn, ch = get_rabbitmq_channel()
        ch.basic_publish(
            exchange='',
            routing_key=QUEUE_TASK_EVENTS,
            body=json.dumps(task_data).encode(),
            properties=pika.BasicProperties(
                content_type="application/json",
                delivery_mode=2  # make message persistent
            )
        )
        logging.info(f"Published task event to '{QUEUE_TASK_EVENTS}': {task_data}")
    except Exception as e:
        logging.error(f"Failed to publish message to RabbitMQ: {e}", exc_info=True)
    finally:
        if conn and conn.is_open:
            conn.close()

# --- FastAPI приложение ---
app = FastAPI(title="Bitrix Webhook Producer", version="1.0.0")

@app.post("/webhook/task_update")
async def handle_task_update(request: Request, background_tasks: BackgroundTasks):
    """
    Эндпоинт для приема вебхуков от Bitrix24 о событиях с задачами.
    Работает для события ONTASKUPDATE.
    """
    try:
        payload = await request.json()
    except json.JSONDecodeError:
        logging.error("Received invalid JSON in webhook.")
        raise HTTPException(status_code=400, detail="Invalid JSON payload")

    event = payload.get("event")
    if event != "ONTASKUPDATE":
        logging.warning(f"Received non-task-update event: {event}. Ignoring.")
        return {"status": "event_ignored"}

    try:
        task_id = int(payload["data"]["FIELDS_AFTER"]["ID"])
        status = int(payload["data"]["FIELDS_AFTER"]["STATUS"])
    except (KeyError, ValueError, TypeError):
        logging.warning(f"Could not extract task ID or status from payload: {payload}")
        return {"status": "payload_incomplete"}

    # Статус "5" в Bitrix24 по умолчанию означает "Завершена"
    if status == 5:
        logging.info(f"Task {task_id} has been completed. Fetching details...")
        # Выполняем получение деталей и отправку в RabbitMQ в фоне,
        # чтобы не заставлять Bitrix24 ждать ответа.
        background_tasks.add_task(process_completed_task, task_id)
        return {"status": "task_accepted"}
    else:
        logging.info(f"Task {task_id} status changed to {status}. Ignoring.")
        return {"status": "status_ignored"}

def process_completed_task(task_id: int):
    """Фоновая задача для обработки завершенной задачи."""
    task_info = get_task_details(task_id)
    task = task_info.get("task")

    if not task or not task.get("responsibleId"):
        logging.error(f"Could not get details for task {task_id} or responsible user is missing.")
        return

    user_id = int(task["responsibleId"])
    message = {"user_id": user_id, "task_id": task_id}
    
    publish_task_event(message)

@app.get("/healthz")
async def healthz():
    """Эндпоинт для проверки работоспособности."""
    return {"ok": True}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=PRODUCER_PORT)
