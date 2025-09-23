import os
import json
import logging
import pika
import requests
from fastapi import FastAPI, Request, HTTPException, BackgroundTasks
from pydantic import BaseModel, Field
from typing import Dict, Any

# --- Настройка логирования ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Загрузка конфигурации из переменных окружения ---
BITRIX_WEBHOOK_URL = os.getenv("BITRIX_WEBHOOK_URL", "").rstrip("/")
RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@rabbitmq:5672/%2F")
QUEUE_TASKS_EVENTS = os.getenv("QUEUE_TASKS_EVENTS", "bitrix.tasks.events")
HTTP_TIMEOUT = int(os.getenv("HTTP_TIMEOUT_SEC", "60"))

if not BITRIX_WEBHOOK_URL:
    raise RuntimeError("BITRIX_WEBHOOK_URL is required")

# --- Модели данных для валидации входящих вебхуков ---
class TaskFields(BaseModel):
    ID: str

class WebhookData(BaseModel):
    Fields: TaskFields = Field(..., alias='data[FIELDS]')

class WebhookPayload(BaseModel):
    event: str
    data: Dict[str, Any]
    ts: int

# --- Инициализация FastAPI ---
app = FastAPI(
    title="Bitrix Webhook Producer",
    description="Receives webhooks from Bitrix24, fetches task details, and sends them to RabbitMQ.",
    version="1.0.0"
)

# --- Функции для работы с Bitrix24 API ---
def get_task_details(task_id: str) -> Dict[str, Any] | None:
    """Запрашивает детали задачи по её ID."""
    try:
        url = f"{BITRIX_WEBHOOK_URL}/tasks.task.get.json"
        payload = {"taskId": task_id, "select": ["ID", "RESPONSIBLE_ID", "STATUS"]}
        r = requests.post(url, json=payload, timeout=HTTP_TIMEOUT)
        r.raise_for_status()
        data = r.json()

        if "error" in data:
            logging.error(f"Bitrix API error for task {task_id}: {data.get('error_description', data['error'])}")
            return None
        
        task_info = data.get("result", {}).get("task")
        if not task_info:
            logging.warning(f"Task {task_id} not found in Bitrix API response.")
            return None
            
        return task_info

    except requests.RequestException as e:
        logging.error(f"HTTP request to Bitrix failed for task {task_id}: {e}")
        return None
    except Exception as e:
        logging.error(f"An unexpected error occurred in get_task_details for task {task_id}: {e}")
        return None

# --- Функция для отправки сообщения в RabbitMQ ---
def publish_to_rabbitmq(message: Dict[str, Any]):
    """Публикует сообщение в очередь RabbitMQ."""
    try:
        params = pika.URLParameters(RABBITMQ_URL)
        connection = pika.BlockingConnection(params)
        channel = connection.channel()
        
        channel.queue_declare(queue=QUEUE_TASKS_EVENTS, durable=True)
        
        channel.basic_publish(
            exchange='',
            routing_key=QUEUE_TASKS_EVENTS,
            body=json.dumps(message).encode('utf-8'),
            properties=pika.BasicProperties(
                content_type="application/json",
                delivery_mode=2  # Сделать сообщение постоянным
            )
        )
        logging.info(f"Published message to '{QUEUE_TASKS_EVENTS}': {message}")
        connection.close()
    except Exception as e:
        logging.error(f"Failed to publish message to RabbitMQ: {e}")

# --- Основная логика обработки вебхука ---
def process_webhook(payload: WebhookPayload):
    """
    Обрабатывает входящий вебхук: проверяет статус задачи и отправляет в очередь.
    """
    # Нас интересует только событие обновления задачи
    if payload.event != 'ONTASKUPDATE':
        logging.info(f"Ignoring event '{payload.event}'.")
        return

    try:
        task_id = payload.data['FIELDS']['ID']
    except KeyError:
        logging.warning("Webhook payload does not contain task ID. Payload: %s", payload.dict())
        return

    logging.info(f"Processing 'ONTASKUPDATE' for task ID: {task_id}")
    
    task_details = get_task_details(task_id)
    
    if not task_details:
        return # Ошибка уже залогирована внутри get_task_details

    # Статус "5" в Bitrix24 по умолчанию означает "Завершена"
    if str(task_details.get("status")) == '5':
        responsible_id = task_details.get("responsibleId")
        if not responsible_id:
            logging.warning(f"Task {task_id} is completed but has no responsible ID.")
            return
            
        # Оборачиваем данные в конверт с типом "event"
        message = {
            "type": "event",
            "data": {
                "user_id": int(responsible_id),
                "task_id": int(task_id),
                "closed_at": payload.ts  # Время события из вебхука
            }
        }
        publish_to_rabbitmq(message)
    else:
        logging.info(f"Task {task_id} status is '{task_details.get('status')}', not '5'. Ignoring.")


# ... (импорты)
from shared.config import get_config_value_async

# ... (остальной код до эндпоинта)

# --- Эндпоинт для приема вебхуков ---
@app.post("/webhook")
async def handle_webhook(request: Request, background_tasks: BackgroundTasks):
    """
    Принимает вебхуки от Bitrix24 и запускает их обработку в фоновом режиме,
    если сервис находится в правильном режиме работы.
    """
    # Проверяем режим работы перед любой обработкой
    mode = await get_config_value_async("producer_mode", "none")
    if mode != "webhook":
        logging.info(f"Ignoring webhook because current mode is '{mode}'.")
        # Важно ответить OK, чтобы Bitrix24 не повторял запрос
        return {"ok": True}

    try:
        payload_data = await request.json()
        payload = WebhookPayload(**payload_data)
    except Exception as e:
        logging.error(f"Failed to parse incoming webhook payload: {e}")
        raise HTTPException(status_code=400, detail="Invalid payload format.")

    # Добавляем задачу в фон, чтобы не заставлять Bitrix24 ждать
    background_tasks.add_task(process_webhook, payload)
    
    # Сразу отвечаем Bitrix24, что все хорошо
    return {"ok": True}

# ... (остальной код)

@app.get("/healthz")
async def healthz():
    """Проверка состояния сервиса."""
    return {"status": "ok"}

# --- Для локального запуска (не используется в Docker) ---
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
