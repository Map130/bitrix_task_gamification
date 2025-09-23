FROM python:3.11-slim

WORKDIR /app

# Добавляем общую папку в PYTHONPATH
ENV PYTHONPATH "${PYTHONPATH}:/app"

# Копирование файла зависимостей и их установка
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Копирование кода приложения и общей папки
COPY producer_webhook/ /app/producer_webhook/
COPY shared/ /app/shared/

# Запуск приложения
CMD ["uvicorn", "producer_webhook.main:app", "--host", "0.0.0.0", "--port", "8000"]
