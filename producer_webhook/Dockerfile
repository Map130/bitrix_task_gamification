FROM python:3.11-slim

WORKDIR /app

# Копируем requirements из папки producer_webhook и устанавливаем зависимости
COPY producer_webhook/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Копируем код producer_webhook и shared в рабочую директорию
COPY producer_webhook/ /app/
COPY shared/ /app/shared/

# Запускаем приложение
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
