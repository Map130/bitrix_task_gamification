FROM python:3.12-slim

WORKDIR /app

# Копируем requirements из папки producer и устанавливаем зависимости
COPY producer/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Копируем код producer и shared в рабочую директорию
COPY producer/ /app/
COPY shared/ /app/shared/

# Запускаем приложение
CMD ["python", "producer.py"]
