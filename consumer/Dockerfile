FROM python:3.12-slim

WORKDIR /app

# Копируем requirements из папки consumer и устанавливаем зависимости
COPY consumer/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Копируем код consumer и shared в рабочую директорию
COPY consumer/ /app/
COPY shared/ /app/shared/

# Запускаем приложение
CMD ["python", "consumer.py"]
