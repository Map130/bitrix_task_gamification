FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# 8090 - порт, который мы пробросили в docker-compose.yml
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8090"]
