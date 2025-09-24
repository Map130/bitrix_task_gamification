FROM python:3.11-slim

WORKDIR /app

# Install postgresql-client and curl for health checks
# Устанавливаем зависимости и утилиту для конвертации строк
RUN apt-get update && apt-get install -y postgresql-client curl dos2unix && rm -rf /var/lib/apt/lists/*

COPY config-server/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY config-server/ .

# Convert line endings and make the entrypoint script executable
RUN dos2unix /app/entrypoint.sh && chmod +x /app/entrypoint.sh

# Set the entrypoint
ENTRYPOINT ["/app/entrypoint.sh"]

# The command to run after the entrypoint successfully waits for the DB
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8090"]


