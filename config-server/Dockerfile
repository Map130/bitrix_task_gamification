FROM python:3.11-slim

WORKDIR /app

# Install postgresql-client and curl for health checks
RUN apt-get update && apt-get install -y postgresql-client curl && rm -rf /var/lib/apt/lists/*

COPY config-server/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY config-server/ .

# Make the entrypoint script executable
RUN chmod +x /app/entrypoint.sh

# Set the entrypoint
ENTRYPOINT ["/app/entrypoint.sh"]

# The command to run after the entrypoint successfully waits for the DB
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8090"]


