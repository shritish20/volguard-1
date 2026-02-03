FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    gcc \
    curl \
    dos2unix \
    gosu \
    && rm -rf /var/lib/apt/lists/*

# Copy and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY volguard_3.3.py /app/volguard.py

# Copy entrypoint script
COPY entrypoint.sh /entrypoint.sh
RUN dos2unix /entrypoint.sh && chmod +x /entrypoint.sh

# Create directories for data and logs
RUN mkdir -p /app/data /app/logs

# Create non-root user (permissions fixed in entrypoint)
RUN groupadd -g 1000 volguard && useradd -u 1000 -g 1000 volguard

# Expose FastAPI port
EXPOSE 8000

ENTRYPOINT ["/entrypoint.sh"]
