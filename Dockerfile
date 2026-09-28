# Production Multi-Stage Dockerfile for Crypto Market Data Pipeline
FROM python:3.11-slim

# Set environment variables for Python performance & unbuffered logging
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app

# Install minimal OS dependencies for compilation & ODBC support
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    unixodbc-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Set working directory inside container
WORKDIR /app

# Step 1: Copy dependencies first to maximize Docker layer cache hits
COPY requirements.txt .

# Step 2: Install Python dependencies
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Step 3: Copy application source code and configurations
COPY src/ /app/src/
COPY bot/ /app/bot/
COPY scripts/ /app/scripts/
COPY pipeline.py /app/pipeline.py
COPY run_etl.py /app/run_etl.py

# Create directories for persistent runtime logs and data snapshots
RUN mkdir -p /app/logs /app/data/raw /app/data/transformed

# Default command: Execute the ETL pipeline
CMD ["python", "run_etl.py"]
