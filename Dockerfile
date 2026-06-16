FROM python:3.13-slim

# Install system dependencies untuk Playwright/Chromium
RUN apt-get update && apt-get install -y \
    wget \
    gnupg \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Install Playwright + Chromium (dilakukan saat BUILD, bukan saat start)
RUN playwright install chromium --with-deps

# Copy source code
COPY . .

# Set PYTHONPATH
ENV PYTHONPATH=/app

# Jalankan ETL
CMD ["python", "src/main.py"]
