FROM python:3.11-slim

WORKDIR /app

# Copy requirements first for better caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application
COPY . .

# Expose port 8080 (Cloud Run default)
EXPOSE 8080

# Use the PORT env var provided by Cloud Run; default to 8080 when not set.
CMD ["sh", "-c", "uvicorn app.main:app --host 0.0.0.0 --port ${PORT:-8080} --timeout-keep-alive 30"]

