FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

# Create non-root user (security hardening — minimal change)
RUN addgroup --system app && adduser --system --ingroup app app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code only — tests excluded from production image
COPY app/ app/

# Switch to non-root user
USER app

# Run with gunicorn + uvicorn workers for production
# Cloud Run sets PORT env var (default 8080)
ENV PORT=8080
EXPOSE 8080

CMD exec gunicorn app.main:app \
    --bind 0.0.0.0:${PORT} \
    --workers 1 \
    --worker-class uvicorn.workers.UvicornWorker \
    --timeout 120 \
    --access-logfile - \
    --error-logfile -
