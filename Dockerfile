FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt && pip install --no-cache-dir fastapi uvicorn[standard] anthropic yfinance

COPY . .

RUN mkdir -p data db logs

EXPOSE 8000

HEALTHCHECK --interval=60s --timeout=10s --start-period=30s --retries=3 CMD curl -f http://localhost:8000/api/health || exit 1

CMD ["uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "2"]
