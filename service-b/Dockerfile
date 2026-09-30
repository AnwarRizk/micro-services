# Local build context — main.py doesn't import anything outside

FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# 8000 serves both the REST API (/sum, /healthz) and /metrics
EXPOSE 8000

# --host 0.0.0.0 is required so other containers (Prometheus) can reach
# this service. No --reload here — that's a development-only convenience;
# a container should just run the code it was built with.
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]