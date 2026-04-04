FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt ./requirements.txt
COPY pyproject.toml ./pyproject.toml
COPY py_earnings_calls ./py_earnings_calls
COPY README.md ./README.md

RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt \
    && pip install --no-cache-dir -e .

CMD ["python", "-m", "py_earnings_calls.service_runtime", "api", "--host", "0.0.0.0", "--port", "8000"]
