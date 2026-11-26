FROM python:3.12-slim
WORKDIR /app
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
COPY worker/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY messaging.py .
COPY worker/main.py .
CMD ["python", "main.py"]
