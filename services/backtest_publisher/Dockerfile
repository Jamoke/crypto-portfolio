FROM python:3.11-slim
WORKDIR /app
RUN pip install --no-cache-dir flask prometheus-client
COPY publisher.py .
CMD ["python", "publisher.py"]
