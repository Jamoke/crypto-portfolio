FROM python:3.11-slim
WORKDIR /app
RUN pip install requests prometheus_client redis --no-cache-dir
COPY exporter.py .
EXPOSE 9101
CMD ["python", "exporter.py"]
