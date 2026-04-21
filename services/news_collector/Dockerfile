FROM python:3.11-slim
WORKDIR /app
RUN pip install redis requests prometheus_client --no-cache-dir
COPY news.py .
EXPOSE 9107
CMD ["python", "news.py"]
