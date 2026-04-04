FROM python:3.11-slim
WORKDIR /app
RUN pip install anthropic redis requests pyyaml flask prometheus_client --no-cache-dir
COPY analyst.py .
CMD ["python", "analyst.py"]
