FROM python:3.11-slim
WORKDIR /app
RUN pip install pyyaml redis requests prometheus_client --no-cache-dir
COPY router.py .
EXPOSE 9103
CMD ["python", "router.py"]
