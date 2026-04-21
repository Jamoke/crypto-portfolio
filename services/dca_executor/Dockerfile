FROM python:3.11-slim
WORKDIR /app
RUN pip install pyyaml redis requests prometheus_client --no-cache-dir
COPY dca.py .
EXPOSE 9106
CMD ["python", "dca.py"]
