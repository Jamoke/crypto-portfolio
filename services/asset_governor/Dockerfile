FROM python:3.11-slim
WORKDIR /app
RUN pip install flask pyyaml redis requests prometheus_client --no-cache-dir
COPY governor.py .
EXPOSE 8090
CMD ["python", "governor.py"]
