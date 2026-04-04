FROM python:3.11-slim
WORKDIR /app
RUN pip install anthropic pyyaml redis requests prometheus_client --no-cache-dir
COPY polymarket.py .
EXPOSE 9104
CMD ["python", "polymarket.py"]
