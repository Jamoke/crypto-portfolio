FROM python:3.11-slim
WORKDIR /app
RUN pip install redis requests pyyaml anthropic --no-cache-dir
COPY executor.py .
CMD ["python", "executor.py"]
