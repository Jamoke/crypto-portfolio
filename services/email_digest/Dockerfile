FROM python:3.11-slim
WORKDIR /app
RUN pip install anthropic redis requests --no-cache-dir
COPY digest.py .
CMD ["python", "digest.py"]
