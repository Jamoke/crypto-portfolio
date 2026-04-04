FROM python:3.11-slim
WORKDIR /app
RUN pip install pyyaml redis requests --no-cache-dir
COPY dca.py .
CMD ["python", "dca.py"]
