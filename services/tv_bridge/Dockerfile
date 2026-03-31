FROM python:3.11-slim
WORKDIR /app
RUN pip install flask redis gunicorn --no-cache-dir
COPY app.py .
EXPOSE 8443
CMD ["gunicorn", "--bind", "0.0.0.0:8443", "--workers", "2", "--timeout", "30", "app:app"]
