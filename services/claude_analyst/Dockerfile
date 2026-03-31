FROM python:3.11-slim
WORKDIR /app
RUN pip install anthropic redis requests pyyaml --no-cache-dir
COPY analyst.py .
CMD ["python", "analyst.py"]
