FROM python:3.11-slim
WORKDIR /app

# System deps for web3 (needed for some crypto primitives)
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc libffi-dev && \
    rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir \
    redis \
    requests \
    pyyaml \
    flask \
    gunicorn \
    web3 \
    anthropic

COPY executor.py .
EXPOSE 8091
CMD ["python", "executor.py"]
