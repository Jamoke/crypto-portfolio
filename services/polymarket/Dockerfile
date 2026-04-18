FROM python:3.11-slim
WORKDIR /app
RUN pip install --no-cache-dir \
        anthropic \
        pyyaml \
        redis \
        requests \
        prometheus_client
COPY polymarket.py whale_tracker.py exit_monitor.py sizing.py calibrate.py ./
EXPOSE 9104
CMD ["python", "polymarket.py"]
