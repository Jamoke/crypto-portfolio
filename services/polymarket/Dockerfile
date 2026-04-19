FROM python:3.11-slim
WORKDIR /app
# sqlite3/jq are used by the dashboard inspection commands and by humans
# ssh'd onto the Pi poking at the whale/positions/calibration DBs. Both are
# a few MB and have no runtime cost if unused.
RUN apt-get update && apt-get install -y --no-install-recommends \
        sqlite3 \
        jq \
    && rm -rf /var/lib/apt/lists/*
RUN pip install --no-cache-dir \
        anthropic \
        pyyaml \
        redis \
        requests \
        prometheus_client
COPY polymarket.py whale_tracker.py exit_monitor.py sizing.py calibrate.py ./
EXPOSE 9104
CMD ["python", "polymarket.py"]
