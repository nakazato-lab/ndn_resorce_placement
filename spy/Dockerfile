FROM python:3.10

ENV DEBIAN_FRONTEND=noninteractive
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    netcat-openbsd \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir python-ndn psutil

COPY ndn_routing.py /app/
COPY spy/spy_agent.py /app/
COPY spy/network_metrics.py /app/

CMD ["/bin/bash", "-c", "\
    echo 'Waiting for NFD endpoint configuration...'; \
    while [ ! -f /etc/ndn-config/ADDRESS ] || [ \"$(cat /etc/ndn-config/ADDRESS)\" = \"not available yet\" ]; do \
        sleep 2; \
    done; \
    NFD_IP=$(cat /etc/ndn-config/ADDRESS | cut -d':' -f1); \
    echo \"Found NFD IP: $NFD_IP. Waiting for NFD to open port 6363...\"; \
    while ! nc -z $NFD_IP 6363; do \
        sleep 2; \
    done; \
    echo 'NFD is ready! Starting Spy...'; \
    python spy_agent.py \
"]
