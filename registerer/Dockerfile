# 汎用的でツールが豊富な標準の Python 3.10 イメージを使用
FROM python:3.10

# 3. インタラクティブなプロンプトを無効化
ENV DEBIAN_FRONTEND=noninteractive
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

# テスト・デバッグに便利なツール群をインストール
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    iputils-ping \
    netcat-openbsd \
    procps \
    && rm -rf /var/lib/apt/lists/*

# Registererに必要なPythonパッケージのインストール
RUN pip install --no-cache-dir python-ndn

# プログラムのコピー
COPY registerer/registerer.py /app/
COPY registerer/average.ndn /app/

# 2. 起動コマンド: NFDの準備ができるまで待機してからRegistererを起動
# (Manager/Spyと同じ待機ロジック)
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
    echo 'NFD is ready! Starting Registerer...'; \
    python registerer.py \
"]
