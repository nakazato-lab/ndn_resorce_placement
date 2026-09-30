import os
import json
import logging
import asyncio
import psutil
from ndn_routing import RoutedNDNApp
from ndn.encoding import Name, Component
from ndn.transport.stream_face import TcpFace
from ndn.app_support.nfd_mgmt import make_command, ControlParameters, ControlResponse
from ndn.security import KeychainDigest

# ロギング設定
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')

MY_NODE_NAME = os.environ.get("NODE_NAME", "producer_1")
# 指定のInterest形式: /MY_NODE_NAME/spy/
PREFIX = f"/{MY_NODE_NAME}/spy"

# NFD接続設定
CONFIG_PATH = "/etc/ndn-config/ADDRESS"
NFD_ADDR = None
if os.path.exists(CONFIG_PATH):
    with open(CONFIG_PATH, "r") as f:
        NFD_ADDR = f.read().strip()

# Appインスタンス作成
if NFD_ADDR and NFD_ADDR != "not available yet":
    if ":" in NFD_ADDR:
        host, port_str = NFD_ADDR.split(":", 1)
        port = int(port_str)
    else:
        host = NFD_ADDR
        port = 6363
    logging.info(f"Connecting to NFD via TCP: {host}:{port}")
    app = RoutedNDNApp(face=TcpFace(host, port), keychain=KeychainDigest())
else:
    logging.info("Connecting to NFD via local UNIX socket")
    app = RoutedNDNApp(keychain=KeychainDigest())

# ホストのリソース取得用パス設定
if os.path.exists("/host/proc"):
    os.environ["PROCFS_PATH"] = "/host/proc"

def get_resource_payload():
    """リソース使用状況を取得し、Manager向けのスコア化されたペイロードを生成する"""
    cpu = psutil.cpu_percent(interval=0.1)
    mem = psutil.virtual_memory().percent

    # ノードの稼働限界（閾値）を80%に設定
    LIMIT_THRESHOLD = 80.0

    # 100点満点で受け入れ可能度（スコア）を計算
    # 使用率が閾値を超えた場合はマイナスになるため、max(0, ...) で最低点を0点にする
    cpu_score = max(0, int((LIMIT_THRESHOLD - cpu) / LIMIT_THRESHOLD * 100))
    mem_score = max(0, int((LIMIT_THRESHOLD - mem) / LIMIT_THRESHOLD * 100))

    # GPUと帯域は現状のpsutilでは取得できないため、ひとまず0点として定義
    gpu_score = 0
    bw_score = 0

    return json.dumps({
        "node_name": MY_NODE_NAME,
        "cpu_score": cpu_score,
        "mem_score": mem_score,
        "gpu_score": gpu_score,
        "bw_score": bw_score,
        "raw_cpu_usage": f"{cpu}%", # デバッグ用に元の生データも残しておく
        "raw_mem_usage": f"{mem}%"  # デバッグ用に元の生データも残しておく
    }).encode('utf-8')

# Interestハンドラ: /{NODE_NAME}/spy/resource などを処理
def on_interest(name, interest_param, app_param):
    name_str = Name.to_str(name)
    # 【追加】Interestを受信したログ
    logging.info(f"Received Interest: {name_str}")

    # 名前の末尾が 'resource' かどうかで判定
    if Component.to_str(name[-1]) == 'resource':
        payload = get_resource_payload()
        app.put_data(name, content=payload, freshness_period=1000)
        
        # 【追加】Managerへデータを返却したログ（中身も確認できるようにデコードして表示）
        logging.info(f"Returned resource info to Manager: {payload.decode('utf-8')}")
    else:
        # 【追加】対象外のInterestだった場合のログ（任意ですがデバッグに役立ちます）
        logging.info(f"Ignored Interest (not 'resource'): {name_str}")


if __name__ == '__main__':
    # 1. NFDからパケットが届いた際の「受け皿（関数）」をPrefixと紐づけて登録
    # ※ PREFIX変数やon_interest関数はご自身のSpyプログラムのものに合わせてください
    app.route(Name.from_str(PREFIX))(on_interest)
    
    logging.info(f"Starting Spy NDNApp... Waiting for Interests on {PREFIX}")
    
    # 2. NDNAppの通信エンジンを起動
    # （NFDへのTCP接続、Prefixの自動登録、永遠にInterestを待機するループをすべて自動で行います）
    try:
        app.run_forever()
    except KeyboardInterrupt:
        logging.info("Spy stopped by user.")