import os
import json
import logging
import asyncio
import psutil
from ndn.app import NDNApp
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
    app = NDNApp(face=TcpFace(host, port), keychain=KeychainDigest())
else:
    logging.info("Connecting to NFD via local UNIX socket")
    app = NDNApp(keychain=KeychainDigest())

# ホストのリソース取得用パス設定
if os.path.exists("/host/proc"):
    os.environ["PROCFS_PATH"] = "/host/proc"

def get_resource_payload():
    cpu = psutil.cpu_percent(interval=0.1)
    mem = psutil.virtual_memory().percent
    return json.dumps({
        "node_name": MY_NODE_NAME,
        "cpu_usage": f"{cpu}%",
        "memory_usage": f"{mem}%",
        "score": int(12000 * ((100.0 - cpu) / 100.0))
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