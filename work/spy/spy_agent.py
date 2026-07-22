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
@app.route(PREFIX)
def on_interest(name, interest_param, app_param):
    # 名前の末尾が 'resource' かどうかで判定
    if Component.to_str(name[-1]) == 'resource':
        payload = get_resource_payload()
        app.put_data(name, content=payload, freshness_period=1000)

async def register_remote_prefix(app: NDNApp, prefix_str: str):
    """リモート登録用: /localhop/nfd/rib/register を使用"""
    topic = Name.from_str('/localhop/nfd/rib/register')
    params = ControlParameters()
    params.name = Name.from_str(prefix_str)
    params.face_id = 0  # 自身のFace
    params.origin = 65
    params.cost = 0
    params.flags = 1

    signer = app.keychain.get_signer({})
    interest_name = make_command(topic, params, signer=signer)

    try:
        _, _, content = await app.express_interest(interest_name, lifetime=4000)
        response = ControlResponse.parse(content)
        if response.status_code in (200, 214):
            logging.info(f"Successfully registered: {prefix_str}")
        else:
            logging.error(f"Registration failed: {response.status_text}")
    except Exception as e:
        logging.error(f"Registration error: {e}")

async def main():
    await app.face.open()
    # 登録処理
    await register_remote_prefix(app, PREFIX)
    # 待機ループ
    await app.face.run()

if __name__ == '__main__':
    asyncio.run(main())