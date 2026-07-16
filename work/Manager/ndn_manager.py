import os
import json
import logging
import asyncio
from datetime import datetime

# Kubernetes APIを利用してノード一覧を取得するため
from kubernetes import client, config

from ndn.app import NDNApp
from ndn.encoding import Name, Component
from ndn.transport.tcp_transport import TcpTransport
from ndn.app_support.nfd_mgmt import make_command, ControlParameters, ControlResponse

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')

PREFIX = "/Manager"
# image_d5a183.png にある通り、PVCにマウントされたDBファイルのパス
DB_FILENAME = "/data/node_db.json"

# ConfigMapからNFDのIPを取得
CONFIG_PATH = "/etc/ndn-config/ADDRESS"
NFD_IP = None
if os.path.exists(CONFIG_PATH):
    with open(CONFIG_PATH, "r") as f:
        NFD_IP = f.read().strip()

if NFD_IP and NFD_IP != "not available yet":
    logging.info(f"Connecting to NFD via TCP: {NFD_IP}:6363")
    app = NDNApp(transport=TcpTransport(NFD_IP, 6363))
else:
    logging.info("Connecting to NFD via local UNIX socket")
    app = NDNApp()

def get_k8s_nodes():
    """Kubernetes APIからクラスター内の全ノード名を取得する"""
    try:
        config.load_incluster_config()
        v1 = client.CoreV1Api()
        nodes = v1.list_node()
        return [node.metadata.name for node in nodes.items]
    except Exception as e:
        logging.error(f"Failed to get K8s nodes: {e}")
        return []

def on_interest(name, interest_param, app_param):
    """プレフィックス /Manager へのInterestを受信した際のルートハンドラ"""
    name_str = Name.to_str(name)
    logging.info(f"Received Interest: {name_str}")
    
    # /Manager/register の処理
    if "register" in name_str:
        asyncio.create_task(process_register(name, app_param))

async def process_register(name, app_param):
    try:
        # 1. K8sから全ノード名を取得
        node_names = get_k8s_nodes()
        if not node_names:
            app.put_data(name, content=b"Error: Cannot find any K8s nodes", freshness_period=1000)
            return

        db_data = []
        logging.info(f"Targeting nodes for resource collection: {node_names}")

        # 2. 各ノードの Spy へリソース照会 (並列処理も可能ですが、ここではシンプルに順次取得)
        for node_name in node_names:
            spy_prefix = f"/{node_name}/spy/resource"
            try:
                logging.info(f"Sending Interest to Spy: {spy_prefix}")
                _, _, content = await app.express_interest(
                    spy_prefix, must_be_fresh=True, can_be_prefix=False, lifetime=2000)
                
                spy_data = json.loads(bytes(content).decode('utf-8'))
                spy_data['timestamp'] = datetime.now().isoformat()
                db_data.append(spy_data)
            except Exception as e:
                logging.warning(f"Timeout or Error getting resource from {node_name}: {e}")

        # 3. node_db.json への保存 (PVC)
        os.makedirs(os.path.dirname(DB_FILENAME), exist_ok=True)
        with open(DB_FILENAME, "w", encoding="utf-8") as f:
            json.dump(db_data, f, indent=4, ensure_ascii=False)
        logging.info(f"Updated {DB_FILENAME} with latest resources.")

        if not db_data:
            app.put_data(name, content=b"Error: Failed to collect resources from all Spies", freshness_period=1000)
            return

        # 4. 最適なノード（スコア最大）の決定
        best_node_info = max(db_data, key=lambda x: x.get('score', 0))
        best_node = best_node_info['node_name']
        logging.info(f"★ Optimal Node Selected: {best_node} (Score: {best_node_info.get('score')})")

        # 5. 決定したノードの Seed へ Create Interest を送信
        # クライアントからの要求パラメータをパース
        req = json.loads(bytes(app_param).decode('utf-8')) if app_param else {}
        
        seed_prefix = f"/{best_node}/seed/create"
        forward_params = json.dumps({
            "type": "CREATE",
            "name": req.get("name", "unknown_func"),
            "content": req.get("content", ""),
            "content_type": req.get("content_type", "ndn")
        }).encode('utf-8')

        logging.info(f"-> Sending Create Interest to Seed: {seed_prefix}")
        try:
            _, _, seed_content = await app.express_interest(
                seed_prefix, app_param=forward_params, must_be_fresh=True, can_be_prefix=False, lifetime=5000)
            result_msg = f"Success: Function deployed on {best_node}. Seed response: {bytes(seed_content).decode('utf-8')}"
        except Exception as e:
            result_msg = f"Error: Seed deployment failed on {best_node}: {e}"

        # 6. クライアントへ最終結果を返却
        app.put_data(name, content=result_msg.encode('utf-8'), freshness_period=1000)

    except Exception as e:
        logging.error(f"Error in process_register: {e}")
        app.put_data(name, content=f"Manager System Error: {e}".encode('utf-8'), freshness_period=1000)

async def register_remote_prefix(app: NDNApp, prefix_str: str):
    """リモートのNFDに対して /localhop を使ってプレフィックス(Face)を登録する (Spyと同等の処理)"""
    topic = Name.from_str('/localhop/nfd/rib/register')
    params = ControlParameters()
    params.name = Name.from_str(prefix_str)
    params.face_id = 0
    params.origin = 65
    params.cost = 0
    params.flags = 1

    signer = app.keychain.get_signer({})
    interest_name = make_command(topic, params, signer=signer)

    try:
        _, _, content = await app.express_interest(interest_name, lifetime=4000)
        response = ControlResponse.parse(content)
        if response.status_code in (200, 214):
            logging.info(f"Successfully registered Manager prefix: {prefix_str}")
        else:
            logging.error(f"Registration failed: {response.status_text}")
    except Exception as e:
        logging.error(f"Registration error: {e}")

async def main():
    app.router.add_route(Name.from_str(PREFIX), on_interest)
    await app.face.open()
    await register_remote_prefix(app, PREFIX)
    await app.face.run()

if __name__ == '__main__':
    asyncio.run(main())