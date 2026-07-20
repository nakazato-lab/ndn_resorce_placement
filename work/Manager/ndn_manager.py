import os
import json
import logging
import asyncio
from datetime import datetime
from typing import List, Dict, Any, Optional

# Kubernetes APIを利用してノード一覧を取得するため
from kubernetes import client, config

from ndn.app import NDNApp
from ndn.encoding import Name, Component
from ndn.transport.tcp_transport import TcpTransport
from ndn.app_support.nfd_mgmt import make_command, ControlParameters, ControlResponse

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')

PREFIX = "/Manager"
DB_FILENAME = "/data/node_db.json"

# =========================================================
# 1. NFD接続設定 (NFD_ADDR の取得と IP:PORT 分割処理)
# =========================================================
CONFIG_PATH = "/etc/ndn-config/ADDRESS"
NFD_ADDR = None

# ConfigMapからポート番号付きのアドレス (例: "10.244.0.5:6363") を取得
if os.path.exists(CONFIG_PATH):
    with open(CONFIG_PATH, "r") as f:
        NFD_ADDR = f.read().strip()

if NFD_ADDR and NFD_ADDR != "not available yet":
    # NFD_ADDR 内の ":" を基準に IP(host) と PORT を分割
    if ":" in NFD_ADDR:
        host, port_str = NFD_ADDR.rsplit(":", 1)
        port = int(port_str)
    else:
        host = NFD_ADDR
        port = 6363  # デフォルトポート
        
    logging.info(f"Connecting to NFD via TCP: {host}:{port} (from NFD_ADDR={NFD_ADDR})")
    app = NDNApp(transport=TcpTransport(host, port))
else:
    logging.info("Connecting to NFD via local UNIX socket")
    app = NDNApp()


# =========================================================
# 3. ノードスコア計算・配置先決定関数 (独立した関数として定義)未完成
# =========================================================
def calculate_scores_and_select_node(
    nodes_resource_data: List[Dict[str, Any]], 
    manual_target_node: Optional[str] = None
) -> Optional[Dict[str, Any]]:
    """
    Spyから収集した全ノードのリソース情報をもとに、配置先ノードを選択する関数。
    
    - manual_target_node: 手動で特定のノードを指定したい場合に渡す
    - 提案手法決定前のため、現在はスコア最大値を選ぶ簡易ロジックを実装
    """
    if not nodes_resource_data:
        logging.warning("[Score Engine] リソースデータが存在しないためスコア計算を行えません。")
        return None

    # --- パターンA: 手動配置指定がある場合 ---
    if manual_target_node:
        for node_data in nodes_resource_data:
            if node_data.get("node_name") == manual_target_node:
                logging.info(f"[Score Engine] 手動指定されたノードを選択: {manual_target_node}")
                return node_data
        logging.warning(f"[Score Engine] 指定されたノード '{manual_target_node}' が見つかりません。自動選択にフォールバックします。")

    # --- パターンB: 簡易スコア計算ロジック（暫定実装） ---
    # 例: Spyから収集した JSON 内の 'score' 属性が最大となるノードを選択
    # ※ 提案手法の独自アルゴリズムが決定次第、この関数内のロジックを差し替えます。
    selected_node = max(nodes_resource_data, key=lambda x: x.get('score', 0))
    
    logging.info(f"[Score Engine] 最適ノードを決定: {selected_node.get('node_name')} (Score: {selected_node.get('score', 0)})")
    return selected_node


def get_k8s_nodes() -> List[str]:
    """Kubernetes APIからクラスター内の全ノード名を取得する"""
    try:
        config.load_incluster_config()
        v1 = client.CoreV1Api()
        nodes = v1.list_node()
        return [node.metadata.name for node in nodes.items]
    except Exception as e:
        logging.error(f"K8sノード一覧の取得に失敗しました: {e}")
        return []


# =========================================================
# 2. Interestハンドラ & # --- .ndn関数の登録要求受付 ---
# =========================================================
def on_interest(name, interest_param, app_param):
    """プレフィックス /Manager へのInterestを受信した際のルートハンドラ"""
    name_str = Name.to_str(name)
    logging.info(f"Received Interest: {name_str}")
    
    cleaned_name = name_str.removeprefix('/Manager').removeprefix('/manager')
    split_name = cleaned_name.strip('/').split('/')
    operation = split_name[0] if split_name and split_name[0] else ""

    # --- .ndn関数の登録要求受付 ---
    if operation == 'register':
        asyncio.create_task(process_register(name, app_param))


async def process_register(name, app_param):
    """
    元のプログラムと全く同じエラー返却・パラメータ検証ロジックを保持しつつ、
    Spyからのリソース収集・スコア判定・Seedへの転送を実行する
    """
    # 1. 元のプログラムと同等のパラメータ検証 (エラーメッセージも完全一致)
    if not app_param:
        app.put_data(name, content=b"Error: REGISTER requires ApplicationParameters", freshness_period=1000)
        return

    try:
        req = json.loads(bytes(app_param).decode('utf-8'))
    except json.JSONDecodeError as e:
        err_msg = f"Error: Invalid JSON in ApplicationParameters ({e})"
        app.put_data(name, content=err_msg.encode('utf-8'), freshness_period=1000)
        return

    func_name = req.get("name")
    content = req.get("content")
    content_type = req.get("content_type", "ndn")
    manual_node = req.get("target_node")  # リクエスト内に手動指定(target_node)があれば取得

    if not func_name or not content:
        app.put_data(name, content=b"Error: 'name' and 'content' are required", freshness_period=1000)
        return

    # 2. K8s全ノードの取得とSpyへの動的リソース照会
    node_names = get_k8s_nodes()
    if not node_names:
        app.put_data(name, content=b"Error: Cannot find any K8s nodes", freshness_period=1000)
        return

    db_data = []
    for node_name in node_names:
        spy_prefix = f"/{node_name}/spy/resource"
        try:
            logging.info(f"Sending Interest to Spy: {spy_prefix}")
            _, _, spy_content = await app.express_interest(
                spy_prefix, must_be_fresh=True, can_be_prefix=False, lifetime=2000)
            
            spy_json = json.loads(bytes(spy_content).decode('utf-8'))
            spy_json['timestamp'] = datetime.now().isoformat()
            db_data.append(spy_json)
        except Exception as e:
            logging.warning(f"Timeout or Error getting resource from {node_name}: {e}")

    # 収集結果を node_db.json に永続化
    os.makedirs(os.path.dirname(DB_FILENAME), exist_ok=True)
    with open(DB_FILENAME, "w", encoding="utf-8") as f:
        json.dump(db_data, f, indent=4, ensure_ascii=False)
    logging.info(f"Updated {DB_FILENAME} with latest resources.")

    if not db_data:
        app.put_data(name, content=b"Error: Failed to collect resources from all Spies", freshness_period=1000)
        return

    # 3. 独立させたスコア計算関数を呼び出し、最適ノードを決定
    best_node_data = calculate_scores_and_select_node(db_data, manual_target_node=manual_node)
    if not best_node_data:
        app.put_data(name, content=b"Error: Failed to select an optimal node", freshness_period=1000)
        return

    best_node = best_node_data['node_name']

    # 4. 決定したノードの Seed へ Create Interest を転送
    seed_prefix = f"/{best_node}/seed/create"
    forward_params = json.dumps({
        "type": "CREATE", 
        "name": func_name, 
        "content": content, 
        "content_type": content_type
    }).encode('utf-8')

    logging.info(f"-> Redirecting Seed with Interest: {seed_prefix} (name={func_name})")
    try:
        _, _, seed_content = await app.express_interest(
            seed_prefix, app_param=forward_params, must_be_fresh=True, can_be_prefix=False, lifetime=5000)

        if seed_content:
            seed_response = bytes(seed_content).decode('utf-8')
            response_msg = f"Manager_Proxy_Success: {seed_response}"
        else:
            response_msg = "Manager_Proxy_Error: Seedから空のデータが返されました"

    except Exception as e:
        response_msg = f"Manager_Proxy_Error: Seedへの送信に失敗しました ({e})"

    app.put_data(name, content=response_msg.encode('utf-8'), freshness_period=1000)


async def register_remote_prefix(app: NDNApp, prefix_str: str):
    """リモートのNFDに対して /localhop を使ってプレフィックス(Face)を登録する"""
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