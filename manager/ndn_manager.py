import os
import json
import logging
import asyncio
from datetime import datetime

from kubernetes import client, config
from ndn.app import NDNApp
from ndn.encoding import Name
from ndn.transport.stream_face import TcpFace
from ndn.app_support.nfd_mgmt import make_command, ControlParameters, ControlResponse
from ndn.security import KeychainDigest

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')

PREFIX = "/Manager"
DB_FILENAME = "/data/node_db.json"

# 1. NFD_ADDRの取得とポート分割処理
CONFIG_PATH = "/etc/ndn-config/ADDRESS"
NFD_ADDR = None

if os.path.exists(CONFIG_PATH):
    with open(CONFIG_PATH, "r") as f:
        NFD_ADDR = f.read().strip()

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


# 3. スコア計算関数の独立化（簡易計算および手動指定）
def calculate_score_and_select_node(db_data, target_node=None):
    """
    各ノードのリソース情報から最適な配置先を決定する。
    1. 手動指定がある場合はそれを使用。
    2. 基本は、CPU, メモリ, GPU, 帯域のスコア（100点満点）に重みを掛けて総合スコアを算出し、最大値のノードを選択。
    """
    if not db_data:
        return None

    # 手動指定がある場合
    if target_node:
        for node in db_data:
            if node.get('node_name') == target_node:
                logging.info(f"Manual target node selected: {target_node}")
                return node
        logging.warning(f"Target node '{target_node}' not found in DB. Falling back to calculation.")

    # 評価指標の重み付け（和が1.0）
    # ※今後の拡張で、要求される関数ごとにこの重みを動的に変えられるよう変更する
    weights = {
        'gpu': 0.50,
        'mem': 0.25,
        'cpu': 0.15,
        'bw':  0.10
    }

    best_node = None
    max_score = -1.0

    # 各ノードの総合スコアを計算
    for node in db_data:
        # Spyから取得したデータから各スコアを抽出（未取得の場合は0とする）
        s_gpu = node.get('gpu_score', 0)
        s_mem = node.get('mem_score', 0)
        s_cpu = node.get('cpu_score', 0)
        s_bw  = node.get('bw_score', 0)

        # ResourceScore = w_cpu*S_cpu + w_mem*S_mem + w_gpu*S_gpu + w_bw*S_bw
        total_score = (weights['gpu'] * s_gpu) + \
                      (weights['mem'] * s_mem) + \
                      (weights['cpu'] * s_cpu) + \
                      (weights['bw']  * s_bw)
        
        # ログ確認用に計算結果を格納
        node['calculated_total_score'] = total_score

        if total_score > max_score:
            max_score = total_score
            best_node = node

    if best_node:
        logging.info(f"Auto-selected node: {best_node.get('node_name')} (score: {max_score:.2f})")
    
    return best_node


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
    name_str = Name.to_str(name)
    logging.info(f"Received Interest: {name_str}")
    
    # --- .ndn関数の登録要求受付 ---
    if "register" in name_str:
        asyncio.create_task(process_register(name, app_param))


async def process_register(name, app_param):
    # 2. 元のプログラムと全く同じエラーハンドリング・パラメータ取得ロジック
    if not app_param:
        app.put_data(name, content=b"Error: ApplicationParameters are required", freshness_period=1000)
        return

    try:
        req = json.loads(bytes(app_param).decode('utf-8'))
    except json.JSONDecodeError as e:
        app.put_data(name, content=f"Error: Invalid JSON ({e})".encode('utf-8'), freshness_period=1000)
        return

    func_name = req.get("name")
    content = req.get("content")
    content_type = req.get("content_type", "ndn")
    target_node = None# テストとかで手動選択するならここを変更
    # target_node = req.get("target_node") もし、app_paramにターゲットを載せるならこれはいるけどたぶんいらないはず

    if not func_name or not content:
        app.put_data(name, content=b"Error: 'name' and 'content' are required", freshness_period=1000)
        return

    # K8sから全ノード名を取得し、Spyへリソース照会
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

    os.makedirs(os.path.dirname(DB_FILENAME), exist_ok=True)
    with open(DB_FILENAME, "w", encoding="utf-8") as f:
        json.dump(db_data, f, indent=4, ensure_ascii=False)

    if not db_data:
        app.put_data(name, content=b"Error: Failed to collect resources from Spies", freshness_period=1000)
        return

    # スコア計算関数を呼び出して最適ノードを決定
    best_node_info = calculate_score_and_select_node(db_data, target_node)
    if not best_node_info:
        app.put_data(name, content=b"Error: Failed to select node", freshness_period=1000)
        return

    best_node = best_node_info['node_name']

    # 決定したノードのSeedへCreate Interestを送信
    seed_prefix = f"/{best_node}/seed"
    forward_params = json.dumps({
        "type": "CREATE",
        "name": func_name,
        "content": content,
        "content_type": content_type
    }).encode('utf-8')

    try:
        _, _, seed_content = await app.express_interest(
            seed_prefix, app_param=forward_params, must_be_fresh=True, can_be_prefix=False, lifetime=5000)
        result_msg = f"Success: Function deployed on {best_node}. Seed response: {bytes(seed_content).decode('utf-8')}"
    except Exception as e:
        result_msg = f"Error: Seed deployment failed on {best_node}: {e}"

    app.put_data(name, content=result_msg.encode('utf-8'), freshness_period=1000)





if __name__ == '__main__':
    # 1. NFDからパケットが届いた際の「受け皿（関数）」をPrefixと紐づけて登録
    app.route(Name.from_str(PREFIX))(on_interest)
    
    logging.info(f"Starting Manager NDNApp... Waiting for Interests on {PREFIX}")
    
    # 2. NDNAppの通信エンジンを起動
    # （NFDへのTCP接続、Prefixの自動登録、永遠にInterestを待機するループをすべて自動で行います）
    try:
        app.run_forever()
    except KeyboardInterrupt:
        logging.info("Manager stopped by user.")