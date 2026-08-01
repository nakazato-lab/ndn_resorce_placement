import os
import sys
import json
import logging
import asyncio
import psutil
import docker
from ndn.app import NDNApp
from ndn.encoding import Name, Component

# ロギング設定
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')

app = NDNApp()

# 静的情報の取得
MY_NODE_NAME = os.environ.get("NODE_NAME", "producer_1")
CPU_MODEL = os.environ.get("CPU_MODEL", "Intel Core i7-2026")
PASSMARK = int(os.environ.get("PASSMARK", "12000"))
MAX_MEM = int(os.environ.get("MAX_MEM", "4096"))  # MB単位

def get_dynamic_resource():
    """
    親コンテナ（自身）の動的リソース情報を取得
    """
    # CPU使用率 (interval=0.1 で短い期間の平均をとる)
    cpu_usage = psutil.cpu_percent(interval=0.1)
    
    # メモリ使用率 (親コンテナのメモリ使用量を推定)
    mem = psutil.virtual_memory()
    mem_usage = mem.percent
    
    return cpu_usage, mem_usage

@app.route(f'/spy/{MY_NODE_NAME}')
def on_spy_interest(name, interest_param, app_param):
    name_str_list = [Component.to_str(c) for c in name]
    operation = name_str_list[2] if len(name_str_list) > 2 else ""

    if operation == 'resource':
        cpu, mem = get_dynamic_resource()
        
        # スコア計算
        availability = (100.0 - cpu) / 100.0
        score = int(PASSMARK * availability)
        
        payload = {
            "node_name": MY_NODE_NAME,
            "cpu_usage": f"{cpu}%",
            "memory_usage": f"{mem}%",
            "score": score
        }
        app.put_data(name, content=json.dumps(payload).encode('utf-8'), freshness_period=1)

    
    # elif operation == 'create_child':
    #     # 子コンテナ生成ロジック (docker-py使用)
    #     child_name = name_str_list[3]
    #     cpu_shares = int(name_str_list[4])
    #     mem_limit = name_str_list[5]

    #     try:
    #         client = docker.from_env()
    #         container = client.containers.run(
    #             image="dockerprac-ndn-worker-task",
    #             name=child_name,
    #             detach=True,
    #             cpu_shares=cpu_shares,
    #             mem_limit=mem_limit
    #         )
    #         app.put_data(name, content=f"Created {child_name}".encode(), freshness_period=1)
    #     except Exception as e:
    #         app.put_data(name, content=str(e).encode(), freshness_period=1)


async def main():
    logging.info(f"Spy process started on {MY_NODE_NAME}")
    await app.main_loop()

if __name__ == '__main__':
    asyncio.run(main())