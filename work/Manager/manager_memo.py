from typing import Callable, Optional
from ndn.app import NDNApp
from ndn.encoding import Name, InterestParam, BinaryStr, FormalName, MetaInfo
import os
import json
import asyncio
from datetime import datetime

from lib.ndn_utils import send_interest, send_interest_with_params

class Manager:
    def __init__(self):
        self.app = NDNApp()
        
        # ユーザーごとの静的な所有リソースリスト（既存維持）
        self.resource_lists = [
            {"user_id": "userA", "resources": ["/data/profile.txt", "/func/calc_sum"]},
            {"user_id": "userB", "resources": ["/data/photos/", "/func/get_location"]},
            {"user_id": "userC", "resources": []}
        ]

        self.db_filename = "node_db.json"

    def record_spy_resource(self, spy_json_bytes: bytes):
        """
        【ご要望の方針】スパイから（照会などで）聞いた時点での動的リソース状況を
        node_db.json にタイムスタンプ付きで追記・記録する
        """
        try:
            spy_data = json.loads(spy_json_bytes.decode('utf-8'))
            
            # スパイのベースコードが返してくれる全パラメーターをそのままマッピング
            log_entry = {
                "timestamp": datetime.now().isoformat(),
                "node_name": spy_data.get("node_name"),
                "cpu_model": spy_data.get("cpu_model"),
                "cpu_performance": spy_data.get("cpu_performance"),
                "gpu_performance": spy_data.get("gpu_performance"),
                "max_memory": spy_data.get("max_memory"),
                "cpu_usage": spy_data.get("cpu_usage"),
                "memory_usage": spy_data.get("memory_usage"),
                "score_no_tdp": spy_data.get("score_no_tdp"),
                "score_with_tdp": spy_data.get("score_with_tdp")
            }
            
            data = []
            if os.path.exists(self.db_filename):
                try:
                    with open(self.db_filename, "r", encoding="utf-8") as f:
                        data = json.load(f)
                        if not isinstance(data, list):
                            data = []
                except Exception:
                    data = []
            
            data.append(log_entry)
            with open(self.db_filename, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=4, ensure_ascii=False)
                
            print(f"★ {self.db_filename} にスパイの動的リソースを追記しました: {log_entry['node_name']} (Score: {log_entry['score_no_tdp']})")
            return "Success"
        except Exception as e:
            print(f"!! スパイデータのパースまたは書き込みに失敗しました: {e}")
            return "Failed"

    # --- 既存の検索・追加・削除ロジック（そのまま維持） ---
    def search_user_from_userid(self, user_id : str) -> bool:
        for user_data in self.resource_lists:
            if user_data.get("user_id") == user_id: return True
        return False
    
    def search_resource_from_list(self, resource_name : str) -> bool:
        for user_data in self.resource_lists:
            if "resources" in user_data and resource_name in user_data["resources"]: return True
        return False
    
    def add_user(self, user_id : str) -> str:
        if self.search_user_from_userid(user_id): return "Failed"
        self.resource_lists.append({"user_id": user_id, "resources": []})
        return "Success"
    
    def add_resource_to_user(self, user_id : str, new_resource : str) -> str:
        for user_data in self.resource_lists:
            if user_data.get("user_id") == user_id:
                if new_resource in user_data.get("resources", []): return "Failed"
                user_data.setdefault("resources", []).append(new_resource)
                return "Success"
        return "Failed"

    def remove_user(self, user_id : str) -> str:
        for i, user_data in enumerate(self.resource_lists):
            if user_data.get("user_id") == user_id:
                del self.resource_lists[i]
                return "Success"
        return "Failed"
    
    def remove_resource_from_user(self, user_id : str, remove_resource : str) -> str:
        for user_data in self.resource_lists:
            if user_data.get("user_id") == user_id:
                resources = user_data.get("resources", [])
                if resources and remove_resource in resources:
                    resources.remove(remove_resource)
                    return "Success"
        return "Failed"

    def run(self, prefix: str, data_request_handler: Callable[[str], str]):
        """プレフィックスに対してデータハンドラを登録して起動（同期版）"""
        # 1. 外部から呼び出される同期関数を登録
        @self.app.route(prefix)
        def on_interest(name: FormalName, param: InterestParam, app_param: Optional[BinaryStr]):
            str_name = Name.to_str(name)
            print(f'>> I: {str_name}')
            
            # 2. 非同期タスクとして処理をキックする
            # これにより、ライブラリ側は同期的な戻り値(None)を受け取り、警告が出ない
            asyncio.create_task(self.async_process_interest(name, str_name, app_param))

        print(f"Manager started on {prefix}")
        self.app.run_forever()

    async def async_process_interest(self, name: FormalName, str_name: str, app_param: Optional[BinaryStr] = None):
        try:
            # ここで本来の処理を呼ぶ
            content = await self.on_interest(str_name, app_param)
            
            # 結果の送信
            self.app.put_data(name, content=content.encode('utf-8'), freshness_period=1)
            print(f'<< D: {str_name}')
            print(MetaInfo(freshness_period=10000))
            print('')
        except Exception as e:
            print(f"!! Error in async_process: {e}")

    async def on_interest(self, name: str, app_param: Optional[BinaryStr] = None) -> str:
        """Interest名に基づき、Managerの各種操作にルーティング"""
        print(f"Processing: {name}")
        cleaned_name = name.removeprefix('/manager')
        split_name = cleaned_name.strip('/').split('/') 

        if len(split_name) < 2:
            return "Error: Too few components."
        
        operation = split_name[0]     # search, add, delete, create, register
        target_type = split_name[1]   # user, resource

# --- .ndn関数の登録要求受付 ---
        # 形式: /manager/register（ペイロードはNameではなくApplicationParametersのJSONに載せる）
        if operation == 'register':
            if not app_param:
                return "Error: REGISTER requires ApplicationParameters"
            try:
                req = json.loads(bytes(app_param).decode('utf-8'))
            except json.JSONDecodeError as e:
                return f"Error: Invalid JSON in ApplicationParameters ({e})"

            func_name = req.get("name")
            content = req.get("content")
            content_type = req.get("content_type", "ndn")

            if not func_name or not content:
                return "Error: 'name' and 'content' are required"

            # TODO: node_db.jsonのスコアから配置先Seedを選ぶ(_select_seed_node)。今は仮で固定prefixに転送する
            seed_prefix = "/seed"
            forward_params = {"type": "CREATE", "name": func_name, "content": content, "content_type": content_type}
            print(f"-> Redirecting Seed with Interest: {seed_prefix} (name={func_name})")
            try:
                seed_content = await send_interest_with_params(self.app, seed_prefix, forward_params)

                if seed_content:
                    seed_response = bytes(seed_content).decode('utf-8')
                    return f"Manager_Proxy_Success: {seed_response}"
                else:
                    return "Manager_Proxy_Error: Seedから空のデータが返されました"

            except Exception as e:
                return f"Manager_Proxy_Error: Seedへの送信に失敗しました ({e})"

        # --- 子コンテナ作成要求の受付（頭脳に徹し、スパイへリダイレクト案内） ---
        # 形式: /manager/create/<worker_id>/<container_name>/[cpu_shares]/[mem_limit]
        # 例: /manager/create/producer_1/task-calc-sum/512/256m
        if operation == 'create':
            if len(split_name) < 3: return "Error: Invalid create format."
            worker_id = split_name[1]
            container_name = split_name[2]
            shares = split_name[3] if len(split_name) >= 4 else "1024"
            memory = split_name[4] if len(split_name) >= 5 else "512m"
            
            # スパイのベースコード仕様（/spy/{NODE_NAME}/create_child/...）へ完全対応
            spy_destination = f"/spy/{worker_id}/create_child/{container_name}/{shares}/{memory}"
            print(f"-> Redirecting Spy with Interest: {spy_destination}")
            try:
                # 第1引数にManager自身の持つ self.app を渡し、第2引数にSpyのNameを渡す
                spy_content = await send_interest(self.app, spy_destination)
                
                if spy_content:
                    # 受信したバイナリデータを文字列にデコード
                    spy_response = bytes(spy_content).decode('utf-8')
                    return f"Manager_Proxy_Success: {spy_response}"
                else:
                    return "Manager_Proxy_Error: Spyから空のデータが返されました"
                    
            except Exception as e:
                # タイムアウトやNackなどの例外をキャッチ
                return f"Manager_Proxy_Error: Spyへの送信に失敗しました ({e})"

        # --- 既存のCRUD・検索ロジック ---
        result = "Error: Unknown operation"
        if operation == 'search':
            if target_type == 'user' and len(split_name) == 3:
                result = f"UserExists:{'True' if self.search_user_from_userid(split_name[2]) else 'False'}"
            elif target_type == 'resource' and len(split_name) >= 3:
                result = f"ResourceExists:{'True' if self.search_resource_from_list('/' + '/'.join(split_name[2:])) else 'False'}"
        
        elif operation == 'add':
            if target_type == 'user' and len(split_name) == 3:
                result = self.add_user(split_name[2])
            elif target_type == 'resource' and len(split_name) >= 4:
                result = self.add_resource_to_user(split_name[2], '/' + '/'.join(split_name[3:]))

        elif operation == 'delete':
            if target_type == 'user' and len(split_name) == 3:
                result = self.remove_user(split_name[2])
            elif target_type == 'resource' and len(split_name) >= 4:
                result = self.remove_resource_from_user(split_name[2], '/' + '/'.join(split_name[3:]))
            
        if operation in ['add', 'delete'] and result == 'Success':
            print(f"\n--- Current User List ---\n{json.dumps(self.resource_lists, indent=2)}\n---------------------\n")

        return result

if __name__ == '__main__':
    manager = Manager()
    manager.run('/manager', manager.on_interest)