import asyncio
import os
from typing import Callable, Optional
from ndn.app import NDNApp
from ndn.encoding import Name, InterestParam, BinaryStr, FormalName
import logging

from lib.ndn_utils import extract_first_level_args, extract_my_function_name, is_function_request, send_interest_process
        
logging.basicConfig(format='[{asctime}]{levelname}:{message}',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    level=logging.INFO,
                    style='{')


class NDNFunction:
    def __init__(self):
        self.app = NDNApp()

    def run(self, prefix: str, function_request_handler: Callable[[str, list[bytes]], bytes], data_request_handler: Callable[[str], str]):
        """
        プレフィックスに対して関数ハンドラとデータハンドラを登録して起動
        Args:
            prefix (str): プレフィックス
            function_handler (Callable[[str, list[bytes]], bytes]): 関数ハンドラ (関数名, 引数) -> 結果
            data_request_handler (Callable[[str], str]): データハンドラ (名前) -> データ
        """

        # nlsrc advertise [prefix] というコマンドで prefix を広告
        os.system(f"nlsrc advertise {prefix}")
        
        @self.app.route(prefix)
        def on_interest(name: FormalName, param: InterestParam, _app_param: Optional[BinaryStr]):
            print(f'>> I: {Name.to_str(name)}, {param}')
            async def async_on_interest():
                # function リクエストでない場合は、データリクエストとして処理
                if not is_function_request(name):
                    content = data_request_handler(Name.to_str(name))
                    content = content.encode()
                    self.app.put_data(name, content=content, freshness_period=10000)
                    return

                # function リクエストの場合は、まず第一階層の引数を抽出
                args = extract_first_level_args(name)

                # それぞれを interest で並列でリクエストし、データを集める
                async def fetch_content(arg):
                    return await send_interest_process(arg)
                tasks = [fetch_content(arg) for arg in args]
                contents = await asyncio.gather(*tasks)

                print(f"データが集まりました: {contents}")

                # 自身の関数名を取得 
                my_function_name = extract_my_function_name(name)

                print(f"Function名: {my_function_name}")

                # 関数を実行
                result = function_request_handler(my_function_name, contents)

                print(f"実行結果: {result}")

                # 結果を返す
                self.app.put_data(name, content=result, freshness_period=10000)

            # 現在のイベントループを取得
            loop = asyncio.get_event_loop()
            if loop.is_running():
                # イベントループがすでに走っている場合はタスクとしてスケジュール
                loop.create_task(async_on_interest())
            else:
                # イベントループが走っていない場合はrunで実行
                loop.run_until_complete(async_on_interest())

        self.app.run_forever()


def function_request_handler(name: str, args: list[bytes]) -> bytes:
    args = [arg.decode() for arg in args]
    args = ",".join(args)
    return f"Hello, {name}! Args: {args}".encode()

def data_request_handler(name: str) -> str:
    print(f"Interest: {name}")
    return "Hello, world!!!!!!"

if __name__ == '__main__':
    producer = NDNFunction()
    producer.run('/func_nodeX', function_request_handler, data_request_handler)
    