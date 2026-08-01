from typing import Callable, Optional
from ndn.app import NDNApp
from ndn.encoding import Name, InterestParam, BinaryStr, FormalName, MetaInfo
import logging

import os


logging.basicConfig(format='[{asctime}]{levelname}:{message}',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    level=logging.INFO,
                    style='{')


class NDNProducer:
    def __init__(self):
        self.app = NDNApp()

    def run(self, prefix: str, data_request_handler: Callable[[str], str]):
        """
        プレフィックスに対してデータハンドラを登録して起動
        Args:
            prefix (str): プレフィックス
            data_request_handler (Callable[[str], str]): データハンドラ (名前) -> データ
        """
        # nlsrc advertise [prefix] というコマンドで prefix を広告
        os.system(f"nlsrc advertise {prefix}")

        @self.app.route(prefix)
        def on_interest(name: FormalName, param: InterestParam, _app_param: Optional[BinaryStr]):
            print(f'>> I: {Name.to_str(name)}, {param}')
            content = data_request_handler(Name.to_str(name))
            content = content.encode()
            self.app.put_data(name, content=content, freshness_period=1)
            print(f'<< D: {Name.to_str(name)}')
            print(MetaInfo(freshness_period=10000))
            print(f'Content: (size: {len(content)})')
            print('')

        self.app.run_forever()


def on_interest(name: str) -> str:
    print(f"Interest: {name}")
    return "Hello, world!!!!!!"

if __name__ == '__main__':
    producer = NDNProducer()
    producer.run('/nodeX', on_interest)
    