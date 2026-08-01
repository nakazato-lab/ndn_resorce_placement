from asyncio import subprocess
import asyncio
import json
import time
from typing import Optional
import urllib.parse
from ndn.encoding import Name, FormalName
from ndn.app import NDNApp
from ndn.types import InterestNack, InterestTimeout, InterestCanceled, ValidationFailure
from ndn.encoding import Name

# ()を使う関係上、nameをデコードし、またメタデータを削除する
# Name.to_str(name)でデコードした場合、/A/%28B%29/t=12411 のようになっているため、/A/(〇〇)/t=12411 のようにデコードする
# /A/(〇〇)/t=12411 のようになる構造上、最後の)以降を削除すれば良い
# /A/data などのデータリクエストの場合は、/t=だけ消して、そのまま返す
def decode_and_remove_metadata(name: FormalName) -> str:
    # 名前をデコード
    decoded_name = Name.to_str(name)
    decoded_name = urllib.parse.unquote(decoded_name)
    
    # 最後の ')' の位置を見つける
    end_index = decoded_name.rfind(')')
    if end_index != -1 and len(decoded_name) > end_index + 1 and decoded_name[end_index + 1] == '/':
        # 最後の　)以降の部分を削除
        decoded_name = decoded_name[:end_index + 1]

    # /t= がある場合があるので削除
    end_index = decoded_name.rfind('/t=')
    if end_index != -1:
        decoded_name = decoded_name[:end_index]

    return decoded_name


# name 構造から、function リクエストかを調べる
# /A/(〇〇) のような構造であれば function リクエストなので、/(が存在すれば関数と判断する
# そうでなければ、data リクエストなので、データを返す
def is_function_request(name: FormalName) -> bool:
    decoded_url = decode_and_remove_metadata(name)
    return "/(" in decoded_url

# function リクエストから、第一階層の引数を抽出する
def extract_first_level_args(name: FormalName) -> list[str]:
    decoded_url = decode_and_remove_metadata(name)
    
    def trim(s: str) -> str:
        return s.strip()

    args = []
    if '(' not in decoded_url:
        args.append(trim(decoded_url))
        return args

    # Correctly extract the function name
    end_of_function_name = decoded_url.find('/(')
    start_of_args = end_of_function_name + 2
    args_str = decoded_url[start_of_args:-1]

    start = 0
    bracket_count = 0
    for i, char in enumerate(args_str):
        if char == '(':
            bracket_count += 1
        elif char == ')':
            bracket_count -= 1
        elif char == ',' and bracket_count == 0:
            args.append(trim(args_str[start:i]))
            start = i + 1

    args.append(trim(args_str[start:]))
    return args

# interestを送る
async def send_interest(app: NDNApp, name: str) -> Optional[bytes]:
    try:
        name = Name.from_str(name)
        data_name, _meta_info, content = await app.express_interest(
            name, must_be_fresh=True, can_be_prefix=False, lifetime=10000)

        return bytes(content) if content else None
    except InterestNack as e:
        print(f'!!!ERROR!!!: Nacked with reason={e.reason}')
    except InterestTimeout:
        print(f'!!!ERROR!!!: Timeout')
    except InterestCanceled:
        print(f'!!!ERROR!!!: Canceled')
    except ValidationFailure:
        print(f'!!!ERROR!!!: Data failed to validate')

# ApplicationParameters(JSON)付きでInterestを送る
async def send_interest_with_params(app: NDNApp, name: str, params: dict) -> Optional[bytes]:
    try:
        params_with_ts = {**params, "_ts": int(time.time() * 1000)}
        name_obj = Name.from_str(name)
        _, _meta_info, content = await app.express_interest(
            name_obj,
            app_param=json.dumps(params_with_ts).encode(),
            must_be_fresh=False,
            can_be_prefix=False,
            lifetime=10000)

        return bytes(content) if content else None
    except InterestNack as e:
        print(f'!!!ERROR!!!: Nacked with reason={e.reason}')
    except InterestTimeout:
        print(f'!!!ERROR!!!: Timeout')
    except InterestCanceled:
        print(f'!!!ERROR!!!: Canceled')
    except ValidationFailure:
        print(f'!!!ERROR!!!: Data failed to validate')

# 別プロセスから無理やりInterestを送信
async def send_interest_process(name: str) -> Optional[bytes]:
    try:
        # 非同期プロセスを起動してInterestを送信
        process = await asyncio.create_subprocess_exec(
            'python3', './ndn_clients/lib/consumer_args.py', name,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        
        stdout, stderr = await process.communicate()

        if process.returncode == 0:
            # 成功した場合、stdoutからデータを取得し、改行を削除して返す
            return bytes(stdout).strip()
        else:
            print(f'!!!ERROR!!!: {stderr.decode()}')
            return None
    except Exception as e:
        print(f'!!!ERROR!!!: {e}')
        return None

# function リクエストから、自分の関数名を取得する
# /A/(/hoge,/B/func(/hoge)) のような構造であれば、/A が関数名になる
def extract_my_function_name(name: FormalName) -> str:
    decoded_url = decode_and_remove_metadata(name)
    end_of_function_name = decoded_url.find('/(')
    return decoded_url[:end_of_function_name]

if __name__ == '__main__':
    data = asyncio.run(send_interest_process('/nodeA/hoge'))
    print(data)