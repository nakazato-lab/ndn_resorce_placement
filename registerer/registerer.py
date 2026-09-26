import os
import json
import logging
import asyncio

from ndn.app import NDNApp
from ndn.encoding import Name
from ndn.transport.stream_face import TcpFace
from ndn.security import KeychainDigest
from ndn.types import InterestNack, InterestTimeout, InterestCanceled, ValidationFailure

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')

MANAGER_REGISTER_NAME = "/Manager/register"
FUNCTION_PATH = os.path.join(os.path.dirname(__file__), "average.ndn")
FUNCTION_NAME = os.path.splitext(os.path.basename(FUNCTION_PATH))[0]
CONTENT_TYPE = "ndn"

# NFD_ADDRの取得とポート分割処理（manager/ndn_manager.pyと同じロジック）
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


async def register_function():
    with open(FUNCTION_PATH, "r", encoding="utf-8") as f:
        content = f.read()

    params = {
        "name": FUNCTION_NAME,
        "content": content,
        "content_type": CONTENT_TYPE,
    }
    app_param = json.dumps(params).encode("utf-8")

    logging.info(f"Sending register Interest: {MANAGER_REGISTER_NAME} (name={FUNCTION_NAME}, file={FUNCTION_PATH})")

    try:
        _, _, content_resp = await app.express_interest(
            Name.from_str(MANAGER_REGISTER_NAME),
            app_param=app_param,
            must_be_fresh=True,
            can_be_prefix=False,
            lifetime=6000)
        logging.info(f"Manager response: {bytes(content_resp).decode('utf-8')}")
    except InterestNack as e:
        logging.error(f"Nacked with reason={e.reason}")
    except InterestTimeout:
        logging.error("Timeout waiting for Manager response")
    except InterestCanceled:
        logging.error("Interest canceled")
    except ValidationFailure:
        logging.error("Data failed to validate")
    finally:
        app.shutdown()


if __name__ == '__main__':
    app.run_forever(after_start=register_function())
