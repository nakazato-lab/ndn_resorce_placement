from ndn.app import NDNApp
from ndn.types import InterestNack, InterestTimeout, InterestCanceled, ValidationFailure
import argparse

from ndn_utils import send_interest

app = NDNApp()


async def main(name):
    try:
        content = await send_interest(app, name)
        print(content.decode('utf-8') if content else None)
    except InterestNack as e:
        print(f'Nacked with reason={e.reason}')
    except InterestTimeout:
        print(f'Timeout')
    except InterestCanceled:
        print(f'Canceled')
    except ValidationFailure:
        print(f'Data failed to validate')
    finally:
        app.shutdown()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("name", help="Name of the interest")
    args = parser.parse_args()
    app.run_forever(after_start=main(args.name))