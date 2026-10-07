import json
import logging
import os
from pathlib import Path

import psutil
from ndn.encoding import Component

from ndn_routing import create_app
from network_metrics import NetworkMonitor

LOG = logging.getLogger(__name__)


def resource_score(usage):
    return max(0, int((80 - usage) / 80 * 100))


class Spy:
    def __init__(self, app, node_name, network=None):
        self.app = app
        self.node_name = node_name
        self.network = network

    def get_resource_payload(self):
        cpu = psutil.cpu_percent(interval=0.1)
        memory = psutil.virtual_memory().percent
        return json.dumps({
            'cpu_score': resource_score(cpu),
            'mem_score': resource_score(memory),
            'network': self.network.snapshot if self.network else None,
        }).encode('utf-8')

    def on_interest(self, name, interest_param, app_param):
        if Component.to_str(name[-1]) == 'resource':
            self.app.put_data(name, content=self.get_resource_payload(), freshness_period=1000)


def main():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
    if Path('/host/proc').exists():
        psutil.PROCFS_PATH = '/host/proc'
    node_name = os.environ.get('NODE_NAME', 'kube1').strip()
    app = create_app()
    network = NetworkMonitor(os.environ.get('NODE_EXPORTER_URL',
                                           'http://127.0.0.1:9100/metrics'))
    spy = Spy(app, node_name, network)
    app.route(f'/{spy.node_name}/spy')(spy.on_interest)
    network.start()
    try:
        app.run_forever()
    except KeyboardInterrupt:
        LOG.info('Spy stopped')
    finally:
        network.stop()


if __name__ == '__main__':
    main()
