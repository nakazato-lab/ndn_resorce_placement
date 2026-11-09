import json
import logging
import os
from pathlib import Path

import psutil
from ndn.encoding import Component

from ndn_routing import create_app
from network_metrics import NetworkMonitor

LOG = logging.getLogger(__name__)


# 参考文献に基づき、CPU等のリソースは75〜80%を超えると処理能力の低下がみられるため、
# 80%を安全上限(実効最大容量)として空き容量・割合を評価する
SAFE_CAPACITY_LIMIT = float(os.getenv('SAFE_CAPACITY_LIMIT', '80.0'))


def resource_score(usage, limit=SAFE_CAPACITY_LIMIT):
    return max(0, int((limit - usage) / limit * 100))


def effective_free_ratio(usage_pct, limit=SAFE_CAPACITY_LIMIT):
    return max(0.0, round((limit - usage_pct) / limit, 4))


class Spy:
    def __init__(self, app, node_name, network=None):
        self.app = app
        self.node_name = node_name
        self.network = network

    def get_resource_payload(self):
        # 1. CPU (psutil)
        total_cores = float(psutil.cpu_count(logical=True) or 1)
        cpu_usage_pct = float(psutil.cpu_percent(interval=0.1))
        used_cores = round((cpu_usage_pct / 100.0) * total_cores, 2)
        free_cores = max(0.0, round(total_cores - used_cores, 2))
        cpu_usage_ratio = round(used_cores / total_cores, 4)
        cpu_free_ratio = round(free_cores / total_cores, 4)

        # 2. メモリ (psutil)
        vmem = psutil.virtual_memory()
        total_mem_bytes = int(vmem.total)
        free_mem_bytes = int(vmem.available)
        used_mem_bytes = max(0, total_mem_bytes - free_mem_bytes)
        mem_usage_pct = float(vmem.percent)
        mem_usage_ratio = round(used_mem_bytes / total_mem_bytes, 4) if total_mem_bytes else 0.0
        mem_free_ratio = round(free_mem_bytes / total_mem_bytes, 4) if total_mem_bytes else 1.0

        # 3. 帯域 (network_metrics)
        try:
            bw_info = self.network.get_bandwidth_metrics() if self.network else None
        except Exception as exc:
            LOG.warning('Network metrics unavailable: %s', exc)
            bw_info = None

        if not bw_info:
            capacity_bps = float(1_000_000_000)
            bw_info = {
                'capacity_bps': capacity_bps,
                'used_bps': 0.0,
                'free_bps': capacity_bps,
                'usage_percent': 0.0,
                'usage_ratio': 0.0,
                'free_ratio': 1.0,
            }

        # 80%安全上限モデルに基づく実効空き割合 (80%以上で実質空き0)
        cpu_effective_free_ratio = effective_free_ratio(cpu_usage_pct)
        mem_effective_free_ratio = effective_free_ratio(mem_usage_pct)
        bw_effective_free_ratio = effective_free_ratio(bw_info['usage_percent'])

        # 80%安全上限に基づく実効空き絶対量
        limit_ratio = SAFE_CAPACITY_LIMIT / 100.0
        cpu_effective_free_cores = max(0.0, round(total_cores * limit_ratio - used_cores, 2))
        mem_effective_free_bytes = max(0, int(total_mem_bytes * limit_ratio - used_mem_bytes))
        bw_effective_free_bps = max(0.0, bw_info['capacity_bps'] * limit_ratio - bw_info['used_bps'])

        payload = {
            # 80%安全上限モデルに基づく実効空き割合 (0.0〜1.0, 80%使用で0.0)
            'cpu_free_ratio': cpu_effective_free_ratio,
            'mem_free_ratio': mem_effective_free_ratio,
            'bandwidth_free_ratio': bw_effective_free_ratio,

            # 使用割合 (0.0〜1.0)
            'cpu_usage_ratio': cpu_usage_ratio,
            'mem_usage_ratio': mem_usage_ratio,
            'bandwidth_usage_ratio': bw_info['usage_ratio'],

            # 汎用キー (実効空き割合を採用)
            'cpu_ratio': cpu_effective_free_ratio,
            'mem_ratio': mem_effective_free_ratio,
            'bandwidth_ratio': bw_effective_free_ratio,

            # 詳細な絶対値オブジェクト (コア数, バイト数, bps)
            'cpu': {
                'total_cores': total_cores,
                'used_cores': used_cores,
                'free_cores': free_cores,
                'effective_free_cores': cpu_effective_free_cores,
                'usage_percent': cpu_usage_pct,
                'usage_ratio': cpu_usage_ratio,
                'free_ratio': cpu_effective_free_ratio,
                'raw_free_ratio': cpu_free_ratio,
            },
            'memory': {
                'total_bytes': total_mem_bytes,
                'used_bytes': used_mem_bytes,
                'free_bytes': free_mem_bytes,
                'effective_free_bytes': mem_effective_free_bytes,
                'usage_percent': mem_usage_pct,
                'usage_ratio': mem_usage_ratio,
                'free_ratio': mem_effective_free_ratio,
                'raw_free_ratio': mem_free_ratio,
            },
            'bandwidth': {
                'capacity_bps': bw_info['capacity_bps'],
                'used_bps': bw_info['used_bps'],
                'free_bps': bw_info['free_bps'],
                'effective_free_bps': bw_effective_free_bps,
                'usage_percent': bw_info['usage_percent'],
                'usage_ratio': bw_info['usage_ratio'],
                'free_ratio': bw_effective_free_ratio,
                'raw_free_ratio': bw_info['free_ratio'],
            },

            # 互換用スコア
            'cpu_score': resource_score(cpu_usage_pct),
            'mem_score': resource_score(mem_usage_pct),
            'bandwidth_score': resource_score(bw_info['usage_percent']),
        }
        return json.dumps(payload, ensure_ascii=False).encode('utf-8')

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
    try:
        app.run_forever()
    except KeyboardInterrupt:
        LOG.info('Spy stopped')


if __name__ == '__main__':
    main()
