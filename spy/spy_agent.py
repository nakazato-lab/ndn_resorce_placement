import asyncio
import collections
import json
import logging
import os
import time
from pathlib import Path

import psutil
from ndn.encoding import Component, Name

from ndn_routing import create_app
from network_metrics import NetworkMonitor, MAX_BANDWIDTH_BPS

LOG = logging.getLogger(__name__)

# 参考文献に基づき、CPU等のリソースは75〜80%を超えると処理能力の低下がみられるため、
# 80%を安全上限(実効最大容量)として空き容量・割合を評価する
SAFE_CAPACITY_LIMIT = float(os.getenv('SAFE_CAPACITY_LIMIT', '80.0'))

# バックグラウンドサンプリング設定:
# 0.5秒ごとにCPU・メモリ・帯域を並列サンプリングし、直近5秒間(10サンプル)の単純移動平均(SMA)を保持
SAMPLE_INTERVAL = float(os.getenv('SPY_SAMPLE_INTERVAL', '0.5'))
WINDOW_SECONDS = float(os.getenv('SPY_WINDOW_SECONDS', '5.0'))
MAX_SAMPLES = max(1, int(WINDOW_SECONDS / SAMPLE_INTERVAL))


def resource_score(usage, limit=SAFE_CAPACITY_LIMIT):
    return max(0, int((limit - usage) / limit * 100))


def effective_free_ratio(usage_pct, limit=SAFE_CAPACITY_LIMIT):
    return max(0.0, round((limit - usage_pct) / limit, 4))


class Spy:
    def __init__(self, app, node_name, network=None):
        self.app = app
        self.node_name = node_name
        self.network = network

        # ハードウェア総量（不変値）: 各ノードのキャパシティ
        self.total_cores = float(psutil.cpu_count(logical=True) or 1)
        vmem = psutil.virtual_memory()
        self.total_mem_bytes = int(vmem.total)
        self.capacity_bps = float(MAX_BANDWIDTH_BPS)

        # 直近5秒間の単純移動平均用リングバッファ (0.5秒 * 10 = 5秒)
        self.samples = collections.deque(maxlen=MAX_SAMPLES)
        self.current_payload = None

        # 初回サンプリングのベースライン作成 (差分測定の準備)
        psutil.cpu_percent(interval=None)
        if self.network:
            try:
                self.network.sample_instant()
            except Exception:
                pass

    async def _sample_once(self):
        """CPU, メモリ, 帯域の3つのリソースを並列にサンプリングする。"""
        loop = asyncio.get_running_loop()

        def get_cpu():
            return float(psutil.cpu_percent(interval=None))

        def get_mem():
            vm = psutil.virtual_memory()
            used = max(0, int(vm.total) - int(vm.available))
            return {
                'percent': float(vm.percent),
                'used_bytes': used,
                'available_bytes': int(vm.available),
            }

        def get_bw():
            if self.network:
                try:
                    return self.network.sample_instant()
                except Exception as exc:
                    LOG.warning('Network sampling error: %s', exc)
            return {
                'used_bps': 0.0,
                'usage_percent': 0.0,
                'capacity_bps': self.capacity_bps,
            }

        # 3つのリソース収集をスレッドプールで同時に並行実行
        cpu_fut = loop.run_in_executor(None, get_cpu)
        mem_fut = loop.run_in_executor(None, get_mem)
        bw_fut = loop.run_in_executor(None, get_bw)

        cpu_pct, mem_info, bw_info = await asyncio.gather(cpu_fut, mem_fut, bw_fut)

        sample = {
            'cpu_pct': cpu_pct,
            'mem_pct': mem_info['percent'],
            'mem_used_bytes': mem_info['used_bytes'],
            'mem_free_bytes': mem_info['available_bytes'],
            'bw_used_bps': bw_info.get('used_bps', 0.0),
            'bw_pct': bw_info.get('usage_percent', 0.0),
        }
        self.samples.append(sample)
        self._update_payload()

    def _update_payload(self):
        """直近サンプル (最大10件=5秒分) の単純移動平均 (SMA) を算出し、Dataパケット用JSONを更新する。"""
        if not self.samples:
            return

        n = len(self.samples)
        # 単純移動平均 (SMA)
        avg_cpu_pct = sum(s['cpu_pct'] for s in self.samples) / n
        avg_mem_pct = sum(s['mem_pct'] for s in self.samples) / n
        avg_used_mem_bytes = int(sum(s['mem_used_bytes'] for s in self.samples) / n)
        avg_free_mem_bytes = max(0, self.total_mem_bytes - avg_used_mem_bytes)
        avg_bw_used_bps = sum(s['bw_used_bps'] for s in self.samples) / n
        avg_bw_pct = sum(s['bw_pct'] for s in self.samples) / n

        # 1. CPU (直近5秒平均)
        used_cores = round((avg_cpu_pct / 100.0) * self.total_cores, 2)
        free_cores = max(0.0, round(self.total_cores - used_cores, 2))
        cpu_usage_ratio = round(used_cores / self.total_cores, 4)
        cpu_free_ratio = round(free_cores / self.total_cores, 4)

        # 2. メモリ (直近5秒平均)
        mem_usage_ratio = round(avg_used_mem_bytes / self.total_mem_bytes, 4) if self.total_mem_bytes else 0.0
        mem_free_ratio = round(avg_free_mem_bytes / self.total_mem_bytes, 4) if self.total_mem_bytes else 1.0

        # 3. 帯域 (直近5秒平均)
        bw_free_bps = max(0.0, self.capacity_bps - avg_bw_used_bps)
        bw_usage_ratio = round(avg_bw_used_bps / self.capacity_bps, 4) if self.capacity_bps else 0.0
        bw_free_ratio = round(bw_free_bps / self.capacity_bps, 4) if self.capacity_bps else 1.0

        # 80%安全上限モデルに基づく実効空き割合 (80%以上で実質空き0)
        cpu_effective_free_ratio = effective_free_ratio(avg_cpu_pct)
        mem_effective_free_ratio = effective_free_ratio(avg_mem_pct)
        bw_effective_free_ratio = effective_free_ratio(avg_bw_pct)

        # 80%安全上限に基づく実効空き絶対量
        limit_ratio = SAFE_CAPACITY_LIMIT / 100.0
        cpu_effective_free_cores = max(0.0, round(self.total_cores * limit_ratio - used_cores, 2))
        mem_effective_free_bytes = max(0, int(self.total_mem_bytes * limit_ratio - avg_used_mem_bytes))
        bw_effective_free_bps = max(0.0, round(self.capacity_bps * limit_ratio - avg_bw_used_bps, 2))

        payload = {
            # 80%安全上限モデルに基づく実効空き割合 (直近5秒平均)
            'cpu_free_ratio': cpu_effective_free_ratio,
            'mem_free_ratio': mem_effective_free_ratio,
            'bandwidth_free_ratio': bw_effective_free_ratio,

            # 使用割合 (直近5秒平均)
            'cpu_usage_ratio': cpu_usage_ratio,
            'mem_usage_ratio': mem_usage_ratio,
            'bandwidth_usage_ratio': bw_usage_ratio,

            # 汎用キー (実効空き割合を採用)
            'cpu_ratio': cpu_effective_free_ratio,
            'mem_ratio': mem_effective_free_ratio,
            'bandwidth_ratio': bw_effective_free_ratio,

            # 詳細な絶対値オブジェクト (総量はハードウェア固定値、使用量・空き量は直近5秒平均)
            'cpu': {
                'total_cores': self.total_cores,
                'used_cores': used_cores,
                'free_cores': free_cores,
                'effective_free_cores': cpu_effective_free_cores,
                'usage_percent': round(avg_cpu_pct, 2),
                'usage_ratio': cpu_usage_ratio,
                'free_ratio': cpu_effective_free_ratio,
                'raw_free_ratio': cpu_free_ratio,
            },
            'memory': {
                'total_bytes': self.total_mem_bytes,
                'used_bytes': avg_used_mem_bytes,
                'free_bytes': avg_free_mem_bytes,
                'effective_free_bytes': mem_effective_free_bytes,
                'usage_percent': round(avg_mem_pct, 2),
                'usage_ratio': mem_usage_ratio,
                'free_ratio': mem_effective_free_ratio,
                'raw_free_ratio': mem_free_ratio,
            },
            'bandwidth': {
                'capacity_bps': self.capacity_bps,
                'used_bps': round(avg_bw_used_bps, 2),
                'free_bps': round(bw_free_bps, 2),
                'effective_free_bps': bw_effective_free_bps,
                'usage_percent': round(avg_bw_pct, 2),
                'usage_ratio': bw_usage_ratio,
                'free_ratio': bw_effective_free_ratio,
                'raw_free_ratio': bw_free_ratio,
            },

            # 互換用スコア
            'cpu_score': resource_score(avg_cpu_pct),
            'mem_score': resource_score(avg_mem_pct),
            'bandwidth_score': resource_score(avg_bw_pct),

            # メタデータ (タイムスタンプと移動平均サンプル数)
            'timestamp': round(time.time(), 3),
            'sample_count': n,
        }
        self.current_payload = json.dumps(payload, ensure_ascii=False).encode('utf-8')

    async def _collection_loop(self):
        """0.5秒おきにリソースを並列サンプリングするバックグラウンドタスク。"""
        LOG.info('Starting background resource collection loop (interval: %.1fs, window: %.1fs, max_samples: %d)',
                 SAMPLE_INTERVAL, WINDOW_SECONDS, MAX_SAMPLES)
        while True:
            try:
                await self._sample_once()
            except Exception as exc:
                LOG.error('Error in background collection loop: %s', exc)
            await asyncio.sleep(SAMPLE_INTERVAL)

    async def start_background_collection(self):
        """起動時に初回サンプリングを行ってからバックグラウンドループを開始する。"""
        await self._sample_once()
        asyncio.create_task(self._collection_loop())

    def get_resource_payload(self):
        if self.current_payload is None:
            self._update_payload()
        return self.current_payload

    def on_interest(self, name, interest_param, app_param):
        if Component.to_str(name[-1]) == 'resource':
            payload = self.get_resource_payload()
            LOG.info('[Data Response] Resource data for %s: %s',
                     Name.to_str(name), payload.decode('utf-8'))
            self.app.put_data(name, content=payload, freshness_period=1000)


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
        app.run_forever(after_start=spy.start_background_collection())
    except KeyboardInterrupt:
        LOG.info('Spy stopped')


if __name__ == '__main__':
    main()
