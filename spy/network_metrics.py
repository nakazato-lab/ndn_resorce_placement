import json
import logging
import math
import os
import re
import time
from urllib.request import ProxyHandler, build_opener

LOG = logging.getLogger(__name__)
SAMPLE_INTERVAL_SECONDS = 1
MAX_BANDWIDTH_BPS = 1_000_000_000 # 1Gbps
TARGET = re.compile(
    r'^node_network_(receive_bytes_total|transmit_bytes_total)'
    r'\{device=("(?:[^"\\]|\\.)*")\}\s+(\S+)$'
)

def parse_metrics(text):
    interface = os.environ.get('NETWORK_INTERFACE', 'ens18')
    metrics = {}
    for line in text.splitlines():
        match = TARGET.match(line)
        if match:
            metric, device, raw = match.groups()
            device = json.loads(device)
            if device != interface:
                continue
            value = float(raw)
            if math.isfinite(value) and value >= 0:
                metrics[metric] = value
    return metrics


class NetworkMonitor:
    def __init__(self, url='http://127.0.0.1:9100/metrics'):
        self.url = url
        self._opener = build_opener(ProxyHandler({}))
        self._last_metrics = None
        self._last_time = None

    def _fetch_metrics(self):
        with self._opener.open(self.url, timeout=5) as response:
            return parse_metrics(response.read().decode('utf-8'))

    def sample_instant(self):
        """ノンブロッキングで直前のサンプリング時からの送受信レート(bps)を算出する。
        前回サンプリングがない初回は基準値のみ記録して0.0を返す。
        """
        now = time.time()
        try:
            current = self._fetch_metrics()
        except Exception as exc:
            LOG.warning('Failed to fetch network metrics: %s', exc)
            return {
                'used_bps': 0.0,
                'usage_percent': 0.0,
                'capacity_bps': float(MAX_BANDWIDTH_BPS),
            }

        rates = {}
        if self._last_metrics is not None and self._last_time is not None:
            dt = max(0.001, now - self._last_time)
            for metric, field in (('receive_bytes_total', 'in_bps'),
                                  ('transmit_bytes_total', 'out_bps')):
                after, before = current.get(metric), self._last_metrics.get(metric)
                rates[field] = ((after - before) * 8 / dt
                                if after is not None and before is not None and after >= before else 0.0)
        else:
            rates = {'in_bps': 0.0, 'out_bps': 0.0}

        self._last_metrics = current
        self._last_time = now

        used = rates.get('in_bps', 0.0) + rates.get('out_bps', 0.0)
        capacity = float(MAX_BANDWIDTH_BPS)
        used_bps = min(float(used), capacity)
        usage_pct = (used_bps / capacity) * 100.0 if capacity > 0 else 0.0

        return {
            'used_bps': round(used_bps, 2),
            'usage_percent': round(usage_pct, 2),
            'capacity_bps': capacity,
        }

    def get_bandwidth_metrics(self):
        previous = self._fetch_metrics()
        time.sleep(SAMPLE_INTERVAL_SECONDS)
        current = self._fetch_metrics()
        rates = {}
        for metric, field in (('receive_bytes_total', 'in_bps'),
                              ('transmit_bytes_total', 'out_bps')):
            after, before = current.get(metric), previous.get(metric)
            rates[field] = ((after - before) * 8 / SAMPLE_INTERVAL_SECONDS
                            if after is not None and
                            before is not None and after >= before else None)
        used = (rates['in_bps'] + rates['out_bps']
                if rates['in_bps'] is not None and rates['out_bps'] is not None else None)
        capacity = MAX_BANDWIDTH_BPS
        if capacity is None or capacity <= 0 or used is None:
            return {
                'capacity_bps': MAX_BANDWIDTH_BPS,
                'used_bps': 0.0,
                'free_bps': float(MAX_BANDWIDTH_BPS),
                'usage_percent': 0.0,
                'usage_ratio': 0.0,
                'free_ratio': 1.0,
            }
        used_bps = min(float(used), float(capacity))
        free_bps = max(0.0, float(capacity) - used_bps)
        usage_pct = (used_bps / capacity) * 100.0
        LOG.info('bandwidth usage: %.2f%%', usage_pct)
        return {
            'capacity_bps': float(capacity),
            'used_bps': round(used_bps, 2),
            'free_bps': round(free_bps, 2),
            'usage_percent': round(usage_pct, 2),
            'usage_ratio': round(used_bps / capacity, 4),
            'free_ratio': round(free_bps / capacity, 4),
        }

    def calculate_rates(self):
        metrics = self.get_bandwidth_metrics()
        return metrics['usage_percent']
