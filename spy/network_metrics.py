import json
import logging
import math
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
    metrics = {}
    for line in text.splitlines():
        match = TARGET.match(line)
        if match:
            metric, device, raw = match.groups()
            device = json.loads(device)
            if device != 'ens18':
                continue
            value = float(raw)
            if math.isfinite(value) and value >= 0:
                metrics[metric] = value
    return metrics


class NetworkMonitor:
    def __init__(self, url='http://127.0.0.1:9100/metrics'):
        self.url = url
        self._opener = build_opener(ProxyHandler({}))

    def _fetch_metrics(self):
        with self._opener.open(self.url, timeout=5) as response:
            return parse_metrics(response.read().decode('utf-8'))

    def calculate_rates(self):
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
            return None
        usage = used / capacity * 100
        LOG.info('bandwidth usage: %.2f%%', usage)
        return usage
