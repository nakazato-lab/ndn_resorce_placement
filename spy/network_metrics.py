import json
import logging
import math
import re
import threading
import time
from urllib.request import ProxyHandler, build_opener

LOG = logging.getLogger(__name__)
SAMPLE_INTERVAL_SECONDS = 5
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
        self.snapshot = None
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._opener = build_opener(ProxyHandler({}))

    # spyの応答処理と別のスレッドで動かす
    def start(self):
        self._thread.start()

    def stop(self):
        self._stop.set()
        self._thread.join(timeout=6)

    def _fetch_metrics(self):
        with self._opener.open(self.url, timeout=5) as response:
            return parse_metrics(response.read().decode('utf-8'))

    def calculate_rates(self):
        previous = self._fetch_metrics()
        if self._stop.wait(SAMPLE_INTERVAL_SECONDS):
            return None
        current = self._fetch_metrics()
        values = {}
        for metric, field in (('receive_bytes_total', 'in_bps'),
                              ('transmit_bytes_total', 'out_bps')):
            after, before = current.get(metric), previous.get(metric)
            values[field] = ((after - before) * 8 / SAMPLE_INTERVAL_SECONDS
                             if after is not None and
                             before is not None and after >= before else None)
        values['used_bps'] = (values['in_bps'] + values['out_bps']
                              if values['in_bps'] is not None and
                              values['out_bps'] is not None else None)
        capacity = MAX_BANDWIDTH_BPS
        used = values['used_bps']
        values['bandwidth_score'] = (max(0.0, (capacity - used) / capacity)
                                     if capacity is not None and capacity > 0 and
                                     used is not None else None)
        return values


    def _run(self):
        while not self._stop.is_set():
            try:
                rates = self.calculate_rates()
                if rates is None:
                    break
                self.snapshot = {'sampled_at': time.time(), **rates}
                LOG.info('network %s', json.dumps(self.snapshot, allow_nan=False))
            except Exception as exc:
                self.snapshot = None
                LOG.warning('Network metrics unavailable: %s', exc)
                self._stop.wait(SAMPLE_INTERVAL_SECONDS)
