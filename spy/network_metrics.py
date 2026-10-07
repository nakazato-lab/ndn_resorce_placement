import json
import logging
import math
import re
import threading
import time
from urllib.request import ProxyHandler, build_opener

LOG = logging.getLogger(__name__)
SAMPLE_INTERVAL_SECONDS = 5
TARGET = re.compile(
    r'^node_network_(speed_bytes|receive_bytes_total|transmit_bytes_total)'
    r'\{device=("(?:[^"\\]|\\.)*")\}\s+(\S+)$'
)


def parse_metrics(text):
    devices = {}
    for line in text.splitlines():
        match = TARGET.match(line)
        if match:
            metric, device, raw = match.groups()
            value = float(raw)
            if math.isfinite(value) and value >= 0:
                devices.setdefault(json.loads(device), {})[metric] = value
    return devices


class NetworkMonitor:
    def __init__(self, url='http://127.0.0.1:9100/metrics'):
        self.url = url
        self.snapshot = None
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._opener = build_opener(ProxyHandler({}))

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
        result = {}
        for device, counters in current.items():
            speed = counters.get('speed_bytes')
            values = {'max_bandwidth_bps': speed * 8 if speed is not None else None}
            old = previous.get(device, {})
            for metric, field in (('receive_bytes_total', 'in_bps'),
                                  ('transmit_bytes_total', 'out_bps')):
                value, before = counters.get(metric), old.get(metric)
                values[field] = ((value - before) * 8 / SAMPLE_INTERVAL_SECONDS
                                 if value is not None and
                                 before is not None and value >= before else None)
            values['used_bps'] = (values['in_bps'] + values['out_bps']
                                  if values['in_bps'] is not None and
                                  values['out_bps'] is not None else None)
            capacity = values['max_bandwidth_bps']
            used = values['used_bps']
            values['bandwidth_score'] = (max(0.0, (capacity - used) / capacity)
                                         if capacity is not None and capacity > 0 and
                                         used is not None else None)
            result[device] = values
        return result

    def _run(self):
        while not self._stop.is_set():
            try:
                rates = self.calculate_rates()
                if rates is None:
                    break
                self.snapshot = {'sampled_at': time.time(), 'interfaces': rates}
                for device, values in sorted(rates.items()):
                    LOG.info('network %s', json.dumps({'device': device, **values},
                                                     allow_nan=False))
            except Exception as exc:
                self.snapshot = None
                LOG.warning('Network metrics unavailable: %s', exc)
                self._stop.wait(SAMPLE_INTERVAL_SECONDS)
