import json
import logging
import os
from pathlib import Path

import psutil
from ndn.encoding import Component
from opentelemetry import trace
from opentelemetry.context import Context
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

from ndn_routing import create_app
from network_metrics import NetworkMonitor
from tracing import setup_tracing

LOG = logging.getLogger(__name__)
tracer = trace.get_tracer(__name__)


def resource_score(usage):
    return max(0, int((80 - usage) / 80 * 100))


class Spy:
    def __init__(self, app, node_name, network=None):
        self.app = app
        self.node_name = node_name
        self.network = network

    def get_resource_payload(self):
        with tracer.start_as_current_span('spy.measure.cpu'):
            cpu = psutil.cpu_percent(interval=0.1)
        with tracer.start_as_current_span('spy.measure.memory'):
            memory = psutil.virtual_memory().percent
        try:
            with tracer.start_as_current_span('spy.measure.bandwidth'):
                bandwidth = self.network.calculate_rates() if self.network else 0
        except Exception as exc:
            LOG.warning('Network metrics unavailable: %s', exc)
            bandwidth = 0
        return json.dumps({
            'cpu_score': resource_score(cpu),
            'mem_score': resource_score(memory),
            'bandwidth_score': resource_score(bandwidth),
        }).encode('utf-8')

    def on_interest(self, name, interest_param, app_param):
        # ApplicationParameters append a digest after the resource component.
        component = name[-2] if Component.get_type(name[-1]) == Component.TYPE_PARAMETERS_SHA256 else name[-1]
        if Component.to_str(component) != 'resource':
            return
        parameters = json.loads(bytes(app_param)) if app_param else {}
        parent = TraceContextTextMapPropagator().extract(parameters, context=Context())
        with tracer.start_as_current_span('spy.handler', context=parent, kind=trace.SpanKind.SERVER,
                                         attributes={'k8s.node.name': self.node_name}):
            self.app.put_data(name, content=self.get_resource_payload(), freshness_period=1000)


def main():
    provider = setup_tracing('spy')
    try:
        run()
    finally:
        if provider is not None:
            provider.shutdown()


def run():
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
