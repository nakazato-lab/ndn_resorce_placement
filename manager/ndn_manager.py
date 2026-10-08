import asyncio
import json
import logging
from pathlib import Path

from kubernetes import client, config
from ndn.encoding import Name

from ndn_routing import create_app

LOG = logging.getLogger(__name__)


def normalize_preference(preference):
    # 戻り値の例: {'cpu': 'high', 'memory': 'medium'}
    # 省略項目はmediumで補完し、不正なキー・値はValueErrorに
    if not isinstance(preference, dict) or set(preference) - {'cpu', 'memory'}:
        raise ValueError('preference must be an object containing only cpu and memory')
    result = {key: preference.get(key, 'medium') for key in ('cpu', 'memory')}
    for key, value in result.items():
        if not isinstance(value, str) or value not in ('high', 'medium', 'low'):
            raise ValueError(f'preference.{key} must be high, medium, or low')
    return result


def select_node(resources, preference):
    weights = {'high': 3, 'medium': 2, 'low': 1}
    cpu_weight = weights[preference['cpu']]
    memory_weight = weights[preference['memory']]

    def score(node):
        return (cpu_weight * node.get('cpu_score', 0)
                + memory_weight * node.get('mem_score', 0)) / (cpu_weight + memory_weight)

    selected = max(resources, key=score, default=None)
    if selected is not None:
        LOG.info('Selected node: %s (score: %.2f, preference: %s)',
                 selected['node_name'], score(selected), preference)
    return selected


def parse_request(app_param):
    if not app_param:
        raise ValueError('ApplicationParameters are required')
    request = json.loads(bytes(app_param).decode('utf-8'))
    if not isinstance(request, dict):
        raise ValueError('ApplicationParameters must be a JSON object')
    if not isinstance(request.get('name'), str) or not request['name'].strip():
        raise ValueError('name is required')
    return request


def function_prefix(name):
    return Name.to_str(Name.normalize('/' + name.lstrip('/')))


class SpyClient:
    def __init__(self, app):
        self.app = app

    async def _get_resource(self, node_name):
        prefix = f'/{node_name}/spy/resource'
        try:
            _, _, content = await self.app.express_interest(
                prefix, must_be_fresh=True, can_be_prefix=False, lifetime=2000)
            resource = json.loads(bytes(content).decode('utf-8'))
            resource['node_name'] = node_name
            return resource
        except Exception as exc:
            LOG.warning('Cannot get resources from %s: %s: %s', prefix, type(exc).__name__, exc)
            return None

    async def collect_resources(self, node_names):
        if not node_names:
            raise RuntimeError('Cannot find any K8s nodes')
        results = await asyncio.gather(*(self._get_resource(name) for name in node_names))
        resources = [resource for resource in results if resource is not None]
        if not resources:
            raise RuntimeError('Failed to collect resources from Spies')
        return resources


class SeedClient:
    def __init__(self, app):
        self.app = app

    async def _send(self, node, request):
        LOG.info('Sending %s to Seed on %s: %s', request['type'], node, request['name'])
        _, _, content = await self.app.express_interest(
            f'/{node}/seed', app_param=json.dumps(request, ensure_ascii=False).encode('utf-8'),
            must_be_fresh=True, can_be_prefix=True, lifetime=90000)
        response = bytes(content or b'').decode('utf-8')
        if not response.strip() or response.lstrip().startswith('Error:'):
            raise RuntimeError(f'Seed on {node} rejected request: {response}')
        return response

    async def create(self, node, name, code):
        response = await self._send(node, {'type': 'CREATE', 'name': name, 'content': code})
        if function_prefix(name) not in response.splitlines():
            raise RuntimeError(f'Unexpected Seed response on {node}: {response}')
        return response

    async def delete(self, node, name):
        response = await self._send(node, {'type': 'DELETE', 'name': name})
        prefix = function_prefix(name)
        if prefix in response.splitlines():
            raise RuntimeError(f'Function {prefix} remains on {node}: {response}')


class FunctionClient:
    def __init__(self, app):
        self.app = app

    async def fetch_code(self, name):
        prefix = function_prefix(name).rstrip('/') + '/code'
        LOG.info('Fetching function code: %s', prefix)
        _, _, content = await self.app.express_interest(
            prefix, must_be_fresh=True, can_be_prefix=False, lifetime=6000)
        code = bytes(content or b'').decode('utf-8')
        if not code.strip() or '\x00' in code or code.lstrip().startswith('Error:'):
            raise ValueError(f'Invalid function code response from {prefix}')
        return code


class Manager:
    def __init__(self, app, api, namespace):
        self.app = app
        self.api = api
        self.namespace = namespace
        self.spy = SpyClient(app)
        self.seed = SeedClient(app)
        self.function = FunctionClient(app)
        self.tasks = set()

    def on_interest(self, name, interest_param, app_param):
        for prefix, handler, freshness in (
            ('/Manager/register', self.register, 1000),
            ('/Manager/delete', self.delete, 0),
        ):
            if Name.is_prefix(Name.from_str(prefix), name):
                task = asyncio.create_task(self.respond(name, app_param, handler, freshness))
                self.tasks.add(task)
                task.add_done_callback(self.tasks.discard)
                break

    async def respond(self, name, app_param, handler, freshness):
        try:
            message = await handler(parse_request(app_param))
        except Exception as exc:
            LOG.exception('Request failed: %s', Name.to_str(name))
            message = f'Error: {type(exc).__name__}: {exc}'
        self.app.put_data(name, content=message.encode('utf-8'), freshness_period=freshness)

    async def register(self, request):
        preference = normalize_preference(request.get('preference', {}))
        code = request.get('content')
        if code is None or (isinstance(code, str) and not code.strip()):
            code = await self.function.fetch_code(request['name'])
        if not isinstance(code, str) or not code.strip() or '\x00' in code:
            raise ValueError('content must be nonempty text without NUL characters')
        nodes = await asyncio.to_thread(self.api.list_node, _request_timeout=10)
        resources = await self.spy.collect_resources([node.metadata.name for node in nodes.items])
        node = select_node(resources, preference)['node_name']
        response = await self.seed.create(node, request['name'], code)
        return f'Success: Function resources created on {node}. Seed response: {response}'

    def get_delete_node(self, prefix):
        pods = self.api.list_namespaced_pod(
            self.namespace, label_selector='managed-by=ndn-seed-python', _request_timeout=10).items
        function_pod = next((pod for pod in pods
                             if (pod.metadata.annotations or {}).get('ndn-prefix') == prefix), None)
        if function_pod is None:
            return None
        if function_pod.spec.node_name:
            return function_pod.spec.node_name
        seeds = self.api.list_namespaced_pod(
            self.namespace, label_selector='app=seed', _request_timeout=10).items
        for pod in seeds:
            if (pod.spec.node_name and not pod.metadata.deletion_timestamp
                    and any(c.type == 'Ready' and c.status == 'True'
                            for c in (pod.status.conditions or []))):
                return pod.spec.node_name
        raise RuntimeError('Function resources remain but no ready Seed is available')

    async def delete(self, request):
        prefix = function_prefix(request['name'])
        if prefix == '/':
            raise ValueError('Cannot delete the root prefix')
        node = await asyncio.to_thread(self.get_delete_node, prefix)
        if node is None:
            return f'Success: Function {prefix} is already deleted'
        await self.seed.delete(node, request['name'])
        return f'Success: Function {prefix} deleted by Seed on {node}'


def main():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
    config.load_incluster_config()
    namespace = Path('/var/run/secrets/kubernetes.io/serviceaccount/namespace').read_text().strip()
    app = create_app()
    manager = Manager(app, client.CoreV1Api(), namespace)
    app.route('/Manager')(manager.on_interest)
    try:
        app.run_forever()
    except KeyboardInterrupt:
        LOG.info('Manager stopped')


if __name__ == '__main__':
    main()
