import asyncio
import json
import logging
import os
from pathlib import Path

from kubernetes import client, config
from ndn.encoding import Name
from ndn.types import InterestNack, InterestTimeout
from segmented_node_wrapper import SegmentedNodeWrapper

from ndn_routing import create_app

LOG = logging.getLogger(__name__)
SEED_CODE_PREFIX = '/manager/functions'
CODE_FETCH_RETRY_TIMEOUT = 90


def router_code_prefix():
    path = os.getenv('NFD_NAME_PATH', '/etc/ndn-config/ROUTER_NAME')
    router_name = Path(path).read_text(encoding='utf-8').strip()
    return router_name


def normalize_preference(preference):
    # 戻り値の例: {'cpu': 'high', 'memory': 'medium', 'bandwidth': 'low'}
    # 省略項目はmediumで補完し、不正なキー・値はValueErrorに
    if not isinstance(preference, dict) or set(preference) - {'cpu', 'memory', 'bandwidth'}:
        raise ValueError('preference must be an object containing only cpu, memory, and bandwidth')
    result = {key: preference.get(key, 'medium') for key in ('cpu', 'memory', 'bandwidth')}
    for key, value in result.items():
        if not isinstance(value, str) or value not in ('high', 'medium', 'low'):
            raise ValueError(f'preference.{key} must be high, medium, or low')
    return result


DEFAULT_WEIGHT_PREFERENCE = float(os.getenv('SCORE_WEIGHT_PREFERENCE', '1.0'))
DEFAULT_WEIGHT_BALANCE = float(os.getenv('SCORE_WEIGHT_BALANCE', '1.0'))
PREFERENCE_DELTA = {
    'high': float(os.getenv('PREFERENCE_DELTA_HIGH', '0.15')),      # 想定増分: +15%
    'medium': float(os.getenv('PREFERENCE_DELTA_MEDIUM', '0.10')),  # 想定増分: +10%
    'low': float(os.getenv('PREFERENCE_DELTA_LOW', '0.05')),        # 想定増分: +5%
}


def select_node(resources, preference, weight_preference=DEFAULT_WEIGHT_PREFERENCE, weight_balance=DEFAULT_WEIGHT_BALANCE):
    weights = {'high': 3, 'medium': 2, 'low': 1}
    cpu_weight = weights[preference['cpu']]
    memory_weight = weights[preference['memory']]
    bandwidth_weight = weights[preference['bandwidth']]

    # 1. 従来手法: リソース優先度(preference)と空き容量に基づくスコア (preference_score)
    def score(node):
        return (cpu_weight * node.get('cpu_score', 0)
                + memory_weight * node.get('mem_score', 0)
                + bandwidth_weight * node.get('bandwidth_score', 0)
                ) / (cpu_weight + memory_weight + bandwidth_weight)

    # 2. BalancedResourceAllocationに基づく均等化スコア:
    #    各ノードの現在の使用率(%)に新規機能のPreference(要求量)による想定増分を加算し、
    #    配置後の3リソース(CPU, メモリ, 帯域)の使用率比率が均等になるノードに高いスコアを与える。
    def balance_score(node):
        def get_current_fraction(key_score, key_usage, key_fraction):
            if key_fraction in node:
                return float(node[key_fraction])
            if key_usage in node:
                return float(node[key_usage]) / 100.0
            # Spyの resource_score(usage) = max(0, int((80 - usage) / 80 * 100)) から使用率(%)を逆算
            score_val = float(node.get(key_score, 0))
            usage = max(0.0, 80.0 - (score_val * 80.0 / 100.0))
            return usage / 100.0

        # 新規関数のPreferenceに応じた想定増分を加え、配置後の予測使用率割合(0.0〜1.0)を算出
        f_cpu = min(1.0, get_current_fraction('cpu_score', 'cpu_usage', 'fraction_cpu') + PREFERENCE_DELTA[preference['cpu']])
        f_mem = min(1.0, get_current_fraction('mem_score', 'mem_usage', 'fraction_mem') + PREFERENCE_DELTA[preference['memory']])
        f_bw = min(1.0, get_current_fraction('bandwidth_score', 'bandwidth_usage', 'fraction_bw') + PREFERENCE_DELTA[preference['bandwidth']])

        # 3リソース(CPU, メモリ, 帯域)のペア間差の平均を算出 (差が小さいほど均等)
        diff = (abs(f_cpu - f_mem) + abs(f_mem - f_bw) + abs(f_bw - f_cpu)) / 3.0
        # 差が小さいほど高得点 (最大100点)
        return max(0.0, 100.0 - diff * 100.0)

    # 3. 2つのscore関数に重みづけ(初期値1対1)でこの2つのscoreを考慮した総合スコア
    def combined_score(node):
        total_weight = weight_preference + weight_balance
        if total_weight == 0:
            return 0.0
        return (weight_preference * score(node) + weight_balance * balance_score(node)) / total_weight

    # ノード選択は従来通り preference_score (score関数) で決定
    selected = max(resources, key=score, default=None)
    if selected is not None:
        LOG.info('Selected node: %s (score: %.2f, balance: %.2f, combined: %.2f, preference: %s)',
                 selected['node_name'], score(selected), balance_score(selected),
                 combined_score(selected), preference)
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


def is_worker_node(node):
    labels = node.metadata.labels or {}
    return ('node-role.kubernetes.io/control-plane' not in labels
            and 'node-role.kubernetes.io/master' not in labels)


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
        router_name = router_code_prefix()
        self.upload_prefix = f'/{router_name}{SEED_CODE_PREFIX}'
        self.publisher = SegmentedNodeWrapper(app, serve=True)

    async def _send(self, node, request):
        app_param = json.dumps(request, ensure_ascii=False).encode('utf-8')
        LOG.info('Sending %s to Seed on %s: %s', request['type'], node, request['name'])
        LOG.info('Seed ApplicationParameters: %s', app_param.decode('utf-8'))
        _, _, content = await self.app.express_interest(
            f'/{node}/seed', app_param=app_param,
            must_be_fresh=True, can_be_prefix=True, lifetime=4000)
        response = bytes(content or b'').decode('utf-8')
        if not response.strip() or response.lstrip().startswith('Error:'):
            raise RuntimeError(f'Seed on {node} rejected request: {response}')
        if response.strip() != 'ACK':
            raise RuntimeError(f'Unexpected Seed response on {node}: {response}')
        return response

    async def create(self, node, name, runtime, code, requirements):
        code_ref = await self.publisher.publish(
            f'{self.upload_prefix}/{name.lstrip("/")}', code.encode('utf-8'))
        return await self._send(node, {
            'type': 'CREATE', 'name': name, 'runtime': runtime, 'code_ref': code_ref,
            'requirements': requirements,
        })

    async def delete(self, node, name):
        await self._send(node, {'type': 'DELETE', 'name': name})


class FunctionClient:
    def __init__(self, app):
        self.fetcher = SegmentedNodeWrapper(app)

    async def fetch_code(self, code_ref):
        LOG.info('Fetching segmented function code: %s', code_ref)
        deadline = asyncio.get_running_loop().time() + CODE_FETCH_RETRY_TIMEOUT
        while True:
            try:
                content = await self.fetcher.fetch(code_ref, must_be_fresh=True)
                break
            except (InterestNack, InterestTimeout) as exc:
                if asyncio.get_running_loop().time() >= deadline:
                    raise
                LOG.warning('Cannot fetch %s yet (%s); retrying', code_ref, exc)
                await asyncio.sleep(2)
        try:
            code = content.decode('utf-8')
        except UnicodeDecodeError as exc:
            raise ValueError('Function code must be UTF-8 text') from exc
        if not code.strip() or '\x00' in code or code.lstrip().startswith('Error:'):
            raise ValueError(f'Invalid function code response from {code_ref}')
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
        runtime = request.get('runtime')
        if not isinstance(runtime, str) or not runtime.strip():
            raise ValueError('runtime is required')
        requirements = request.get('requirements', [])
        code = await self.function.fetch_code(request.get('code_ref'))
        if not isinstance(code, str) or not code.strip() or '\x00' in code:
            raise ValueError('content must be nonempty text without NUL characters')
        nodes = await asyncio.to_thread(self.api.list_node, _request_timeout=10)
        worker_nodes = [node.metadata.name for node in nodes.items if is_worker_node(node)]
        resources = await self.spy.collect_resources(worker_nodes)
        node = select_node(resources, preference)['node_name']
        response = await self.seed.create(node, request['name'], runtime, code, requirements)
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
