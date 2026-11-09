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
from opentelemetry import trace
from opentelemetry.context import Context
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator
from tracing import setup_tracing

LOG = logging.getLogger(__name__)
tracer = trace.get_tracer(__name__)
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

# 参考文献に基づき、CPU等のリソースは75〜80%を超えると処理能力の低下がみられるため、
# 80%を安全上限(実効最大容量)として空き容量・割合を評価する
SAFE_CAPACITY_LIMIT = float(os.getenv('SAFE_CAPACITY_LIMIT', '80.0'))

# 新規関数のPreferenceに応じた想定要求リソース量 (絶対値)
# CPU (コア数), メモリ (Bytes), 帯域 (bps)
PREFERENCE_DEMAND = {
    'cpu': {
        'high': float(os.getenv('DEMAND_CPU_HIGH', '1.0')),       # 1.0 コア
        'medium': float(os.getenv('DEMAND_CPU_MEDIUM', '0.5')),   # 0.5 コア
        'low': float(os.getenv('DEMAND_CPU_LOW', '0.2')),         # 0.2 コア
    },
    'memory': {
        'high': float(os.getenv('DEMAND_MEM_HIGH', str(1024 * 1024 * 1024))),     # 1 GiB
        'medium': float(os.getenv('DEMAND_MEM_MEDIUM', str(512 * 1024 * 1024))), # 512 MiB
        'low': float(os.getenv('DEMAND_MEM_LOW', str(256 * 1024 * 1024))),       # 256 MiB
    },
    'bandwidth': {
        'high': float(os.getenv('DEMAND_BW_HIGH', '100000000')),    # 100 Mbps
        'medium': float(os.getenv('DEMAND_BW_MEDIUM', '50000000')), # 50 Mbps
        'low': float(os.getenv('DEMAND_BW_LOW', '20000000')),       # 20 Mbps
    },
}


def select_node(resources, preference, weight_preference=DEFAULT_WEIGHT_PREFERENCE, weight_balance=DEFAULT_WEIGHT_BALANCE):
    weights = {'high': 3, 'medium': 2, 'low': 1}
    cpu_weight = weights[preference['cpu']]
    memory_weight = weights[preference['memory']]
    bandwidth_weight = weights[preference['bandwidth']]

    # 1. リソース優先度(preference)と実効空き割合に基づくスコア (preference_score)
    #    参考文献に基づく「CPU等は75〜80%を超えると処理能力の低下がみられる」という知見を踏まえ、
    #    80%を安全上限(実効最大容量)として空き割合(0〜100点)を加重平均する。
    #    使用率80%以上は実質的な空きゼロ(0点)とする。
    def score(node):
        def extract_effective_free_score(key_free, key_ratio, key_usage, fallback_score_key, key_obj, obj_total, obj_used):
            # Spy側で80%安全上限モデル (80 - usage)/80 として算出された free_ratio があれば直接利用
            if key_free in node:
                val = float(node[key_free])
                return max(0.0, val * 100.0 if val <= 1.0 else val)
            if key_ratio in node:
                val = float(node[key_ratio])
                return max(0.0, val * 100.0 if val <= 1.0 else val)
            # 使用率(%)または使用率割合から 80% 安全上限モデルで算出
            if key_usage in node:
                val = float(node[key_usage])
                usage_pct = val * 100.0 if val <= 1.0 else val
                return max(0.0, (SAFE_CAPACITY_LIMIT - usage_pct) / SAFE_CAPACITY_LIMIT * 100.0)
            # 絶対値オブジェクトから実測使用率を計算して 80% 上限で算出
            obj = node.get(key_obj, {})
            if obj_total in obj and obj_used in obj:
                total_val = float(obj[obj_total])
                used_val = float(obj[obj_used])
                if total_val > 0:
                    usage_pct = (used_val / total_val) * 100.0
                    return max(0.0, (SAFE_CAPACITY_LIMIT - usage_pct) / SAFE_CAPACITY_LIMIT * 100.0)
            if fallback_score_key in node:
                return float(node[fallback_score_key])
            return 0.0

        cpu_free = extract_effective_free_score('cpu_free_ratio', 'cpu_ratio', 'cpu_usage_ratio', 'cpu_score', 'cpu', 'total_cores', 'used_cores')
        mem_free = extract_effective_free_score('mem_free_ratio', 'mem_ratio', 'mem_usage_ratio', 'mem_score', 'memory', 'total_bytes', 'used_bytes')
        bw_free = extract_effective_free_score('bandwidth_free_ratio', 'bandwidth_ratio', 'bandwidth_usage_ratio', 'bandwidth_score', 'bandwidth', 'capacity_bps', 'used_bps')

        return (cpu_weight * cpu_free + memory_weight * mem_free + bandwidth_weight * bw_free) / (cpu_weight + memory_weight + bandwidth_weight)

    # 2. BalancedResourceAllocationに基づく均等化スコア:
    #    各ノードの絶対的な空きコア数・空きメモリ量・空き帯域量を取得し、
    #    新規関数の要求リソース量(Preferenceによる絶対値)を配置した後の
    #    各リソースの使用率割合(fraction)が最も均等になるノードに高いスコアを与える。
    #    なお、配置後の使用率が80%安全上限を超える場合は性能劣化領域に入るため0点とする。
    def balance_score(node):
        cpu_obj = node.get('cpu', {})
        mem_obj = node.get('memory', {})
        bw_obj = node.get('bandwidth', {})

        free_cores = float(cpu_obj.get('free_cores', node.get('free_cores', 0.0)))
        total_cores = float(cpu_obj.get('total_cores', node.get('total_cores', 1.0))) or 1.0

        free_mem = float(mem_obj.get('free_bytes', node.get('free_bytes', 0.0)))
        total_mem = float(mem_obj.get('total_bytes', node.get('total_bytes', 1.0))) or 1.0

        free_bw = float(bw_obj.get('free_bps', node.get('free_bps', 0.0)))
        total_bw = float(bw_obj.get('capacity_bps', node.get('capacity_bps', 1_000_000_000.0))) or 1_000_000_000.0

        # フォールバック処理 (絶対値オブジェクトが存在しない古いDataパケット用)
        if free_cores == 0.0 and 'cpu_score' in node:
            usage = max(0.0, SAFE_CAPACITY_LIMIT - (float(node['cpu_score']) * (SAFE_CAPACITY_LIMIT / 100.0))) / 100.0
            total_cores = 4.0
            free_cores = total_cores * (1.0 - usage)
        if free_mem == 0.0 and 'mem_score' in node:
            usage = max(0.0, SAFE_CAPACITY_LIMIT - (float(node['mem_score']) * (SAFE_CAPACITY_LIMIT / 100.0))) / 100.0
            total_mem = 8.0 * 1024 * 1024 * 1024
            free_mem = total_mem * (1.0 - usage)
        if free_bw == 0.0 and 'bandwidth_score' in node:
            usage = max(0.0, SAFE_CAPACITY_LIMIT - (float(node['bandwidth_score']) * (SAFE_CAPACITY_LIMIT / 100.0))) / 100.0
            total_bw = 1_000_000_000.0
            free_bw = total_bw * (1.0 - usage)

        req_cores = PREFERENCE_DEMAND['cpu'][preference['cpu']]
        req_mem = PREFERENCE_DEMAND['memory'][preference['memory']]
        req_bw = PREFERENCE_DEMAND['bandwidth'][preference['bandwidth']]

        # 80%安全上限での実効空き容量チェック:
        # 配置によって使用率が80%を超える場合 (実効空き量 < 要求量)、性能低下を招くため配置不可 (スコア 0)
        limit_ratio = SAFE_CAPACITY_LIMIT / 100.0
        eff_free_cores = max(0.0, total_cores * limit_ratio - (total_cores - free_cores))
        eff_free_mem = max(0.0, total_mem * limit_ratio - (total_mem - free_mem))
        eff_free_bw = max(0.0, total_bw * limit_ratio - (total_bw - free_bw))

        if eff_free_cores < req_cores or eff_free_mem < req_mem or eff_free_bw < req_bw:
            return 0.0

        # 配置後の各リソース予測使用率割合 (0.0〜1.0)
        f_cpu = min(1.0, max(0.0, (total_cores - free_cores + req_cores) / total_cores))
        f_mem = min(1.0, max(0.0, (total_mem - free_mem + req_mem) / total_mem))
        f_bw = min(1.0, max(0.0, (total_bw - free_bw + req_bw) / total_bw))

        # 3リソース(CPU, メモリ, 帯域)のペア間差の平均 (差が小さいほど均等)
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
        LOG.info('Selected node: %s (preference_score: %.2f, balance_score: %.2f, combined_score: %.2f, preference: %s)',
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
        with tracer.start_as_current_span('manager.seed.request', kind=trace.SpanKind.CLIENT,
                                         attributes={'k8s.node.name': node}):
            TraceContextTextMapPropagator().inject(request)
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

    @tracer.start_as_current_span('manager.publish.function')
    async def create(self, node, name, runtime, code, requirements, zip_content=None, source_file=None):
        request = {
            'type': 'CREATE', 'name': name, 'runtime': runtime,
            'requirements': requirements,
        }
        if zip_content is not None:
            request['zip_content'] = await self.publisher.publish(
                f'{self.upload_prefix}/{name.lstrip("/")}/package', zip_content)
            request['source'] = source_file
        else:
            request['code_ref'] = await self.publisher.publish(
                f'{self.upload_prefix}/{name.lstrip("/")}', code.encode('utf-8'))
        return await self._send(node, request)

    async def delete(self, node, name):
        await self._send(node, {'type': 'DELETE', 'name': name})


class FunctionClient:
    def __init__(self, app):
        self.fetcher = SegmentedNodeWrapper(app)

    @tracer.start_as_current_span('manager.fetch.content')
    async def fetch_content(self, content_ref):
        trace.get_current_span().set_attribute('ndn.name', content_ref)
        LOG.info('Fetching segmented content: %s', content_ref)
        deadline = asyncio.get_running_loop().time() + CODE_FETCH_RETRY_TIMEOUT
        while True:
            try:
                return await self.fetcher.fetch(content_ref, must_be_fresh=True)
            except (InterestNack, InterestTimeout) as exc:
                if asyncio.get_running_loop().time() >= deadline:
                    raise
                LOG.warning('Cannot fetch %s yet (%s); retrying', content_ref, exc)
                await asyncio.sleep(2)

    async def fetch_code(self, code_ref):
        content = await self.fetch_content(code_ref)
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
            request = parse_request(app_param)
            parent = TraceContextTextMapPropagator().extract(request, context=Context())
            with tracer.start_as_current_span(f'manager.{handler.__name__}', context=parent,
                                             kind=trace.SpanKind.SERVER,
                                             attributes={'ndn.function.name': request['name']}):
                message = await handler(request)
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
        package = {}
        code = None
        zip_ref = request.get('zip_content')
        if zip_ref:
            if not isinstance(zip_ref, str):
                raise ValueError('zip_content must be an NDN name')
            source_file = request.get('source')
            if not isinstance(source_file, str) or not source_file.strip():
                raise ValueError('source is required when zip_content is provided')
            package['zip_content'] = await self.function.fetch_content(zip_ref)
            package['source_file'] = source_file
        else:
            code = await self.function.fetch_code(request.get('code_ref'))
            if not isinstance(code, str) or not code.strip() or '\x00' in code:
                raise ValueError('content must be nonempty text without NUL characters')
        with tracer.start_as_current_span('manager.collect.resources'):
            nodes = await asyncio.to_thread(self.api.list_node, _request_timeout=10)
            worker_nodes = [node.metadata.name for node in nodes.items if is_worker_node(node)]
            resources = await self.spy.collect_resources(worker_nodes)
        with tracer.start_as_current_span('manager.select.node') as span:
            node = select_node(resources, preference)['node_name']
            span.set_attribute('k8s.node.name', node)
        # **package is expanded so create() receives zip_content and source_file as arguments.
        response = await self.seed.create(node, request['name'], runtime, code, requirements, **package)
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
    provider = setup_tracing('manager')
    try:
        run()
    finally:
        if provider is not None:
            provider.shutdown()


def run():
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
