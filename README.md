## アーキテクチャ
![manager-spy](docs/images/manager-spy.png)

## 処理フロー（全体）

関数登録要求（`/Manager/register`）を受信してから配置先ノードを決定するまでの3フェーズ：

```text
[ クライアント (Registerer) ]
       │  Interest: /Manager/register
       │  ApplicationParameters: { "code_ref": "/{client-router}/...", "preference": {...} }
       ▼
[ Manager ]
  ┌─────────────────────────────────────────────────────────────────────────┐
  │ フェーズ1: フィルタリング（環境変数 NODE_FILTER_POLICY で制御）           │
  │  'ALL'   : 全ノードを対象（デフォルト・後方互換）                         │
  │  'NLSR'  : クライアント側ルータの NLSR に Interest を送信し               │
  │            累積コスト（最短経路コスト）上位 M 台に絞り込む                │
  │  'RANDOM': ランダムに K 台をサンプリング                                  │
  │  'HYBRID': NLSR近傍フィルタ（上位 M 台）→ ランダムサンプリング（K 台）   │
  └────────────────────────────┬────────────────────────────────────────────┘
                               │ 候補ノード（K台）
  ┌────────────────────────────▼────────────────────────────────────────────┐
  │ フェーズ2: リソース収集                                                   │
  │  候補ノードの Spy に Interest を並行送信 → リソース Dataパケット受信       │
  └────────────────────────────┬────────────────────────────────────────────┘
                               │
  ┌────────────────────────────▼────────────────────────────────────────────┐
  │ フェーズ3: スコアリング・ノード選定（select_node）                         │
  │  3つの計算手法でスコアを算出、preference_score（手法1）で配置先を決定      │
  └────────────────────────────┬────────────────────────────────────────────┘
                               │
[ 選定ノードの Seed ] へ Pod作成指示
```

## 計算ロジック

参考文献に基づき、CPU等の計算リソースは **75〜80% を超えると処理能力の低下がみられる** ため、本システムでは **80% を安全上限（実効最大容量）** とみなし、80% を超えないようにスコアリング・配置判定を行います（環境変数 `SAFE_CAPACITY_LIMIT` で変更可能、デフォルト: `80.0`）。

### Spy

Spy はホストの `/proc`（`psutil`）および `node-exporter` から各ノードの**絶対的なリソース量（総量・使用量・空き量）**と**80%安全上限モデルに基づく実効空き割合**を取得し、Dataパケットとして返却します。

#### 80%安全上限モデルによる実効空き割合の計算
使用率（%）が 80% 以上に達したノードは実質的な空き容量ゼロ（0.0）とし、0%〜80% の範囲を 1.0〜0.0 にスケールします。

```text
effective_free_ratio = max(0.0, (80.0 - usage_percent) / 80.0)
```

#### 取得メトリクス
- **CPU**:
  - `total_cores`: 総論理コア数（`psutil.cpu_count(logical=True)`）
  - `used_cores`: 実効使用コア数（`(cpu_percent / 100) × total_cores`）
  - `free_cores`: 実効空きコア数（`total_cores - used_cores`）
  - `effective_free_cores`: 80%安全上限に基づく実効空きコア数（`max(0, total_cores × 0.8 - used_cores)`）
  - `cpu_free_ratio`: 実効空き割合（`effective_free_ratio(cpu_usage_percent)`）
  - `cpu_usage_ratio`: 使用割合（`used_cores / total_cores`）
- **メモリ**:
  - `total_bytes`: 総物理メモリ量（`psutil.virtual_memory().total`）
  - `used_bytes`: 使用メモリ量（`total_bytes - available`）
  - `free_bytes`: 利用可能メモリ量（`psutil.virtual_memory().available`）
  - `effective_free_bytes`: 80%安全上限に基づく実効空きメモリ量（`max(0, total_bytes × 0.8 - used_bytes)`）
  - `mem_free_ratio`: 実効空き割合（`effective_free_ratio(mem_usage_percent)`）
  - `mem_usage_ratio`: 使用割合（`used_bytes / total_bytes`）
- **帯域**:
  - `capacity_bps`: インターフェース最大容量（デフォルト 1 Gbps）
  - `used_bps`: 実効送受信帯域（`in_bps + out_bps`）
  - `free_bps`: 空き帯域（`capacity_bps - used_bps`）
  - `effective_free_bps`: 80%安全上限に基づく実効空き帯域（`max(0, capacity_bps × 0.8 - used_bps)`）
  - `bandwidth_free_ratio`: 実効空き割合（`effective_free_ratio(usage_percent)`）
  - `bandwidth_usage_ratio`: 使用割合（`used_bps / capacity_bps`）

監視対象のインターフェースは環境変数 `NETWORK_INTERFACE` で変更できます（例: `NETWORK_INTERFACE=eth0`、未設定時は `ens18`）。

---

### Manager

Manager は関数の登録要求（`/Manager/register`）を受け取ると、各ノードの Spy から収集したメトリクスを基にノードの評価を行います。
評価および最適な配置アルゴリズム模索のため、以下の3つの計算手法が実装されています。
（※現在の配置ノード決定は、従来通り **手法1（`score`）** を用いて行われ、ログにすべてのスコアが出力されます）

#### 手法1: リソース優先度・実効空き割合重視スコア（`score` / preference_score）
CPU・メモリ・帯域の優先度を重み（`high`: 3、`medium`: 2、`low`: 1、省略時は`medium`）に変換し、80%安全上限モデルに基づく各ノードの**実効空き割合（`free_ratio`、0〜100点換算）**を加重平均する。`/Manager/register` の `preference` には `bandwidth` も指定できる。
使用率 80% 以上のノードは実質空き容量なし（0点）となります。

```text
score = (cpu_free_ratio × 100 × cpu_weight + mem_free_ratio × 100 × memory_weight + bw_free_ratio × 100 × bandwidth_weight) / (cpu_weight + memory_weight + bandwidth_weight)
```

#### 手法2: リソース均等化スコア（`balance_score` / BalancedResourceAllocation）
Kubernetes の `NodeResourcesBalancedAllocation` の思想に基づき、各ノードの**絶対的な空きコア数・空きメモリ量・空き帯域量**と、新規関数の `preference`（要求の高さ）から割り当てられる**絶対要求リソース量（$\Delta$）**を用いて、配置後の3リソース使用率比率が均等になるノードに高いスコアを与える計算式（最大100点）。

```text
# 想定要求リソース量 (Preferenceによる絶対値設定):
# CPU: high 1.0コア, medium 0.5コア, low 0.2コア
# メモリ: high 1GiB, medium 512MiB, low 256MiB
# 帯域: high 100Mbps, medium 50Mbps, low 20Mbps

# 80%安全上限チェック:
# 配置後の使用率が80%を超える場合 (実効空き容量 < 要求量) は性能低下領域に入るため 0 点
if eff_free_cores < req_cores or eff_free_mem < req_mem or eff_free_bw < req_bw:
    return 0

# 配置後の予測使用率割合 (0.0〜1.0)
f_cpu = (total_cores - free_cores + req_cores) / total_cores
f_mem = (total_mem - free_mem + req_mem) / total_mem
f_bw  = (capacity_bps - free_bw + req_bw) / capacity_bps

diff = (|f_cpu - f_mem| + |f_mem - f_bw| + |f_bw - f_cpu|) / 3
balance_score = max(0, 100 - diff × 100)
```

#### 手法3: 複合スコア（`combined_score`）
上記2つのスコア（`score` と `balance_score`）に重み付け（初期値 1:1）を行い、両者を統合したスコアを算出する。重みは環境変数 `SCORE_WEIGHT_PREFERENCE` および `SCORE_WEIGHT_BALANCE` で調整可能。

```text
combined_score = (weight_preference × score + weight_balance × balance_score) / (weight_preference + weight_balance)
```

---

### フィルタリングフェーズ（NlsrClient）

Managerは関数登録要求を受け取ると、スコアリング・リソース収集の**前段**にフィルタリングフェーズを実施できます。

#### クライアント接続先ルータの特定

関数登録 Interest の `ApplicationParameters` 内に含まれる `code_ref`（例: `/{router_name}/registerer/functions/...`）の先頭コンポーネントから、クライアントが接続しているルータ名を取得します（`client_router` フィールドによる明示的な指定も可能）。

#### NLSR Status Dataset によるルーティングテーブル取得

```text
Interest: /ndn/k8s/%C1.Router/<client-router>/nlsr/routing-table
Data   : TLV形式のルーティングテーブル
         RouteTableEntry (type=145) × n:
           Name (type=7)       ... 宛先プレフィックス
           Nexthop (type=146):
             Cost (type=149)   ... 最短累積コスト (uint64)
```

- NLSRはダイクストラ法でLSDB（Link State Database）から計算した**最短累積コスト**を保持しており、1回のInterestで取得可能。
- 「クライアントのルータ」へ問い合わせることで、**クライアント視点の**ネットワーク距離を取得。

#### 環境変数による制御

| 環境変数 | デフォルト | 説明 |
|---|---|---|
| `NODE_FILTER_POLICY` | `ALL` | `ALL` / `NLSR` / `RANDOM` / `HYBRID` |
| `NLSR_TOP_M` | `10` | NLSRフィルタで残す低コスト上位ノード数（M台） |
| `SAMPLE_NODE_COUNT` | `5` | ランダムサンプリングで問い合わせるノード数（K台） |
| `NLSR_INTEREST_TIMEOUT_MS` | `3000` | NLSRへのInterestタイムアウト（ms） |
| `NLSR_ROUTER_PREFIX_FORMAT` | `/ndn/k8s/%%C1.Router/%s/nlsr/routing-table` | NLSRルータプレフィックスのフォーマット |

#### フォールバック動作

- NLSRからのData取得が失敗した場合は、Manager自身のルータのNLSRへフォールバック。
- さらに失敗した場合は、フィルタなし（全ノード対象）で動作を継続。

