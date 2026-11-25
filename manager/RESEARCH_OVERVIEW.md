# NDN-Kubernetes サーバーレス関数配置・実行基盤 研究概要

## 1. 研究の全体像と背景

### 研究テーマ
**「Named Data Networking（NDN）とKubernetesを統合したエッジコンピューティング環境における、インテリジェントなサーバーレス関数（FaaS）配置・分散実行基盤」**

### 背景と目的
従来のIPベースのクラウド／エッジ環境におけるFaaS（Function-as-a-Service）では、IPアドレスとポート番号を指定した中央集権的なスケジューリングとサービスディスカバリが必要です。また、データの取得と関数の実行が別々の通信として扱われるため、通信オーバーヘッドやネットワーク負荷が増大しやすい課題があります。

本研究では、情報指向ネットワーク（ICN: Information-Centric Networking）の有力なアーキテクチャである **NDN（Named Data Networking）** とコンテナオーケストレータ **Kubernetes** を融合させ、以下の特徴を持つ新しいエッジFaaS基盤を実現しています。

1. **名前ベース（NDN）の関数呼び出し・コード配布**
   - IPアドレスではなく、関数名（プレフィックス、例: `/average`）を用いて関数実行やコード取得を行う。
2. **ローカル実行（コードキャッシュ）とリモート実行の柔軟な切り替え**
   - クライアント（Invoker）は、関数のコード自体をNDNデータ（`/average/code`）として取得・キャッシュし、端末側でローカル実行するか、ノード上のFunction Podへリモート実行を依頼するかを自律的に切り替える。
3. **ノードリソースを考慮した動的関数配置（ユーザー様の主担当）**
   - エッジノードのCPU・メモリ・ネットワーク帯域の負荷状態や関数の要求特性に基づき、最も適切なノードに関数Podを動的にスケジューリングする。

---

## 2. 4つの主要ディレクトリとシステム構成

```
                      [ クライアント / ユーザー ]
                             │
            ┌────────────────┴────────────────┐
            │ (1) 関数登録                     │ (4) 関数実行
   function-utils/registerer         function-utils/invoker
            │                                 │
            ▼                                 ▼
┌─────────────────────────┐       ┌─────────────────────────┐
│  NDN-resource-placement │       │  ndn-tools-seed-python  │
│  [ Manager / Spy ]      │       │  (Function Pod: sidecar)│
│  ※ユーザー様の主担当     │       │  プレフィックス: /average │
└───────────┬─────────────┘       └─────────────────────────┘
            │ (2) 配置指示
            ▼
┌─────────────────────────┐
│  ndn-tools-seed-python  │
│  [ Seed (DaemonSet) ]   │
└───────────┬─────────────┘
            │ (3) Pod生成
            ▼
┌─────────────────────────┐
│ Kubernetes Cluster      │
│ (k8s-manifestで管理)     │
└─────────────────────────┘
```

### ① `k8s-manifest` （クラスタ基盤・デプロイ定義）
Kubernetesクラスタ上でNDNネットワークおよび各デーモンを動作させるためのマニフェスト集です。
- **`nfd.yaml`**: 各ノードでNDNパケットを転送するNFD（Named Data Forwarding Daemon）およびルーティングプロトコルNLSRを稼働。
- **`spy.yaml`**: 全ノードでリソース監視を行う `Spy` をDaemonSetとして展開。
- **`seed.yaml`**: 各ノードで関数Podの作成・削除を行う `Seed` をDaemonSetとして展開。
- **`manager.yaml`**: 最適ノード判定と配置指示を行う `Manager` を展開。
- **`function-register.yaml` / `function-invoke.yaml`**: 関数の登録・実行テストを行うJob。

### ② `ndn-tools-seed-python` （関数ライフサイクル管理＆実行環境）
ノード上での関数の生成・削除および実行コンテナを管理します。
- **Seedデーモン (`seed/server.py`, `seed/backend.py`)**:
  - 各ノード固有のプレフィックス（`/{node}/seed`）で待機。
  - Managerからの `CREATE` リクエストを受信すると、Kubernetes API経由で自身のノード上にFunction Podを生成。
- **Function Pod（サイドカーアーキテクチャ）**:
  - **`sidecar` コンテナ (`ndn_function_grpc.py`)**:
    - NFDに関数プレフィックス（例: `/average`）を登録。
    - `/average/code` へのInterestを受信した場合：スクリプトコードをDataパケットとして返送（クライアント側でのキャッシュ＆ローカル実行を可能にする）。
    - 引数付きInterestを受信した場合：gRPC経由で同一Pod内の `function` コンテナへ実行を転送。
  - **`function` コンテナ (`interpreter_server.py`)**:
    - gRPC経由でリクエストを受け取り、Python等のスクリプトを実行して結果を返す。

### ③ `function-utils` （クライアントツール群）
関数を登録・実行するためのユーティリティです。
- **Registerer (`registerer/register.py`)**:
  - 関数名、コード、リソース優先度（`preference`: cpu/memory/bandwidth を high/medium/low で指定）をまとめ、`/Manager/register` にInterestを送信。
- **Invoker (`invoker/invoke.py`)**:
  - `ndnc` を用いてNDNスクリプトを実行。キャッシュがあればローカル実行し、なければ `/code` をフェッチするか、リモートのFunction Podへ実行を委譲。

### ④ `NDN-resource-placement` （ユーザー様の主担当：リソース監視＆配置エンジン）
システム全体のスケジューリング・コアとなるコンポーネントです。
- **Spy (`spy/spy_agent.py`, `spy/network_metrics.py`)**:
  - 各ノード上でCPU使用率・メモリ使用率（`psutil`）、ネットワーク帯域使用率（`node_exporter` 連携）を収集。
  - 空き容量スコア（0〜100、使用率80%以上で0点）を算出し、`/{node}/spy/resource` へのInterestに対してDataパケットで応答。
- **Manager (`manager/ndn_manager.py`)**:
  - `/Manager/register` へのInterestを受信すると、Kubernetes APIからワーカーノード一覧を取得。
  - 全ノードのSpyにNDN経由で並行してリソース取得Interestを送信。
  - スコアリング関数により最適ノードを選定し、選定ノードのSeedへPod作成を指示。
- **NDNルーティング連携 (`ndn_routing.py`)**:
  - `RoutedNDNApp` を実装し、RIB（Routing Information Base）に対して `origin=65`（Client Origin）でルート登録。NLSRによるノード間自動再広告（Readvertisement）を可能にしています。

---

## 3. 実装されたスコアリング手法とSpyの絶対値メトリクス取得

評価・実験および最適な配置アルゴリズム模索のため、SpyおよびManager（`select_node`）は**絶対値メトリクス（コア数・バイト数・bps）**と**80%安全上限モデルに基づく実効空き割合**を統合した高度な設計に刷新されています。
※現在の配置ノード選定は従来通り `score`（preference_score）で決定され、同時に `balance_score` や `combined_score` の値もログに出力されて比較評価が可能です。

### 3.0. 80%安全上限モデルの背景と設計方針
- **参考文献に基づく知見**: CPUやメモリ、ネットワーク等の計算リソースは、**使用率が75〜80%を超えると処理能力の低下（キューイング遅延の急増、スロットリング、スワップ発生など）が顕著にみられる**ことがわかっています。
- **実効最大容量の設定**: そのため、本システムではノードの最大キャパシティの **80% を実効的な安全上限（`SAFE_CAPACITY_LIMIT = 80.0`）** と定義しています。
- **実効空き割合の計算式**:
  使用率が 80% 以上のノードは実質的な空き容量ゼロ（`0.0`）とし、使用率 0%〜80% の範囲を `1.0`（100点）〜`0.0`（0点）にスケールします：
  $$\text{effective\_free\_ratio} = \max\left(0.0, \frac{80.0 - \text{usage\_percent}}{80.0}\right)$$

---

### 3.1. Spyにおける `psutil` / `node-exporter` からのメトリクス取得詳細

Spy（`spy/spy_agent.py`）は、ホストの `/proc`（`psutil.PROCFS_PATH = '/host/proc'`）および同一Pod内の `node-exporter` を利用して、以下の関数・プロパティから情報を取得し変数に格納しています。

#### ① CPU情報 (`psutil`)
| `psutil` 関数 / プロパティ | 戻り値の型 | 取得内容・単位 | Spy内での計算と格納変数名 | 返却JSONキー |
| :--- | :--- | :--- | :--- | :--- |
| `psutil.cpu_count(logical=True)` | `int` | システムの総論理コア数（個） | `total_cores = float(psutil.cpu_count() or 1)` | `cpu.total_cores` |
| `psutil.cpu_percent(interval=0.1)` | `float` | ノード全体のCPU使用率（%） | `cpu_usage_pct = float(psutil.cpu_percent())` | `cpu.usage_percent` |
| （計算値） | `float` | 実効使用コア数（個） | `used_cores = (cpu_usage_pct / 100.0) * total_cores` | `cpu.used_cores` |
| （計算値） | `float` | 物理空きコア数（個） | `free_cores = max(0.0, total_cores - used_cores)` | `cpu.free_cores` |
| （計算値） | `float` | **80%安全上限での実効空きコア数**（個） | `effective_free_cores = max(0.0, total_cores * 0.8 - used_cores)` | `cpu.effective_free_cores` |
| （計算値） | `float` | **80%安全上限での実効空き割合**（0.0〜1.0） | `cpu_free_ratio = max(0.0, (80.0 - cpu_usage_pct) / 80.0)` | `cpu_free_ratio`, `cpu_ratio` |
| （計算値） | `float` | 使用割合（0.0〜1.0） | `cpu_usage_ratio = used_cores / total_cores` | `cpu_usage_ratio` |

#### ② メモリ情報 (`psutil`)
| `psutil` 関数 / プロパティ | 戻り値の型 | 取得内容・単位 | Spy内での計算と格納変数名 | 返却JSONキー |
| :--- | :--- | :--- | :--- | :--- |
| `vmem = psutil.virtual_memory()` | `namedtuple` | 仮想メモリ統計情報オブジェクト | - | - |
| `vmem.total` | `int` | 総物理メモリ容量（Bytes） | `total_mem_bytes = int(vmem.total)` | `memory.total_bytes` |
| `vmem.available` | `int` | スワップを伴わず割当可能な空きメモリ（Bytes） | `free_mem_bytes = int(vmem.available)` | `memory.free_bytes` |
| （計算値） | `int` | 実質使用中メモリ量（Bytes） | `used_mem_bytes = max(0, total_mem_bytes - free_mem_bytes)` | `memory.used_bytes` |
| `vmem.percent` | `float` | メモリ使用率（%） | `mem_usage_pct = float(vmem.percent)` | `memory.usage_percent` |
| （計算値） | `int` | **80%安全上限での実効空きメモリ**（Bytes） | `effective_free_bytes = max(0, total_mem_bytes * 0.8 - used_mem_bytes)` | `memory.effective_free_bytes` |
| （計算値） | `float` | **80%安全上限での実効空き割合**（0.0〜1.0） | `mem_free_ratio = max(0.0, (80.0 - mem_usage_pct) / 80.0)` | `mem_free_ratio`, `mem_ratio` |
| （計算値） | `float` | 使用割合（0.0〜1.0） | `mem_usage_ratio = used_mem_bytes / total_mem_bytes` | `mem_usage_ratio` |

#### ③ ネットワーク帯域情報 (`network_metrics.py` / `node-exporter`)
| 取得元・プロパティ | 戻り値の型 | 取得内容・単位 | 格納変数名 | 返却JSONキー |
| :--- | :--- | :--- | :--- | :--- |
| `MAX_BANDWIDTH_BPS` | `int` | NICの最大許容帯域（1 Gbps） | `capacity_bps = float(1_000_000_000)` | `bandwidth.capacity_bps` |
| `node_network_*_bytes_total` 差分 | `float` | 現在の送受信合計スループット（bps） | `used_bps = in_bps + out_bps` | `bandwidth.used_bps` |
| （計算値） | `float` | 物理空き帯域（bps） | `free_bps = max(0.0, capacity_bps - used_bps)` | `bandwidth.free_bps` |
| （計算値） | `float` | **80%安全上限での実効空き帯域**（bps） | `effective_free_bps = max(0.0, capacity_bps * 0.8 - used_bps)` | `bandwidth.effective_free_bps` |
| （計算値） | `float` | 帯域使用率（%） | `usage_pct = (used_bps / capacity_bps) * 100` | `bandwidth.usage_percent` |
| （計算値） | `float` | **80%安全上限での実効空き割合**（0.0〜1.0） | `bw_free_ratio = max(0.0, (80.0 - usage_pct) / 80.0)` | `bandwidth_free_ratio`, `bandwidth_ratio` |

#### 返却DataパケットのJSON構造例
```json
{
  "cpu_free_ratio": 0.6875,
  "mem_free_ratio": 0.5000,
  "bandwidth_free_ratio": 0.7500,
  "cpu_ratio": 0.6875,
  "mem_ratio": 0.5000,
  "bandwidth_ratio": 0.7500,
  "cpu": {
    "total_cores": 4.0,
    "used_cores": 1.0,
    "free_cores": 3.0,
    "effective_free_cores": 2.2,
    "usage_percent": 25.0,
    "usage_ratio": 0.25,
    "free_ratio": 0.6875
  },
  "memory": {
    "total_bytes": 8589934592,
    "used_bytes": 3435973836,
    "free_bytes": 5153960756,
    "effective_free_bytes": 3435973837,
    "usage_percent": 40.0,
    "usage_ratio": 0.40,
    "free_ratio": 0.5000
  },
  "bandwidth": {
    "capacity_bps": 1000000000.0,
    "used_bps": 200000000.0,
    "free_bps": 800000000.0,
    "effective_free_bps": 600000000.0,
    "usage_percent": 20.0,
    "usage_ratio": 0.20,
    "free_ratio": 0.7500
  }
}
```

---

### 3.2. Managerにおけるスコアリングアルゴリズム

#### 1. リソース優先度・実効空き割合重視スコア（`score` / preference_score）
関数の `preference`（high: 3, medium: 2, low: 1）を重みとし、80%安全上限モデルに基づく各ノードの**実効空き割合（`free_ratio`、0〜100点換算）**を加重平均：
$$\text{score} = \frac{W_{\text{cpu}} \cdot (\text{cpu\_free\_ratio} \times 100) + W_{\text{mem}} \cdot (\text{mem\_free\_ratio} \times 100) + W_{\text{bw}} \cdot (\text{bw\_free\_ratio} \times 100)}{W_{\text{cpu}} + W_{\text{mem}} + W_{\text{bw}}}$$
- **80%上限の適用**: 使用率 80% 以上のノードは実効空き割合が 0.0（0点）となり、過負荷ノードへの配置が自動的に抑制されます。

#### 2. リソース均等化スコア（`balance_score` / BalancedResourceAllocation）
Kubernetesの `NodeResourcesBalancedAllocation` の思想に完全準拠し、ノードの**絶対的な空きリソース量（`free_cores`, `free_bytes`, `free_bps`）**と**総キャパシティ**、新規関数の**絶対要求リソース量（$\Delta$）**を用いて計算します。

1. **新規関数の要求リソース量（$\Delta$）の定義**:
   - CPU: `high`: 1.0コア, `medium`: 0.5コア, `low`: 0.2コア
   - メモリ: `high`: 1 GiB, `medium`: 512 MiB, `low`: 256 MiB
   - 帯域: `high`: 100 Mbps, `medium`: 50 Mbps, `low`: 20 Mbps
2. **80%安全上限キャパシティ制約チェック**:
   新規関数を配置した結果、いずれかのリソースの使用率が 80% を超過する場合（実効空き容量 $\text{effective\_free} < \text{req}$）、処理性能低下領域に入るためスコアは **`0.0` 点（配置不可）**。
3. **配置後の予測使用率割合（$f_r \in [0.0, 1.0]$）の算出**:
   $$f_{\text{cpu}} = \frac{(\text{total\_cores} - \text{free\_cores}) + \text{req\_cores}}{\text{total\_cores}}$$
   $$f_{\text{mem}} = \frac{(\text{total\_bytes} - \text{free\_bytes}) + \text{req\_bytes}}{\text{total\_bytes}}$$
   $$f_{\text{bw}} = \frac{(\text{capacity\_bps} - \text{free\_bps}) + \text{req\_bps}}{\text{capacity\_bps}}$$
4. **均等化スコア（最大100点）**:
   3リソース（CPU, メモリ, 帯域）のペアワイズ差の平均を算出し、差が小さいほど高得点：
   $$\text{diff} = \frac{|f_{\text{cpu}} - f_{\text{mem}}| + |f_{\text{mem}} - f_{\text{bw}}| + |f_{\text{bw}} - f_{\text{cpu}}|}{3}$$
   $$\text{balanceScore} = \max(0, 100 - \text{diff} \times 100)$$

- **不均一クラスタにおける効果**:
  同じ「空き率50%」でも、2コアノード（空き1コア）に1コア要求の関数を置くと使用率は100%に急上昇してバランススコアが大きく低下（48点）する一方、16コアノード（空き8コア）では使用率が56%程度に微増するだけでバランスが維持（77点）されます。大型ノードの残余キャパシティが正しく評価され、小型ノードのCPU枯渇を防ぎます。

#### 3. 総合評価スコア（`combined_score`）
2つのスコアを重み付け（初期値 1:1）で統合：
$$\text{combined\_score} = \frac{W_{\text{preference}} \cdot \text{score} + W_{\text{balance}} \cdot \text{balance\_score}}{W_{\text{preference}} + W_{\text{balance}}}$$
- 環境変数 `SCORE_WEIGHT_PREFERENCE` および `SCORE_WEIGHT_BALANCE` で重みを調整可能。

---

## 4. 大規模環境におけるリソース収集負荷の低減と絞り込み手法（将来構想・解決策）

### 4.1. 課題の背景
現在のManagerは、関数登録（`/Manager/register`）のたびにKubernetesクラスタ内の**全ワーカーノード（$N$ 台）のSpyに対して並行してリソース収集Interestを送信**しています（$O(N)$）。
- **ノード数が小規模な環境**: 全数調査でも短時間で完了し、問題ありません。
- **実環境・大規模エッジ環境（ノード数が数十〜数千台規模）**:
  1. **ネットワーク負荷の爆発**: 1回の関数登録につき $N$ 個の Interest パケットと $N$ 個の Data パケットがクラスタ内を飛び交う。
  2. **ManagerのI/Oボトルネック**: 多数の非同期ソケット通信とタイムアウト管理が発生。
  3. **テールレイテンシ問題（ストラグラー影響）**: ネットワーク混雑や遠隔ノードのタイムアウト（`lifetime=2000ms`）待ちによって、全体の登録レスポンスが遅延。
  4. **地理的分散エッジでの不合理性**: 本来、クライアントや配置元から遠く離れたノードに関数を配置しても実行時のレイテンシが大きくなるため、遠隔ノードを全数調査するのは無駄が大きい。

---

### 4.2. 収集対象Spyの絞り込み指標とアプローチ

#### アプローチA：ネットワーク距離・トポロジ指標による近傍選択
| 指標 | 概要・取得方法 | メリット・デメリット |
| :--- | :--- | :--- |
| **① NLSRルーティングコスト (Link Cost)** | NLSR（リンクステート型ルーティング）が保持する最短経路の累積メトリック。NFD/NLSRの管理情報（LSDB/FIB）から取得。 | **大 (NDN親和性最高)**: すでにクラスタ内で動いているNLSRの知見をそのまま利用できる。 |
| **② ホップ数 (Hop Count)** | Managerノードから各ノードまでのルータ経由数。NLSRのルーティング計算結果やNDNパケットのHopCountから特定。 | 直感的に「2ホップ以内の近隣エッジ」などのゾーン制限が可能。 |
| **③ 実測RTT (Round Trip Time)** | 過去のInterest/Data交換時間、または定期プローブによる平滑化RTT（SRTT）。 | 実際のネットワーク遅延・混雑をダイレクトに反映できるが、事前の測定テーブル維持が必要。 |
| **④ K8sトポロジーラベル (Zone/Region)** | ノードに付与された `topology.kubernetes.io/zone` やエッジ拠点ラベルを活用。 | **実装が最も平易**: K8s APIのラベルセレクタで瞬時にフィルタ可能。 |

#### アプローチB：確率論的サンプリング手法（Power of Two Choices）
- **概要**: 全ノードに問い合わせるのではなく、候補プールから**ランダムに $K$ 台（例: $K=5 \sim 10$ 台）のみをサンプリング**してSpyに問い合わせ、その中で最良のノードを選択する。
- **学術的根拠**: コンピュータ科学における「Power of Two Choices（2つの選択の力）」定理。全数調査（$O(N)$）を行わなくても、ごく少数のランダムサンプリング（$O(1)$）を行うだけで、システム全体の負荷分散効率は全数調査とほぼ同等に収束することが証明されている（Google Borg等のスケジューラでも採用）。
- **効果**: ノード数が1万台に増えても、通信コストは常に $K$ 件（定数 $O(1)$）に抑えられる。

#### アプローチC：ハイブリッド手法（近傍フィルタ ＋ ランダムサンプリング）
- **推奨方式**:
  1. まずトポロジーやNLSRコストに基づき、Manager/クライアントから「近傍の $M$ ノード（例: 20ノード）」を抽出。
  2. その $M$ ノードの中からランダムに $K$ ノード（例: 5ノード）をサンプリングしてSpyからリソースを収集。
- **効果**: ネットワーク近接性（実行時レイテンシの低減）と、問い合わせ負荷の完全な定数化（$O(1)$）を両立。

#### アプローチD：NDNキャッシングと定期パブリッシュ（Pull型からPush+Cache型へ）
- **概要**:
  - 現在: 関数登録リクエストの発生時にオンデマンドでPull（問い合わせ）。
  - 改善案: 各ノードのSpyが、一定周期（例: 5〜10秒）で自身のリソースDataパケットを `FreshnessPeriod=10000` でNFDに登録・送出（Push/Beacon）しておく。
- **効果**: ManagerがInterestを送った際、各ノードまでパケットが届かなくても、途中のルータやローカルのContent Store（キャッシュ）から即座に応答が返るため、通信負荷が大幅に削減される。

#### アプローチE：階層型マネージャ（Hierarchical Management）
- クラスタを複数のエッジエリアに分割し、エリアごとに「Local Manager」を配置。
- 最上位のManagerは各エリアの大まかな代表負荷（サマリ）のみを把握し、配置対象エリアを特定した上で、該当エリアのLocal Managerがエリア内のSpyからのみ収集。

---

### 4.3. NLSRにおけるネットワーク経路コスト情報の保持と取得方法（詳細比較・調査結果）

#### ① そもそもNLSRはネットワーク全体の経路コスト情報を持っているのか？
**結論：完全に保持しています。**
- **理由**: NLSR（Named Link-state Routing）はリンクステート型のルーティングプロトコルです。各ルータは隣接リンクコストを含むAdjacency LSA（Link State Advertisement）を定期的に交換し、ネットワーク全体の完全なトポロジーグラフである **LSDB（Link State Database）** を各ノード内に保持・同期しています。
- **最短累積コストの計算**: NLSRはLSDBに対してダイクストラ法（Dijkstra algorithm）を実行し、自ノードから各宛先ルータ・プレフィックスに至る**「最短累積コスト（End-to-End Cumulative Cost）」** を計算・更新してルーティングテーブル（Routing Table）に保持しています。

#### ② 方式A（NLSR）vs 方式B（NFD FIB）の比較：どちらがより良いか？
**結論：方式A（NLSR Status Dataset）が圧倒的に優れています。**

| 比較項目 | 方式A: NLSR Status Dataset (推奨) | 方式B: NFD FIB Status Dataset |
| :--- | :--- | :--- |
| **取得できるコストの種類** | **宛先までの「最短累積コスト（Cumulative Cost）」** | **直近のFace（隣接リンク）のLink Cost / 転送メトリック** |
| **トポロジー認識** | ネットワーク全体のトポロジーを把握して計算済み | 次ホップ（NextHop）の転送先しか関知しない |
| **累積コストの取得難易度** | **1回のInterest送信で即座に取得可能** | NFDから取得するには各ノードをホップバイホップで辿る必要があり非現実的 |
| **問い合わせの柔軟性** | **ネットワーク内の任意のルータ（クライアント側ルータ等）に問い合わせ可能** | 接続先のローカルNFDのみに限定される |

- **方式Bの問題点**: NFDのFIBはパケット転送（フォワーディング）専用のテーブルであり、保持しているCostは基本「そのFaceの隣接リンクコスト」または優先度です。FIBからネットワーク全体の最短累積コストを求めようとすると、各ホストのNFDを順繰りに辿って足し算していく必要があり破綻します。
- 一方、**方式A（NLSR）はすでにダイクストラ計算が完了した累積コストそのものを保持しているため、方式Aを採用するのが圧倒的に適切**です。

---

### 4.4. 要求元（クライアント）ルータの特定メカニズムとパケットフロー

#### ① クライアント（Registerer）が接続先ルータ名を知っている仕組み
- **結論**: **完全に把握しています。**
- **仕組み**:
  - Kubernetesマニフェスト（`k8s-manifest/yaml/function-register.yaml`）の `initContainers` において、`ghcr.io/nakazato-lab/ndn-dhcp/ndn-dhcp-client:latest` が動作。
  - 起動時に直近のエッジルータと通信し、ルータ名（例: `ndn-router-0`）をローカルファイル `/etc/ndn-config/ROUTER_NAME` に自動書き込み。
  - `function-utils/registerer/register.py` の `get_router_prefix()` でそのファイルを直接読み取り、自身のルータ名を取得しています。

#### ② `code_ref` の送信先とManagerでのクライアントルータ把握
- **結論**: `code_ref` は NDNRouter 宛てではなく、**`/Manager/register` という名前の Interest の `ApplicationParameters` に載せて Manager に直接送信**されています。
- **パケットの流れ**:
  ```
  [ Registerer ]
        │  (1) コードをローカル公開 (NDN名: /{client-router}/registerer/functions/<name>)
        ▼
  [ Local NFD/Router ]
        │
        │  (2) Interest: /Manager/register
        │      ApplicationParameters = {"name": ..., "code_ref": "/{client-router}/...", ...}
        ▼
  [ Manager (ndn_manager.py) ]
        │  (3) ApplicationParameters をデコード
        │      → code_ref からクライアントの接続先ルータ名 ({client-router}) を瞬時に特定！
        │
        │  (4) クライアント側ルータのNLSRへInterest送信:
        │      Interest: /ndn/k8s/%C1.Router/{client-router}/nlsr/routing-table
        ▼
  [ Client Router の NLSR ]
        │  (5) Data: クライアント視点での各ノード宛て最短累積コスト一覧
        ▼
  [ Manager ]
        │  (6) 低コスト近傍ノード群 (M台) を抽出 ＋ K台ランダムサンプリング (フィルタリング)
        │  (7) K台のSpyからのみリソースを収集し、select_node で配置ノードを決定
        ▼
  [ 選定ノードの Seed ] へ Pod作成指示
  ```
- **Managerでの同時取得**: Managerはクライアントから関数登録Interestを受信したその瞬間に、パラメータ内の `code_ref`（または新設の `client_router`）から**「要求元ユーザーがどのルータ配下にいるか」を100%同時に把握**できます。

#### ③ RegistererとManagerの物理的位置関係
- **Manager**: 常駐 Deployment（通常 1 レプリカ）としてクラスタ内の特定ノード（例: `kube1`）で動作。
- **Registerer**: 関数登録時に都度立ち上がる Job としてスケジュール。`topologySpreadConstraints` が設定されているため、空いているワーカーノード（`kube1` や `kube2` など）に分散配置。
- したがって、**RegistererとManagerは同一ノードで動くこともあれば、別ノードで動くこともあります**。どのような物理配置であっても、NDNの名前ベースルーティングによって `/Manager/register` へのInterestがManagerへ確実に届きます。

---

### 4.5. フィルタリングの実装設計構想（NLSR近傍選択 ＋ 確率的ランダムサンプリング）

スコアリング（`select_node`）を実行する前段に、Kubernetesスケジューラと同様の**フィルタリング・フェーズ（Filtering Phase）**を導入します。

```text
[ 全ワーカーノード群 (N台: 例 1,000台) ]
                 │
                 ▼
【 第1段階: クライアント視点NLSRコストによる近傍フィルタリング 】
  ManagerがクライアントルータのNLSR Status Datasetから累積コストを取得し、
  コストが閾値以下のノード、または低コスト上位 M 台 (例: 20台) を抽出
                 │
                 ▼
【 第2段階: 確率論的ランダムサンプリング (Power of Two Choices) 】
  近傍 M 台の中からランダムに K 台 (例: 5台) を抽出
                 │
                 ▼
【 第3段階: リソース収集 (Interest送信) & スコアリング 】
  抽出された K 台のSpyからのみリソースを収集し、select_node で最適配置先を決定
```

- **効果**:
  1. 遠隔地・高遅延ノードを確実に除外（実行時レイテンシの最小化）。
  2. ノード数がどれほど大規模化しても、リソース収集負荷・通信パケット数を常に **$K$ 件（完全な定数 $O(1)$）** に抑えられる。

---

### 4.6. アプローチの比較まとめ

| 方式 | 通信量削減効果 | 実装難易度 | 特徴・推奨フェーズ |
| :--- | :---: | :---: | :--- |
| **K-ランダムサンプリング** | **絶大 ($O(1)$)** | **低 (即座に実装可)** | **第1段階として強く推奨**。ノード数が激増しても破綻しない数学的保証あり。 |
| **NLSRコスト連携 (方式A)** | 大 | 中 | **研究としての独自性・新規性が最も高い**。NLSRのルーティング知識をFaaSスケジューリングに活用。 |
| **ハイブリッド (近傍+サンプリング)** | **絶大** | 中 | 実用環境における最もバランスの良い最適解。 |

---

## 5. 補足：リソースメトリクス取得の仕組みとローカルシミュレーション環境

### 5.1. `psutil` の内部的な「％計算」の仕組み
- **Linuxカーネル側の仕様**: Linuxカーネルは「％」の値を直接記録しておらず、`/proc/stat` 内にCPUが各状態（user, system, idle, iowait等）で消費した累積時間カウンター（クロックティック数 / jiffies）を保持しています。
- **`psutil.cpu_percent(interval=0.1)` の動作**:
  1. 最初の時点で `/proc/stat` を読み、総CPU時間 $T_1$ とアイドル時間 $I_1$ を記録。
  2. `interval=0.1` 秒間スリープ。
  3. 0.1秒後に再度 `/proc/stat` を読み、総CPU時間 $T_2$ とアイドル時間 $I_2$ を記録。
  4. その間の差分から、$$\text{CPU使用率}(\%) = \frac{(T_2 - T_1) - (I_2 - I_1)}{T_2 - T_1} \times 100$$ を内部で計算して返却しています。
- メモリ（`vmem.percent`）も同様に、`/proc/meminfo` の `MemTotal` と `MemAvailable`（kB単位）から `(total - available) / total * 100` をpsutilが内部計算しています。

### 5.2. 取得リソースが「ノード全体（ホストVM）」である理由
- **マニフェスト構成**: [`k8s-manifest/yaml/spy.yaml`](file:///c:/Users/tmkmi/masterlab/k8s-manifest/yaml/spy.yaml) において、ホストマシンの `/proc` が `hostPath` によりコンテナ内の `/host/proc` にマウントされています。
- **プログラムの指定**: [`spy/spy_agent.py`](file:///c:/Users/tmkmi/masterlab/NDN-resource-placement/spy/spy_agent.py) の起動時に `psutil.PROCFS_PATH = '/host/proc'` が指定されています。これにより、psutilはコンテナ自身のprocではなく、**ホストOSの `/proc`（ホスト全体のコア数・メモリ量）** を読み取ります。
- さらに `hostNetwork: true` も設定されているため、ネットワーク帯域（`network_metrics.py`）もホスト全体の物理NIC（`ens18`）を直接監視しています。
- したがって、Spyが送信するデータは**コンテナ単体ではなく、Spyが常駐するKubernetesノード全体の実測値**です。

### 5.3. ローカルシミュレーション環境（`simulate_placement.py`）
- **目的**: VM環境にデプロイしなくても、手元のローカルPC（Windows/Mac）だけでスコアリングアルゴリズム（`preference_score`, `balance_score`, `combined_score`）の動作やノード選定結果を即座に検証可能。
- **仕組み**: `kubernetes`, `ndn`, `opentelemetry` などの未インストールライブラリを `unittest.mock` で自動モック化し、`ndn_manager.py` の `select_node` を純粋なPythonロジックとして直接実行。
- **実行方法**:
  ```bash
  python simulate_placement.py
  ```
- **検証可能な内容**:
  - 小型ノード（2core）と大型ノード（16core）の挙動差（不均一クラスタでの実証）。
  - 80%安全上限（過負荷ノードの排除）の動作。
  - Preference（CPU集約型、メモリ集約型、バランス型）ごとの選定結果。


---

## 6. フィルタリングフェーズの実装（NLSRルーティングコスト連携）

> [!IMPORTANT]
> この章は、4章の構想を実際に `ndn_manager.py` へ組み込んだ実装の詳細記録です。

### 6.1. 実装の動機と議論の経緯

研究室の議論において以下の課題が指摘された：

> 「現在のManagerは、全ワーカーノード（N台）のSpyにリソース収集Interestを送信しており、ノード数が増えるとO(N)の通信負荷が生じる。Kubernetesのように、スコアリング前にフィルタリングを導入できないか？」

この課題に対し、本研究のNDNネイティブな解法として、**NLSRが保持する最短累積コスト情報をフィルタリングに活用する**方針を採用した。最終的な設計・実装方針は以下の通り：

1. **方式A（NLSR Status Dataset）を採用**:
   - NLSRはLSDB（Link State Database）に基づくダイクストラ法で計算した「各ルータへの最短累積コスト」をルーティングテーブルに保持。
   - 1回のInterestで当該ルータの全宛先への累積コスト一覧をDataパケットとして取得可能（方式Bの「FIBから各ホップを辿る」より圧倒的に優れる）。

2. **クライアント起点のルーティングコスト取得**:
   - Manager自身のルータではなく、**クライアント（Registerer）が接続しているルータのNLSR**に問い合わせる。
   - これにより「Managerから近いノード」ではなく「要求元ユーザーから近いノード」を選ぶことができる。

3. **2段階フィルタリング（HYBRID）**:
   - 第1段階: NLSRコストで上位M台に絞り込み
   - 第2段階: M台からランダムにK台をサンプリング（Power of Two Choices）

---

### 6.2. 実装概要（`ndn_manager.py` の変更点）

#### 追加した関数・クラス

| 関数/クラス | 役割 |
|---|---|
| `parse_nlsr_routing_table(raw_bytes)` | NLSR Status Dataset の TLV バイト列をパースし `{宛先名: 最小コスト}` の辞書を返す |
| `extract_router_name_from_code_ref(request)` | 関数登録リクエストからクライアントのルータ名を抽出する |
| `filter_candidate_nodes(...)` | NLSRコスト近傍フィルタ ＋ ランダムサンプリングでノードリストを絞り込む |
| `NlsrClient` クラス | NLSRルーティングテーブルをNDN Interest/Data で取得するクライアント |

#### 修正した箇所

- `Manager.__init__`: `self.nlsr = NlsrClient(app)` を追加
- `Manager.register`: スコアリング前にフィルタリングフェーズを挿入

---

### 6.3. TLVパーサー（`parse_nlsr_routing_table`）の実装詳細

#### NLSR Status Dataset のTLV構造（NLSR仕様に基づく）

```text
RoutingTableStatus := OUTER-TLV TLV-LENGTH RouteTableEntry*

RouteTableEntry := 145 TLV-LENGTH
                   Name       (type=7)
                   Nexthop*   (type=146)

Nexthop := 146 TLV-LENGTH
           Cost  (type=149)   ... uint64 ビッグエンディアン

Name := 7 TLV-LENGTH
        GenericNameComponent* (type=8)
```

#### 設計上のポイント

- **複数 Nexthop 対応**: 1つの宛先に対して複数の NextHop が存在する場合、最小コストを選択する。
- **NDN TLV 可変長エンコード**: 1バイト（<0xFD）、3バイト（=0xFD）、5バイト（=0xFE）、9バイト（=0xFF）の可変長に対応。
- **純粋 Python 実装**: ndn-cxx ライブラリへの依存なしにバイト列を直接パースするため、Pythonのみで動作する。

#### TLV type番号（環境変数で変更可能）

```python
NLSR_RT_STATUS_TLV = 145   # RouteTableEntry
NLSR_NEXTHOP_TLV   = 146   # Nexthop
NLSR_COST_TLV      = 149   # Cost
```

---

### 6.4. クライアントルータ名の取得方法（`extract_router_name_from_code_ref`）

関数登録 Interest の `ApplicationParameters` から、クライアントが接続しているルータ名を取得する。

```python
# 優先度1: 明示的な client_router フィールド
request = {"client_router": "ndn-router-0", ...}

# 優先度2: code_ref の先頭コンポーネントから抽出（現行コードでも対応可能）
# 例: "/ndn-router-0/registerer/functions/average" → "ndn-router-0"
request = {"code_ref": "/ndn-router-0/registerer/functions/average", ...}

# 取得できない場合: None → Managerのルータへフォールバック
```

**なぜ取得できるか**:
- Registerer は `ndn-dhcp-client` により自動的に接続先ルータ名（`/etc/ndn-config/ROUTER_NAME`）を取得済み。
- `get_router_prefix()` でその値を読み取り、`code_ref` のプレフィックスとして使用している（`register.py` 88〜111行目）。
- したがって、Manager がリクエストを受け取った瞬間に `code_ref` の先頭コンポーネントからルータ名を読み取るだけでよい。

---

### 6.5. フィルタリングポリシーの切り替え（環境変数）

```bash
# 後方互換（デフォルト）: フィルタなし、全ノード対象
NODE_FILTER_POLICY=ALL

# NLSRコスト近傍フィルタのみ: 低コスト上位10台に絞り込み
NODE_FILTER_POLICY=NLSR
NLSR_TOP_M=10

# ランダムサンプリングのみ: 全ノードから5台をランダム選択
NODE_FILTER_POLICY=RANDOM
SAMPLE_NODE_COUNT=5

# ハイブリッド（推奨）: NLSR近傍 → ランダムサンプリング
NODE_FILTER_POLICY=HYBRID
NLSR_TOP_M=10
SAMPLE_NODE_COUNT=5
NLSR_INTEREST_TIMEOUT_MS=3000
```

#### フォールバック設計

| フォールバック条件 | 動作 |
|---|---|
| `code_ref` からルータ名が取得できない | Manager自身のルータのNLSRへフォールバック |
| クライアントルータのNLSR取得失敗 | Manager自身のルータのNLSRへフォールバック |
| Manager自身のNLSRも取得失敗 | 空のルーティングテーブルでフィルタを実行（全ノードがコスト∞として扱われ、順序は変わらず） |

---

### 6.6. 実装後の処理フロー（全体像）

```text
[ クライアント (Registerer) ]
       │  Interest: /Manager/register
       │  ApplicationParameters:
       │    { "code_ref": "/ndn-router-0/registerer/functions/average",
       │      "preference": {"cpu": "high", ...}, ... }
       ▼
[ Manager ]
  │
  ├─ [1] code_ref から client_router = "ndn-router-0" を抽出
  │
  ├─ [2] NlsrClient: Interest 送信
  │      名前: /ndn/k8s/%C1.Router/ndn-router-0/nlsr/routing-table
  │      ↓ Data (TLV): {'/ndn-router-1': 10, '/ndn-router-2': 25, ...}
  │
  ├─ [3] filter_candidate_nodes (HYBRID):
  │        全ノード (N台) → 低コスト上位M台 → ランダムK台
  │
  ├─ [4] SpyClient: K台のSpyに並行してリソース収集 Interest 送信
  │        /{node}/spy/resource
  │
  └─ [5] select_node: preference_score / balance_score / combined_score を計算
              → preference_score が最大のノードを配置先として決定

[ 選定ノードの Seed ] へ Pod作成指示
```

---

### 6.7. 設計上の判断記録（なぜこう実装したか）

| 設計決定 | 採用した方針 | 却下した方針 | 理由 |
|---|---|---|---|
| ルーティング情報の取得方式 | **方式A: NLSR Status Dataset** | 方式B: NFD FIB Status Dataset | 方式Bは隣接ノードのコストしか取得できず、累積コストの再計算が必要で非現実的 |
| どのルータに問い合わせるか | **クライアント側ルータ** | Manager側ルータ | 配置の目的は「要求元から近いノード」に関数を置くこと。Managerからの距離は不適切 |
| TLVのパース方法 | **純粋 Python でバイト列を直接パース** | ndn-cxx の Python バインディング使用 | Python環境への ndn-cxx インストール不要のシンプルな実装を優先 |
| フィルタリングポリシー | **環境変数で動的切り替え（デフォルトALL）** | ハードコード | 後方互換を保ちながら段階的に機能を有効化できる設計 |
| フォールバック | **NLSRが取得できない場合は段階的にフォールバック** | エラーで停止 | 実環境での耐障害性が重要。フィルタ失敗でも配置処理を継続させる |


---

## 7. Spyのリソース収集方式の刷新（オンデマンド型からバックグラウンド並列サンプリング＋移動平均へ）

> [!IMPORTANT]
> 教授との議論およびJaegerトレース分析によって判明した「リソース収集の遅延問題」と「瞬間値の信頼性課題」を解決するため、Spyのリソース収集アーキテクチャを刷新しました。

---

### 7.1. なぜオンデマンド型からバックグラウンド型に変更したか（動機と背景）

#### ① 瞬間的スパイク（ノイズ）による誤判定の危険性
- **従来の課題**: 
  従来のSpyは、ManagerからInterest（`/{node}/spy/resource`）を受信した瞬間に `psutil.cpu_percent(interval=0.1)`（0.1秒の瞬間値）や `psutil.virtual_memory()`（スナップショット）を取得していました。
- **問題点**: 
  OSのバックグラウンドプロセスや別タスクの一瞬のCPUスパイクを拾ってしまい、「本来は定常的に空いている優良ノード」を過負荷と誤判定して除外したり、逆に「瞬間的に谷間になった過負荷ノード」に誤って配置してしまう危険性がありました。
- **解決**: 
  直近数秒間の単純移動平均（SMA）を取ることで、一時的なノイズを平滑化し、ノードの「真の定常負荷状態」を正確に評価できるようにしました。

#### ② Jaegerトレースで判明した処理時間・遅延のボトルネック
- **従来の課題**: 
  Jaegerによる分散トレース分析の結果、Managerが各ノードのSpyからメトリクスを収集する処理に **1.1秒以上** を要しており、関数配置全体のレイテンシの大半を占めていることが判明しました。
- **原因の特定**:
  `spy/spy_agent.py` と `spy/network_metrics.py` において、以下の処理が**1つの関数内で直列（同期）実行**されていました：
  1. `psutil.cpu_percent(interval=0.1)` $\to$ **0.1秒待機**
  2. `psutil.virtual_memory()` $\to$ 即時
  3. `network.get_bandwidth_metrics()` $\to$ 内部で `time.sleep(1)`（**1.0秒待機**）
  
  合計で **最低1.1秒以上の同期ブロック（停止）** が発生し、Managerの配置決定を著しく遅延させていました。
- **バックグラウンド化による効果**:
  Spyが起動後、裏で定期的にサンプリングを行って最新の平均値をメモリ上に常に保持しておくことで、ManagerからのInterestに対して**待機時間ゼロ（数マイクロ秒〜数ミリ秒で即答）**でDataパケットを返せるようになりました。配置判定の収集オーバーヘッドは **1.1秒以上 $\to$ ほぼ0秒** に劇的に短縮されました。

---

### 7.2. バックグラウンド並列収集の設計と実装仕様

#### ① 0.5秒間隔の並列サンプリング
Managerがコレクトした際、CPU・メモリ・帯域の間で取得タイミングのズレ（CPUは最新だが帯域は過去のもの等）が生じないよう、**3つのリソースをスレッドプール（`asyncio.gather`）で完全に同一時刻に並行取得**します。

- **CPU**: `psutil.cpu_percent(interval=None)`（前回サンプリングからの差分に基づくノンブロッキング取得）
- **メモリ**: `psutil.virtual_memory()`（使用量・空き量）
- **帯域**: `NetworkMonitor.sample_instant()`（前回サンプリング時との送受信バイト数差分 $\Delta B / \Delta t$ によるノンブロッキング算出）

#### ② 直近5秒間の単純移動平均（SMA: Simple Moving Average）
- **サンプリング間隔**: 0.5秒（環境変数 `SPY_SAMPLE_INTERVAL`、デフォルト: `0.5`）
- **移動平均窓長**: 5.0秒（環境変数 `SPY_WINDOW_SECONDS`, デフォルト: `5.0`）
- **サンプル保持数**: 最大10サンプル（`collections.deque(maxlen=10)`）
- **不変値と平均値の分離**:
  - **ハードウェア固定値（変更なし）**: 総コア数（`total_cores`）、総物理メモリ量（`total_bytes`）、帯域キャパシティ（`capacity_bps`）
  - **移動平均対象**: CPU使用率、使用コア数、空きコア数、実効空きコア数、メモリ使用量、メモリ空き量、実効空きメモリ量、帯域使用bps、帯域空きbps、実効空き帯域bps、各スコア

#### ③ 送受信Dataパケットのログ可視化
- **Spy側**: Interest受信時、返却するDataパケット（JSON）の内容を `[Data Response]` としてログ出力。
- **Manager側**: SpyからDataパケットを受信した直後、内容を `[Spy Response]` としてログ出力。
これにより、`kubectl logs` を確認するだけで両端の通信内容・数値が完全に追跡可能となりました。

---

### 7.3. バックグラウンド実装に伴う懸念点と、実装したからこそ生まれる検討事項

バックグラウンド収集と移動平均の導入は、遅延短縮と平滑化において極めて大きな効果を持つ一方、分散システムおよびFaaS研究の観点から以下の**新たなトレードオフと考慮すべき課題**が生じます。

#### ① スムージング（平滑化）と急激な過負荷への追従遅延（トレードオフ）
- **懸念点**: 
  5秒間の移動平均を取ることで一時的スパイクを無視できる利点がある反面、「急激に大量のリクエストが押し寄せた場合」や「ある関数が暴走して急激にCPU 100%に達した場合」、その急増が5秒平均に反映されるまでに **約2.5〜5秒のタイムラグ（追従遅れ）** が生じます。
- **生じる問題**: 
  すでにノードが過負荷状態に陥りつつあるにもかかわらず、Managerは直近5秒平均を見て「まだ空いている」と誤認し、そのノードにさらに新規関数を配置してしまうリスク（集中配置 / Herding現象）があります。
- **今後の検討・対策案**:
  - **指数移動平均（EMA: Exponential Moving Average）の導入**: 直近のサンプルに指数関数的に高い重みを与え、ノイズ耐性と急変への追従性を両立させる。
  - **緊急スパイク検知（ハイブリッド判定）**: 「直近の瞬時値が90%を超えている場合は、平均値を待たずに即座に危険フラグ（スコア0）とする」安全装置の組み込み。

#### ② コールドスタート（Spy起動直後）のサンプル不足
- **懸念点**: 
  Spy Podが再起動または新規ノードに追加された直後の5秒間は、バッファ内に10サンプルが揃っていません。
- **現状の挙動**: 
  現行コードでは「その時点で蓄積されているサンプル数（1〜9個）」で平均を算出するフォールバックを実装しているため即時応答可能ですが、最初の1〜2秒間は平均のサンプル母数が小さく、一時的に値がブレる可能性があります。

#### ③ メモリ使用量における「時間平均」の意味論的課題
- **懸念点**: 
  CPUや帯域は「流量・処理レート（単位時間あたりの仕事量）」であるため、時間平均を取る工学的・統計的妥当性が非常に高いです。
  一方、メモリは「状態量（ある瞬間に占有されている容量）」です。
- **生じる問題**: 
  短命なサーバーレス関数コンテナが瞬間的に大量のメモリ（例: 2GB）を消費してすぐ終了した場合、5秒平均で見ると「平均200MB消費していた」という穏やかな値になります。しかし、次の関数を配置する瞬間に本当に物理メモリが空いているか（OOM Killer による強制終了が起きないか）という観点では、平均値だけを見ることにリスクが残ります。
- **今後の検討・対策案**: 
  メモリに関しては、「5秒平均使用量」に加えて「直近5秒の最大使用量（ピーク値）」も監視し、安全マージンとして考慮する設計が有効と考えられます。

#### ④ バックグラウンドタスクの生存監視とデータの鮮度（Staleness）
- **懸念点**: 
  万一バックグラウンドの収集ループが予期せぬ例外で停止した場合、Spyは最後に計算した「古いキャッシュデータ」をManagerに正常応答として返し続けてしまう恐れがあります。
- **今後の検討・対策案**: 
  Dataパケットに `timestamp` や `sample_count` を含め、Manager側で「一定時間以上更新されていない古いメトリクスは破棄してフォールバックする」仕組みを将来的に検討する必要があります。


---

### 7.4. バックグラウンドアーキテクチャの動作検証と運用上の詳細（ゼミ・研究報告用整理）

#### ① スリープ待ち時間0秒でも帯域使用量が正しく計算できる数学的仕組み

従来のオンデマンド型では、以下のアルゴリズムに基づいて帯域使用量を求めていました：

```text
・ 時刻 t1 における送受信バイト数合計を B1、時刻 t2 における送受信バイト数合計を B2 として：
  - in_bps  = (in2 - in1) * 8 / (t2 - t1)
  - out_bps = (out2 - out1) * 8 / (t2 - t1)
  - used_bps = in_bps + out_bps
・ (t2 - t1) を 1秒 として time.sleep(1) で待機。
・ 帯域使用率 = used_bps / capacity_bps
```

**バックグラウンド型における計算の同一性**:
- バックグラウンド型においても、**上記と全く同一の数式（$\frac{B_2 - B_1}{t_2 - t_1} \times 8$）** を用いています。
- 違いは「**いつスリープするか**」だけです：
  - **以前**: ManagerからInterestが届いてから `time.sleep(1)` を開始していたため、Managerが1秒以上待たされていた。
  - **現在**: Spyが裏で 0.5秒おき（$t_2 - t_1 \approx 0.5\text{s}$）に定期サンプリングを行い、直前のサンプリング時からの差分で瞬時 bps を算出してリングバッファ（10サンプル = 5秒分）に格納。ManagerからInterestが届いた瞬間は、**すでに計算済みの最新5秒平均値を返すだけなので、待機時間が0秒（即答）** になる。
- したがって、数学的・物理的な帯域測定精度を完全に維持したまま、Managerへの返答遅延だけを 0 秒に短縮しています。

#### ② バックグラウンドループの管理体制

- **起動トリガー**: `ndn_routing.py` の `app.run_forever(after_start=spy.start_background_collection())` により、NDNの非同期イベントループが立ち上がると同時に開始。
- **タスク生成**: `start_background_collection()` 内で初回サンプリングを行ってベースライン（差分計算の基準点）を作った後、`asyncio.create_task(self._collection_loop())` でバックグラウンドタスクとして登録。
- **非同期スリープ**: `_collection_loop` は `while True:` 内で `await asyncio.sleep(0.5)` を呼び出して待機。CPUをOSに完全に明け渡すため、ビジーループによるCPU高負荷は発生しません。
- **例外保護**: 1回のサンプリング処理全体が `try...except Exception as exc: LOG.error(...)` で保護されており、一時的な通信エラー等が発生してもループタスク自体が異常終了せず、0.5秒後に自動的に再試行されます。

#### ③ バックグラウンドループが異常停止する要因とタイムスタンプによる鮮度証明

- **異常停止が考えられるシナリオ**:
  1. `node-exporter`（`http://127.0.0.1:9100/metrics`）のPod再起動やクラッシュによるHTTP通信障害。
  2. ノードのメモリ枯渇（他コンテナの暴走等）に伴う、OSの OOM Killer による Spy プロセス自体の強制終了。
  3. ホストマウント（`/host/proc`）のアクセス権限喪失やファイルディスクリプタの枯渇。
- **タイムスタンプによる対策（実装完了）**:
  - 返却する Data パケットの JSON に `timestamp`（サンプリング時のUNIXエポック秒）および `sample_count`（平均計算に用いたサンプル数: 1〜10）を付与。
  - これにより、Manager側や運用者が「このメトリクスが何秒前に計算された新鮮なものか」を即座に検証可能となり、万一Spyの更新が停止した場合も古いデータ（Stale Data）であることを検知できるようになりました。

#### ④ メモリ上のデータ保持がノードリソースを圧迫しない定量的根拠

「メモリ上にリソース情報を保持することで、ノードのメモリを圧迫しないか？」という懸念に対する定量的検証です：

- **保持しているデータ量**:
  - `collections.deque(maxlen=10)`：最大10件の測定サンプル。
  - サンプル1件（数値6個のPython辞書）：約 250〜300 バイト。
  - 10件合計：**約 2.5〜3.0 KB（キロバイト = 0.003 MB）**。
  - シリアライズされた最新の JSON キャッシュ文字列：約 1.5 KB。
  - システム全体でのメモリ保持量：**10 KB 未満**。
- **ノードへの影響**:
  - 一般的な Kubernetes ノードの物理メモリ容量（4GB〜32GB等）と比較して、消費割合は **約 0.00003% 〜 0.0002%** です。
  - したがって、ノードの計算リソースやメモリを圧迫することは物理的にあり得ず、極めて軽量（ゼロ負荷に近い状態）で動作します。


---

## 8. 要求リソースの絶対値指定対応と再配置機能に向けたSeed連携プロトコル拡張

> [!IMPORTANT]
> 共同研究者による「配置後の予測リソースと実測リソースの乖離に基づく再配置機能」の実装に先立ち、Managerにおけるリソース要求受付とSeedへの予測要求リソース伝達プロトコルを拡張しました。

---

### 8.1. 課題と設計方針

#### ① 二重入力の手間を排除するハイブリッドモデル
- **課題**: 
  Kubernetesのように絶対値（例: CPU 0.8コア、メモリ 1GiB）で要求リソースを指定したいユーザーに対して、`demand`（絶対値）と `preference`（low/medium/high）の両方を書かせるのは冗長であり、指定ミスやユーザーの手間を招く。
- **方針**: 
  基本は従来の `preference` だが、**`demand`（絶対値）が送られてきた場合は、Manager側でその絶対値が `high / medium / low` のどれに相当するかを自動判定（逆算マッピング）** する。ユーザーはどちらか一方のみを指定すればよい設計とした。

#### ② 再配置機能（Re-placement）への予測基準値の伝達
- **課題**: 
  別担当者が開発する「再配置機能」では、Pod起動後の実際のリソース使用量と、「Managerが配置時に想定していた予測使用量」とを比較して乖離を検知する必要がある。しかし、従来のCREATE Interestには予測リソース量の絶対値が含まれていなかった。
- **方針**: 
  ManagerからSeedへ送るCREATE Interest（`/{node}/seed`）の `ApplicationParameters` に、配置計算で使用した確定要求リソース量 **`predicted_demand`（絶対値）** を含めて送信する。

---

### 8.2. 実装詳細

#### ① 絶対値から優先度への自動マッピング（`infer_preference_level`）
環境変数 `PREFERENCE_DEMAND` の基準値（low, medium, high）の中間値をしきい値として、入力された絶対値を自動判定する：

- **CPU**（基準: low 0.2, medium 0.5, high 1.0 コア）:
  - $\le 0.35$ コア $\to$ `low` (重み1)
  - $0.35 \sim 0.75$ コア $\to$ `medium` (重み2)
  - $> 0.75$ コア $\to$ `high` (重み3)
- **メモリ**（基準: low 256MB, medium 512MB, high 1GB）:
  - $\le 384$ MB $\to$ `low` / $384 \sim 768$ MB $\to$ `medium` / $> 768$ MB $\to$ `high`
- **帯域**（基準: low 20Mbps, medium 50Mbps, high 100Mbps）:
  - $\le 35$ Mbps $\to$ `low` / $35 \sim 75$ Mbps $\to$ `medium` / $> 75$ Mbps $\to$ `high`

#### ② 要求リソースの総合解決（`resolve_resource_requirements`）
- `demand` が指定された場合：
  - 指定された絶対値をそのまま配置計算（`balance_score` や 80%安全上限判定）に使用。
  - `preference` が未指定なら上記判定により自動設定（`preference_score` の加重平均重みに使用）。
- `demand` が未指定の場合（従来通り）：
  - `preference`（low/med/high）から `PREFERENCE_DEMAND` を引いて絶対値を算出。

#### ③ Seedへの確定要求リソースの転送（`SeedClient.create`）
- Managerはノード選定後、選定先ノードのSeedへ送るCREATE Interestの `ApplicationParameters` に `predicted_demand`（確定要求リソース量）を格納して送信。
- これにより、Seed側はクライアントが `preference` と `demand` のどちらで指定したかに関わらず、**常に一貫した絶対値の予測リソース（CPUコア数, メモリBytes, 帯域bps）を受領できる**。

---

### 8.3. 通信パケット仕様（命名規則とフォーマット）

#### 1. クライアント → Manager (`/Manager/register`)

```json
{
  "name": "average",
  "runtime": "python",
  "requirements": [],
  "code_ref": "/ndn-router-0/registerer/functions/average",
  "registerer_prefix": "/ndn-router-0/registerer/functions/average/status",
  
  // パターンA: 抽象優先度指定 (従来互換)
  "preference": {
    "cpu": "high",
    "memory": "medium",
    "bandwidth": "low"
  },

  // パターンB: 絶対値要求指定 (新設)
  // ※ demand 単体で指定可能 (Managerが自動的に preference を逆算)
  "demand": {
    "cpu": 0.8,
    "memory": 1073741824,
    "bandwidth": 50000000
  }
}
```

#### 2. Manager → Seed (`/{node}/seed`)

```json
{
  "type": "CREATE",
  "name": "average",
  "runtime": "python",
  "requirements": [],
  "code_ref": "/ndn-router-0/manager/functions/average",
  "registerer_prefix": "/ndn-router-0/registerer/functions/average/status",
  
  // ★ 再配置機能向けに付与される確定予測要求リソース
  "predicted_demand": {
    "cpu": 0.8,
    "memory": 1073741824,
    "bandwidth": 50000000
  }
}
```
