# 基本
作業は"DockerPrac/work"に移動してから行う。
もし、追加したいパッケージがあるならdockerfileに追加。
後で修正します。これ全部

# Dockerでの通信
1. 前提として、docker-compose.yamlを編集して、通信で扱う役割のあるノードを定義しておこう。
2. ターミナルをノード分開き、"docker compose run --rm ~~"(bashで使いたいときは最後にbashをつける)とするとその名前のノードを起動できる。
3. "nfd-start"によって、NFDの起動
4. "ifconfig"によって自身のIPアドレスを取得。これはのちにFaceの作成などに使用
5. 相手側のIPアドレスを用いて、各ノードにFaceを作成する"nfdc face create udp://172.18.0.2"のようにするとFace作成できる。
6. 最後に、各ノードに対して、どのようなprefixがあるときに、そのノードにホップするのかを決定するため、"nfdc route add prefix /manager nexthop udp://172.18.0.2"のように入力して、ルーティングの経路を定める。
7. 注意点として、docker composeでDockerに入った後もworkディレクトリに移動しないといけない。


node_db.jsonについて
1. シミュレーションとして使用するエリア内の各ノードの性能を保持しておく。
2. cpuのスコアとしては、$$Node\_Score = \left( \frac{PassMark}{TDP} \right) \times (1 - Usage)$$を使おうかな

# NDNのNetworkの構築について
1. 前提としてMasterlab内のNDN-network-generator-main

#　個人メモ
・同期関数の中で非同期処理を行いたいのであれば、同期関数内に直接核と衝突が起きてタイムアウトしてしまうので、同期関数の中で別のタスクとして呼び出す方法をとらないといけない。
・ssh vmで対象の環境に飛べる。
・鍵とかの設定はssh-keygen -t ed25519で作ったところにGet-Content ~/.ssh/id_ed25519.pub | ssh 飛び先や踏み台の名前 "mkdir -p ~/.ssh; chmod 700 ~/.ssh; cat >> ~/.ssh/authorized_keys; chmod 600 ~/.ssh/authorized_keys"