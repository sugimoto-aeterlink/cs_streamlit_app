Google ColabでStreamlitアプリを立ち上げる方法
このガイドでは、Google Colaboratory (Colab) 上でStreamlitアプリケーションを起動し、ngrokを使用して外部から安定してアクセスするための、確実な手順を解説します。

特に、セルの再実行時などに頻発する「ポートが既に使用されています (Port already in use)」というエラーへの対処法を組み込んだ、実践的な内容となっています。

1. 準備：必要なもの
ngrokアカウントと認証トークン (Authtoken)

ngrok公式サイトで無料アカウントを作成し、ダッシュボードから認証トークンをコピーしておきます。

Colab Secretsへのトークン設定

Colabノートブックの左パネルにある「🔑」（鍵アイコン）をクリックします。

「新しいシークレットを追加」を選び、以下のように設定します。

名前: NGROK_AUTHTOKEN

値: コピーしたngrok認証トークンを貼り付け

「ノートブックのアクセス権」のスイッチをオンにします。

2. 実行手順
以下の3つのセルを順番にColabで実行することで、Streamlitアプリが起動し、公開URLが発行されます。

ステップ1：必要なライブラリのインストール
最初に、Streamlitとpyngrokをインストールします。このセルはノートブックを開くたびに一度だけ実行すればOKです。

!pip install streamlit pyngrok


ステップ2：Streamlitアプリケーションのコード作成
%%writefileマジックコマンドを使い、Streamlitのコードをapp.pyというファイルに書き出します。

%%writefile app.py
import streamlit as st

st.set_page_config(
    page_title="Colab-Streamlit App",
    page_icon="🎈",
    layout="centered"
)

st.title("ようこそ！Colab Streamlitアプリへ")
st.write("これはGoogle Colab上で実行されています。")

name = st.text_input("名前を入力してください:")
if name:
    st.success(f"こんにちは、{name}さん！")

st.info("このアプリはngrokを通じて公開されています。")


ステップ3：アプリの起動とURLの発行
このセルがメインの処理です。以下の機能がすべて含まれています。

古いプロセスの強制終了: セルを再実行した際に「Port already in use」エラーが出るのを防ぎます。

ngrok認証: Colab Secretsから安全に認証トークンを読み込みます。

バックグラウンド実行: Streamlitアプリをバックグラウンドで起動し、ログをstreamlit.logに出力します。

トンネル作成: ngrokトンネルを作成し、公開URLを表示します。

import os
import subprocess
import time
from pyngrok import ngrok, conf
from google.colab import userdata

# --- 前準備：古いプロセスを確実に終了させる ---
# Streamlitのデフォルトポート8501を使用しているプロセスを検索し、強制終了
try:
    result = subprocess.run(["fuser", "8501/tcp"], capture_output=True, text=True)
    pid = result.stdout.strip()
    if pid:
        print(f"Port 8501 is used by process {pid}. Killing it...")
        subprocess.run(["kill", "-9", pid])
        print("Process killed.")
except FileNotFoundError:
    print("fuser command not found, skipping process kill. (This is normal if not on Linux)")
except Exception as e:
    print(f"Error killing old process: {e}")

# ngrokの既存のトンネルも終了
try:
    ngrok.kill()
    print("Killed existing ngrok tunnels.")
except Exception as e:
    print(f"No existing ngrok processes to kill or error: {e}")

# --- ngrok認証トークンの設定 ---
try:
    NGROK_TOKEN = userdata.get('NGROK_AUTHTOKEN')
    if not NGROK_TOKEN:
        raise ValueError("NGROK_AUTHTOKENが見つかりません。Colab Secretsで設定してください。")
    conf.get_default().auth_token = NGROK_TOKEN
    print("✅ ngrok認証トークンの設定が完了しました。")
except Exception as e:
    print(f"⚠️ ngrok認証トークンの設定エラー: {e}")

# --- Streamlitアプリのバックグラウンド実行 ---
# ログは streamlit.log に出力されます
os.system("nohup streamlit run app.py --server.port 8501 &> streamlit.log &")
print("🚀 Streamlitアプリをバックグラウンドで起動しました。")

# Streamlitサーバーが起動するのを少し待つ
print("⏳ Streamlitサーバーの起動を5秒間待機します...")
time.sleep(5)


# --- ngrokトンネルの作成とURL表示 ---
try:
    public_url = ngrok.connect(8501)
    print("🎉 アプリケーションの準備ができました！")
    print(f"🌐 公開URL: {public_url}")
except Exception as e:
    print(f"🚨 ngrokトンネルの作成に失敗しました: {e}")
    print("ログを確認してください: !cat streamlit.log")



これで設定は完了です！ 出力された公開URLにアクセスしてください。

3. トラブルシューティング
もしアプリが正常に表示されない場合は、以下のコマンドでログを確認してください。エラーの原因究明に役立ちます。

!cat streamlit.log
