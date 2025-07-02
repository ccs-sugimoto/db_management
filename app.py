import streamlit as st

# --- ページ設定 ---
# Streamlitページの基本的な設定を行います。レイアウトはワイドモードを使用。
st.set_page_config(layout="wide")
st.title("データベース管理ツール") # アプリケーションのタイトル

# --- 状態管理の初期化 ---
# アプリケーション全体で使用するセッション状態を初期化します。
from state import initialize_session_state
initialize_session_state()

# --- UIコンポーネントの描画 ---
# 各UI部品をそれぞれのモジュールからインポートして描画します。

# サイドバーの描画
from views.sidebar import render_sidebar
render_sidebar()

# データベース接続UIの描画 (タブ形式)
from views.connection_ui import render_connection_tabs
with st.expander("接続先データベース設定", expanded=True):
    render_connection_tabs()

# データベース情報表示UIの描画
from views.db_info_ui import render_database_info_columns
with st.expander("データベース情報", expanded=True):
    render_database_info_columns()

# カラムマッピングUIの描画
from views.mapping_ui import render_mapping_ui
render_mapping_ui()

# データ移行・操作UIの描画
from views.data_migration_ui import render_data_migration_ui
render_data_migration_ui()

# --- メインの実行ブロック ---
# 通常のPythonスクリプトとして実行された場合の処理 (今回はStreamlitアプリなので直接は使用しないことが多い)
# if __name__ == "__main__":
#     pass
