import streamlit as st

def initialize_session_state():
    """
    Streamlitアプリケーションのセッション状態 (st.session_state) を初期化します。
    この関数はアプリケーションの起動時に一度だけ呼び出されることを想定しています。
    各キーが存在しない場合のみ、デフォルト値で初期化を行います。
    """

    # --- データベース接続関連の状態 ---
    # 選択中のデータベースタイプ (例: "postgresql", "sqlite")
    if "db_type" not in st.session_state:
        st.session_state.db_type = "postgresql"  # デフォルトはPostgreSQL

    # PostgreSQL接続パラメータのデフォルトテンプレート
    if "postgres_conn_params" not in st.session_state:
        st.session_state.postgres_conn_params = {
            "host": "localhost",
            "port": "5432",
            "db_name": "",  # ユーザーが入力
            "user": "",     # ユーザーが入力
            "password": "", # ユーザーが入力
        }

    # SQLite接続パラメータ (メタデータDB用)
    if "sqlite_conn_params" not in st.session_state:
        st.session_state.sqlite_conn_params = {
            "db_path": "metadata.db"  # デフォルトのメタデータDBファイルパス
        }

    # SQLAlchemyエンジンのインスタンス (接続後に設定される)
    if "source_engine" not in st.session_state: # 接続1 (ソース/ターゲット) 用エンジン
        st.session_state.source_engine = None
    if "target_engine" not in st.session_state: # 接続2 (ターゲット) 用エンジン
        st.session_state.target_engine = None
    if "metadata_engine" not in st.session_state: # メタデータDB用エンジン
        st.session_state.metadata_engine = None

    # 各接続フォーム固有のPostgreSQL接続パラメータ
    # これらは上記の postgres_conn_params テンプレートを基に、個別のデフォルト値を持つ
    if "source_postgres_conn_params" not in st.session_state:
        st.session_state.source_postgres_conn_params = st.session_state.postgres_conn_params.copy()
        st.session_state.source_postgres_conn_params["db_name"] = "source_db" # 例: ソースDB名
    if "target_postgres_conn_params" not in st.session_state:
        st.session_state.target_postgres_conn_params = st.session_state.postgres_conn_params.copy()
        st.session_state.target_postgres_conn_params["db_name"] = "target_db" # 例: ターゲットDB名

    # --- UIの状態管理 (主にデータベース情報表示関連) ---
    if "source_selected_table" not in st.session_state: # 接続1で選択中のテーブル名
        st.session_state.source_selected_table = None
    if "target_selected_table" not in st.session_state: # 接続2で選択中のテーブル名
        st.session_state.target_selected_table = None

    if "source_tables" not in st.session_state: # 接続1のテーブル名リスト
        st.session_state.source_tables = []
    if "target_tables" not in st.session_state: # 接続2のテーブル名リスト
        st.session_state.target_tables = []

    if "source_columns" not in st.session_state: # 接続1の選択中テーブルのカラム情報
        st.session_state.source_columns = []
    if "target_columns" not in st.session_state: # 接続2の選択中テーブルのカラム情報
        st.session_state.target_columns = []

    # --- カラムマッピング機能の状態 ---
    if "current_mapping_name" not in st.session_state: # 現在編集または読み込まれているマッピング設定の名前
        st.session_state.current_mapping_name = ""
    if "column_map" not in st.session_state: # 現在アクティブなカラムマッピング ({'ソースカラム': 'ターゲットカラム', ...})
        st.session_state.column_map = {}
    if "saved_mappings" not in st.session_state: # 保存済みのマッピング設定名リスト (メタデータDBから読み込む)
        st.session_state.saved_mappings = []

    # --- 注意事項 (開発者向けコメント) ---
    # 以下のコメントは、この初期化関数と各UIモジュール間の連携に関する補足です。

    # 動的なキーの初期化について:
    #   - `render_db_connection_form` 内で `f"{conn_key_prefix}_postgres_conn_params"` のような
    #     動的にキー名が生成されるセッション状態がありますが、これらは `conn_key_prefix` ('source' または 'target')
    #     に依存するため、この一元的な初期化関数では扱いにくいです。
    #     これらのキーは、`source_postgres_conn_params` や `target_postgres_conn_params` として
    #     上で初期化されているため、通常は `render_db_connection_form` 内の `st.session_state.get()`
    #     で適切に処理されます (キーが存在しない場合のデフォルト値提供など)。
    #
    #   - `display_db_info` で使用される `tables_key`, `selected_table_key`, `columns_key` も同様に動的です。
    #     これらは `st.session_state.get(key, default_value)` によって、
    #     キーが存在しない場合でもエラーなくデフォルト値 (例: []) で処理されるため、
    #     ここでの明示的な初期化は必須ではありません。主要なものは上でカバーしています。

    # 過去のバージョンの初期化ロジックについて:
    #   - `app.py` の古いバージョンにあった `with conn_tab1:` 内の初期化ブロックなどは、
    #     この `initialize_session_state` 関数に統合・整理されました。
    #     `source_postgres_conn_params` や `target_postgres_conn_params` の初期化がそれらに該当します。
    #
    #   - `app.py` の複数箇所に散らばっていた `if "key" not in st.session_state:` という形式の
    #     チェックと初期化は、原則としてこの関数に集約されています。

    # 実行時の状態更新について:
    #   - `st.session_state.saved_mappings` は、サイドバーでメタデータDBに接続した際や、
    #     マッピングUIで保存・削除操作を行った際に動的に更新されます。初期値は空リストです。
    #
    #   - `st.session_state.column_map` や `st.session_state.current_mapping_name` も
    #     マッピングUIの操作によって更新されます。

    pass # 関数が空ブロックと解釈されるのを防ぐためのpass文 (全てのif文がFalseの場合など)
