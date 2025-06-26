import streamlit as st
from db_utils import test_postgres_connection, test_sqlite_connection, get_db_engine

# --- アプリケーションの状態管理 ---
if "db_type" not in st.session_state:
    st.session_state.db_type = "postgresql"  # デフォルトはPostgreSQL
if "postgres_conn_params" not in st.session_state:
    st.session_state.postgres_conn_params = {
        "host": "localhost",
        "port": "5432",
        "db_name": "",
        "user": "",
        "password": "",
    }
if "sqlite_conn_params" not in st.session_state:
    st.session_state.sqlite_conn_params = {
        "db_path": "metadata.db"  # カラム情報などを保存するSQLite DB
    }
if "source_engine" not in st.session_state:
    st.session_state.source_engine = None
if "target_engine" not in st.session_state:
    st.session_state.target_engine = None
if "metadata_engine" not in st.session_state:  # カラム情報などを保存するSQLite DB用
    st.session_state.metadata_engine = None


st.set_page_config(layout="wide")
st.title("データベース管理ツール")

# --- サイドバー: データベース接続設定 ---
st.sidebar.header("接続設定")
db_type_options = ["postgresql", "他のDB (未実装)"]  # 今後の拡張用
st.session_state.db_type = st.sidebar.selectbox(
    "データベースタイプ",
    db_type_options,
    index=db_type_options.index(st.session_state.db_type),
)

# --- メタデータ保存用SQLite DBの接続 ---
st.sidebar.subheader("メタデータDB (SQLite)")
sqlite_path = st.sidebar.text_input(
    "SQLite データベースファイルパス",
    value=st.session_state.sqlite_conn_params["db_path"],
    key="metadata_sqlite_path",
)
st.session_state.sqlite_conn_params["db_path"] = sqlite_path

if st.sidebar.button("メタデータDB接続テスト/接続", key="connect_metadata_db"):
    if sqlite_path:
        success, message = test_sqlite_connection(sqlite_path)
        if success:
            st.sidebar.success(message)
            st.session_state.metadata_engine = get_db_engine(
                "sqlite", {"db_path": sqlite_path}
            )
            if st.session_state.metadata_engine:
                st.sidebar.info("メタデータDBへのエンジン取得成功")
            else:
                st.sidebar.error("メタデータDBへのエンジン取得失敗")
        else:
            st.sidebar.error(message)
            st.session_state.metadata_engine = None
    else:
        st.sidebar.warning("SQLiteデータベースのパスを入力してください。")

if st.session_state.metadata_engine:
    st.sidebar.success(f"メタデータDB ({sqlite_path}) に接続済み")
else:
    st.sidebar.warning("メタデータDBに接続してください。")


# --- メインエリア: 接続先DBの設定 ---
st.header("接続先データベース")

conn_tab1, conn_tab2 = st.tabs(
    ["接続1 (ソース/ターゲット)", "接続2 (ターゲットDB - 任意)"]
)


def render_db_connection_form(conn_key_prefix):
    st.subheader(f"{conn_key_prefix.capitalize()} データベース設定")

    if st.session_state.db_type == "postgresql":
        pg_params = st.session_state.get(
            f"{conn_key_prefix}_postgres_conn_params",
            {
                "host": "localhost",
                "port": "5432",
                "db_name": "",
                "user": "",
                "password": "",
            },
        )

        pg_params["host"] = st.text_input(
            f"ホスト [{conn_key_prefix}]",
            value=pg_params["host"],
            key=f"{conn_key_prefix}_pg_host",
        )
        pg_params["port"] = st.text_input(
            f"ポート [{conn_key_prefix}]",
            value=pg_params["port"],
            key=f"{conn_key_prefix}_pg_port",
        )
        pg_params["db_name"] = st.text_input(
            f"データベース名 [{conn_key_prefix}]",
            value=pg_params["db_name"],
            key=f"{conn_key_prefix}_pg_dbname",
        )
        pg_params["user"] = st.text_input(
            f"ユーザー名 [{conn_key_prefix}]",
            value=pg_params["user"],
            key=f"{conn_key_prefix}_pg_user",
        )
        pg_params["password"] = st.text_input(
            f"パスワード [{conn_key_prefix}]",
            type="password",
            value=pg_params["password"],
            key=f"{conn_key_prefix}_pg_password",
        )

        st.session_state[f"{conn_key_prefix}_postgres_conn_params"] = pg_params

        if st.button(
            f"接続テスト ({conn_key_prefix} - PostgreSQL)",
            key=f"{conn_key_prefix}_pg_test_conn",
        ):
            success, message = test_postgres_connection(**pg_params)
            if success:
                st.success(message)
                engine = get_db_engine("postgresql", pg_params)
                if engine:
                    st.info(f"PostgreSQLエンジン ({conn_key_prefix}) 取得成功")
                    if conn_key_prefix == "source":
                        st.session_state.source_engine = engine
                    elif conn_key_prefix == "target":
                        st.session_state.target_engine = engine
                else:
                    st.error(f"PostgreSQLエンジン ({conn_key_prefix}) 取得失敗")
                    if conn_key_prefix == "source":
                        st.session_state.source_engine = None
                    elif conn_key_prefix == "target":
                        st.session_state.target_engine = None

            else:
                st.error(message)
                if conn_key_prefix == "source":
                    st.session_state.source_engine = None
                elif conn_key_prefix == "target":
                    st.session_state.target_engine = None
    # 他のDBタイプはここにelifで追加

    # 接続状態の表示
    current_engine = None
    if conn_key_prefix == "source":
        current_engine = st.session_state.source_engine
    elif conn_key_prefix == "target":
        current_engine = st.session_state.target_engine

    if current_engine:
        st.success(
            f"{conn_key_prefix.capitalize()} DBに接続済み ({current_engine.url})"
        )
    else:
        st.warning(f"{conn_key_prefix.capitalize()} DBに接続されていません。")


with conn_tab1:
    st.write("データ移行のソースDB、または単一DB操作時のターゲットDBとして使用します。")
    if "source_postgres_conn_params" not in st.session_state:
        st.session_state.source_postgres_conn_params = {
            "host": "localhost",
            "port": "5432",
            "db_name": "source_db",
            "user": "user",
            "password": "password",
        }
    render_db_connection_form("source")


with conn_tab2:
    st.write(
        "データ移行のターゲットDBとして使用します。接続1と同じDBを指定することも可能です。"
    )
    if "target_postgres_conn_params" not in st.session_state:
        st.session_state.target_postgres_conn_params = {
            "host": "localhost",
            "port": "5432",
            "db_name": "target_db",
            "user": "user",
            "password": "password",
        }
    render_db_connection_form("target")


# --- アプリケーションの状態管理 ---
if "db_type" not in st.session_state:
    st.session_state.db_type = "postgresql"  # デフォルトはPostgreSQL
# ... (既存のセッション状態の初期化はそのまま) ...
if "source_selected_table" not in st.session_state:
    st.session_state.source_selected_table = None
if "target_selected_table" not in st.session_state:
    st.session_state.target_selected_table = None
if "source_tables" not in st.session_state:
    st.session_state.source_tables = []
if "target_tables" not in st.session_state:
    st.session_state.target_tables = []
if "source_columns" not in st.session_state:
    st.session_state.source_columns = []
if "target_columns" not in st.session_state:
    st.session_state.target_columns = []


st.set_page_config(layout="wide")
st.title("データベース管理ツール")

# --- サイドバー: データベース接続設定 ---
st.sidebar.header("接続設定")
db_type_options = ["postgresql", "他のDB (未実装)"]  # 今後の拡張用
st.session_state.db_type = st.sidebar.selectbox(
    "データベースタイプ",
    db_type_options,
    index=db_type_options.index(st.session_state.db_type)
    if st.session_state.db_type in db_type_options
    else 0,
)

# --- メタデータ保存用SQLite DBの接続 ---
st.sidebar.subheader("メタデータDB (SQLite)")
sqlite_path = st.sidebar.text_input(
    "SQLite データベースファイルパス",
    value=st.session_state.sqlite_conn_params["db_path"],
    key="metadata_sqlite_path",
)
st.session_state.sqlite_conn_params["db_path"] = sqlite_path

if st.sidebar.button("メタデータDB接続テスト/接続", key="connect_metadata_db"):
    if sqlite_path:
        try:
            # get_db_engine内で接続テストも行われる
            engine = get_db_engine("sqlite", {"db_path": sqlite_path})
            if engine:
                st.session_state.metadata_engine = engine
                st.sidebar.success(
                    f"メタデータDB ({sqlite_path}) への接続に成功しました。"
                )
            else:
                # get_db_engine内でエラーが表示されるはずだが念のため
                st.sidebar.error(
                    f"メタデータDB ({sqlite_path}) への接続に失敗しました。"
                )
                st.session_state.metadata_engine = None
        except Exception as e:
            st.sidebar.error(f"メタデータDB接続エラー: {e}")
            st.session_state.metadata_engine = None
    else:
        st.sidebar.warning("SQLiteデータベースのパスを入力してください。")

if st.session_state.metadata_engine:
    st.sidebar.success(f"メタデータDB ({sqlite_path}) に接続済み")
else:
    st.sidebar.warning("メタデータDBに接続してください。")


# --- メインエリア: 接続先DBの設定 ---
st.header("接続先データベース")

conn_tab1, conn_tab2 = st.tabs(
    ["接続1 (ソース/ターゲット)", "接続2 (ターゲットDB - 任意)"]
)


def render_db_connection_form(conn_key_prefix):
    st.subheader(f"{conn_key_prefix.capitalize()} データベース設定")

    db_engine_key = f"{conn_key_prefix}_engine"
    db_params_key = f"{conn_key_prefix}_postgres_conn_params"  # 現状PostgreSQLのみ想定

    if st.session_state.db_type == "postgresql":
        if db_params_key not in st.session_state:
            st.session_state[db_params_key] = {
                "host": "localhost",
                "port": "5432",
                "db_name": f"{conn_key_prefix}_db",
                "user": "user",
                "password": "password",
            }
        pg_params = st.session_state[db_params_key]

        pg_params["host"] = st.text_input(
            f"ホスト [{conn_key_prefix}]",
            value=pg_params["host"],
            key=f"{conn_key_prefix}_pg_host",
        )
        pg_params["port"] = st.text_input(
            f"ポート [{conn_key_prefix}]",
            value=pg_params["port"],
            key=f"{conn_key_prefix}_pg_port",
        )
        pg_params["db_name"] = st.text_input(
            f"データベース名 [{conn_key_prefix}]",
            value=pg_params["db_name"],
            key=f"{conn_key_prefix}_pg_dbname",
        )
        pg_params["user"] = st.text_input(
            f"ユーザー名 [{conn_key_prefix}]",
            value=pg_params["user"],
            key=f"{conn_key_prefix}_pg_user",
        )
        pg_params["password"] = st.text_input(
            f"パスワード [{conn_key_prefix}]",
            type="password",
            value=pg_params["password"],
            key=f"{conn_key_prefix}_pg_password",
        )

        st.session_state[db_params_key] = pg_params

        if st.button(
            f"接続テスト/接続 ({conn_key_prefix} - PostgreSQL)",
            key=f"{conn_key_prefix}_pg_connect",
        ):
            try:
                engine = get_db_engine("postgresql", pg_params)
                if engine:
                    st.session_state[db_engine_key] = engine
                    st.success(
                        f"PostgreSQL ({conn_key_prefix}) への接続に成功しました。"
                    )
                    # 接続成功時にテーブル情報を更新
                    try:
                        if conn_key_prefix == "source":
                            st.session_state.source_tables = get_table_names(engine)
                            st.session_state.source_selected_table = None
                            st.session_state.source_columns = []
                        elif conn_key_prefix == "target":
                            st.session_state.target_tables = get_table_names(engine)
                            st.session_state.target_selected_table = None
                            st.session_state.target_columns = []
                    except RuntimeError as e:
                        st.error(f"テーブル一覧の取得に失敗しました: {e}")
                        if conn_key_prefix == "source":
                            st.session_state.source_tables = []
                        elif conn_key_prefix == "target":
                            st.session_state.target_tables = []
                else:
                    st.session_state[db_engine_key] = None
                    st.error(f"PostgreSQL ({conn_key_prefix}) への接続に失敗しました。")
            except Exception as e:
                st.session_state[db_engine_key] = None
                st.error(f"PostgreSQL ({conn_key_prefix}) 接続エラー: {e}")
    # 他のDBタイプはここにelifで追加

    # 接続状態の表示
    current_engine = st.session_state.get(db_engine_key)
    if current_engine:
        st.success(
            f"{conn_key_prefix.capitalize()} DBに接続済み ({current_engine.url})"
        )
    else:
        st.warning(f"{conn_key_prefix.capitalize()} DBに接続されていません。")


with conn_tab1:
    st.write("データ移行のソースDB、または単一DB操作時のターゲットDBとして使用します。")
    render_db_connection_form("source")


with conn_tab2:
    st.write(
        "データ移行のターゲットDBとして使用します。接続1と同じDBを指定することも可能です。"
    )
    render_db_connection_form("target")


# --- データベース情報表示 ---
st.header("データベース情報")


def display_db_info(engine, tables_key, selected_table_key, columns_key, db_label):
    if engine:
        st.subheader(f"{db_label} 情報")
        tables = st.session_state.get(tables_key, [])
        if (
            not tables and engine
        ):  # セッションにない場合、かつエンジンが存在する場合のみ再取得試行
            try:
                tables = get_table_names(engine)
                st.session_state[tables_key] = tables
            except RuntimeError as e:
                st.error(f"{db_label} のテーブル一覧取得に失敗: {e}")
                st.session_state[tables_key] = []  # エラー時は空リストに
                tables = []  # ローカル変数も更新

        if tables:
            # テーブル選択のインデックス計算を安全に
            current_selected_table = st.session_state.get(selected_table_key)
            try:
                select_idx = (
                    tables.index(current_selected_table) + 1
                    if current_selected_table and current_selected_table in tables
                    else 0
                )
            except (
                ValueError
            ):  # current_selected_table が tables にない場合(古いキャッシュなど)
                select_idx = 0
                st.session_state[selected_table_key] = None  # 選択をリセット

            selected_table = st.selectbox(
                f"{db_label} テーブル一覧",
                options=[""] + tables,  # 未選択オプションを追加
                index=select_idx,
                key=f"{db_label}_table_select",
            )
            st.session_state[selected_table_key] = (
                selected_table if selected_table else None
            )

            if st.session_state.get(selected_table_key):
                try:
                    columns = get_table_columns(
                        engine, st.session_state.get(selected_table_key)
                    )
                    st.session_state[columns_key] = columns
                    if columns:
                        st.write(
                            f"テーブル '{st.session_state.get(selected_table_key)}' のカラム:"
                        )
                        st.dataframe(columns, use_container_width=True)
                    else:
                        st.info(
                            f"テーブル '{st.session_state.get(selected_table_key)}' にカラムが見つかりません。"
                        )
                except RuntimeError as e:
                    st.error(
                        f"{db_label} のテーブル '{st.session_state.get(selected_table_key)}' のカラム情報取得に失敗: {e}"
                    )
                    st.session_state[columns_key] = []  # エラー時は空リストに
            else:
                st.session_state[columns_key] = []  # テーブル未選択時はカラム情報クリア
        else:
            if engine:  # エンジンはあるがテーブルがない場合
                st.info(f"{db_label} にテーブルが見つかりません。")
            # エンジンがない場合は display_db_info の冒頭で警告が出ているはず
    else:
        st.warning(f"{db_label} に接続されていません。")


info_col1, info_col2 = st.columns(2)

with info_col1:
    display_db_info(
        st.session_state.get("source_engine"),
        "source_tables",
        "source_selected_table",
        "source_columns",
        "接続1 (ソース)",
    )

with info_col2:
    if st.session_state.get(
        "target_engine"
    ):  # ターゲットDBが接続されている場合のみ表示
        display_db_info(
            st.session_state.get("target_engine"),
            "target_tables",
            "target_selected_table",
            "target_columns",
            "接続2 (ターゲット)",
        )
    else:
        st.info("接続2 (ターゲットDB) は接続されていません。")


from db_utils import (  # インポートを整理
    test_postgres_connection,
    test_sqlite_connection,
    get_db_engine,
    get_table_names,
    get_table_columns,
    create_metadata_tables_if_not_exists,
    save_column_mapping,
    get_mapping_config_names,
    load_column_mapping,
    delete_column_mapping,
)
import pandas as pd  # DataFrame表示用にインポート

# --- アプリケーションの状態管理 ---
# ... (既存のセッション状態) ...
if "current_mapping_name" not in st.session_state:
    st.session_state.current_mapping_name = ""
if "column_map" not in st.session_state:  # {'source_col': 'target_col', ...}
    st.session_state.column_map = {}
if "saved_mappings" not in st.session_state:  # 保存済みのマッピング名リスト
    st.session_state.saved_mappings = []


st.set_page_config(layout="wide")
st.title("データベース管理ツール")

# --- サイドバー: データベース接続設定 ---
# ... (既存のサイドバーコード) ...
# メタデータDB接続成功時にテーブル作成を試みる
if st.sidebar.button("メタデータDB接続テスト/接続", key="connect_metadata_db"):
    if sqlite_path:
        try:
            engine = get_db_engine("sqlite", {"db_path": sqlite_path})
            if engine:
                st.session_state.metadata_engine = engine
                create_metadata_tables_if_not_exists(
                    st.session_state.metadata_engine
                )  # テーブル作成/確認
                st.sidebar.success(
                    f"メタデータDB ({sqlite_path}) への接続とテーブル準備が完了しました。"
                )
                # 保存済みマッピング一覧を読み込む
                st.session_state.saved_mappings = get_mapping_config_names(
                    st.session_state.metadata_engine
                )
            else:
                st.sidebar.error(
                    f"メタデータDB ({sqlite_path}) への接続に失敗しました。"
                )
                st.session_state.metadata_engine = None
        except Exception as e:
            st.sidebar.error(f"メタデータDB処理エラー: {e}")
            st.session_state.metadata_engine = None
    else:
        st.sidebar.warning("SQLiteデータベースのパスを入力してください。")

# ... (既存のメインエリア: 接続先DBの設定) ...


# --- データベース情報表示 ---
# ... (既存のデータベース情報表示コード) ...


# --- カラムマッピング機能 ---
st.header("カラムマッピング設定")

if st.session_state.get("metadata_engine"):
    if not st.session_state.get("source_engine"):
        st.warning(
            "カラムマッピングを行うには、まず「接続1 (ソース)」を設定・接続してください。"
        )
    elif not st.session_state.get("source_selected_table"):
        st.warning("ソースDBに接続後、ソーステーブルを選択してください。")
    # ターゲットテーブルは任意（同一テーブルへの操作やINSERT文生成なども考慮）
    # elif not st.session_state.get("target_engine") or not st.session_state.get("target_selected_table"):
    #     st.warning("ターゲットDBに接続後、ターゲットテーブルを選択してください。")
    else:
        source_cols = [
            col["name"] for col in st.session_state.get("source_columns", [])
        ]
        # ターゲットカラムは、ターゲットDBが接続されていればそこから、なければ空リスト or 手入力
        target_cols_options = [""]  # 未選択を許容
        if st.session_state.get("target_engine") and st.session_state.get(
            "target_selected_table"
        ):
            target_cols_options.extend(
                [col["name"] for col in st.session_state.get("target_columns", [])]
            )

        # マッピング管理UI
        map_col1, map_col2 = st.columns([2, 1])
        with map_col1:
            st.subheader("現在のマッピング")
            # マッピング入力UIの改善
            if source_cols:
                new_mapping = {}
                mapping_data = []
                for i, src_col in enumerate(source_cols):
                    # st.session_state.column_map から現在のマッピングを取得
                    # キーが存在しない場合やNoneの場合はデフォルトで未選択("")
                    current_tgt_col = st.session_state.column_map.get(src_col, "")

                    # ターゲットカラムの選択肢に現在のマッピング値が含まれているか確認
                    # 含まれていなければ、一時的に選択肢に追加して表示できるようにする（ロード時など）
                    temp_target_cols_options = list(
                        target_cols_options
                    )  # コピーして操作
                    if (
                        current_tgt_col
                        and current_tgt_col not in temp_target_cols_options
                    ):
                        temp_target_cols_options.append(current_tgt_col)

                    # selectboxのindexを決定
                    try:
                        idx = temp_target_cols_options.index(current_tgt_col)
                    except ValueError:
                        idx = 0  # 見つからなければ未選択

                    selected_tgt_col = st.selectbox(
                        f"`{src_col}` (ソース)  -> ",
                        options=temp_target_cols_options,
                        index=idx,
                        key=f"map_{src_col}_to_{i}",  # ユニークキー
                    )
                    if selected_tgt_col:  # 空文字でない場合のみマッピング対象
                        new_mapping[src_col] = selected_tgt_col
                    mapping_data.append(
                        {
                            "ソースカラム": src_col,
                            "ターゲットカラム": selected_tgt_col or "--- 未選択 ---",
                        }
                    )

                if st.button("このマッピングを適用", key="apply_current_mapping"):
                    st.session_state.column_map = new_mapping
                    st.success("現在のマッピングを更新しました。")

                if st.session_state.column_map:
                    st.write("適用中のマッピング:")
                    st.dataframe(
                        pd.DataFrame(
                            list(st.session_state.column_map.items()),
                            columns=["ソースカラム", "ターゲットカラム"],
                        ),
                        use_container_width=True,
                    )

            else:
                st.info("ソーステーブルのカラムを読み込んでください。")

        with map_col2:
            st.subheader("マッピングの保存と読み込み")
            # 保存済みマッピング一覧を更新
            if st.session_state.get("metadata_engine"):
                st.session_state.saved_mappings = get_mapping_config_names(
                    st.session_state.metadata_engine
                )

            current_mapping_name_input = st.text_input(
                "マッピング名",
                value=st.session_state.current_mapping_name,
                key="mapping_name_input",
            )
            st.session_state.current_mapping_name = current_mapping_name_input

            if st.button("現在のマッピングを保存", key="save_mapping_button"):
                if not st.session_state.current_mapping_name:
                    st.error("マッピング名を入力してください。")
                elif not st.session_state.column_map:
                    st.error("保存するカラムマッピングがありません。")
                elif not st.session_state.get("source_selected_table"):
                    st.error("ソーステーブルが選択されていません。")
                # ターゲットテーブルは必須ではない（データ移行以外での利用も想定）
                # elif not st.session_state.get("target_selected_table"):
                #     st.error("ターゲットテーブルが選択されていません。")
                else:
                    source_db_url_str = (
                        str(st.session_state.source_engine.url)
                        if st.session_state.get("source_engine")
                        else ""
                    )
                    target_db_url_str = (
                        str(st.session_state.target_engine.url)
                        if st.session_state.get("target_engine")
                        else ""
                    )

                    success, message = save_column_mapping(
                        st.session_state.metadata_engine,
                        st.session_state.current_mapping_name,
                        source_db_url_str,
                        target_db_url_str,
                        st.session_state.source_selected_table,
                        st.session_state.get(
                            "target_selected_table", ""
                        ),  # 未選択の場合は空文字
                        st.session_state.column_map,
                    )
                    if success:
                        st.success(message)
                        st.session_state.saved_mappings = get_mapping_config_names(
                            st.session_state.metadata_engine
                        )  # リスト更新
                    else:
                        st.error(message)

            st.markdown("---")

            if st.session_state.saved_mappings:
                selected_map_to_load = st.selectbox(
                    "保存済みマッピングを選択",
                    options=[""] + st.session_state.saved_mappings,
                    key="load_mapping_select",
                )

                col_load, col_delete = st.columns(2)
                with col_load:
                    if st.button(
                        "選択したマッピングを読み込み",
                        disabled=not selected_map_to_load,
                        key="load_mapping_button",
                    ):
                        config_details, mappings = load_column_mapping(
                            st.session_state.metadata_engine, selected_map_to_load
                        )
                        if (
                            config_details and mappings is not None
                        ):  # mappings can be empty dict
                            st.session_state.current_mapping_name = config_details[
                                "name"
                            ]
                            st.session_state.column_map = mappings
                            # TODO: 必要であれば、保存されていたDB情報やテーブル名に基づいて
                            # 現在の接続やテーブル選択を更新する処理を追加する
                            st.info(
                                f"マッピング '{selected_map_to_load}' を読み込みました。"
                            )
                            st.info(
                                f"ソーステーブル: {config_details['source_table']}, ターゲットテーブル: {config_details['target_table'] or 'N/A'}"
                            )
                            # UIを再描画して selectbox に反映させるために rerun
                            st.rerun()
                        else:
                            st.error(
                                f"マッピング '{selected_map_to_load}' の読み込みに失敗しました。"
                            )

                with col_delete:
                    if st.button(
                        "選択したマッピングを削除",
                        type="secondary",
                        disabled=not selected_map_to_load,
                        key="delete_mapping_button",
                    ):
                        success, message = delete_column_mapping(
                            st.session_state.metadata_engine, selected_map_to_load
                        )
                        if success:
                            st.success(message)
                            st.session_state.saved_mappings = get_mapping_config_names(
                                st.session_state.metadata_engine
                            )  # リスト更新
                            if (
                                st.session_state.current_mapping_name
                                == selected_map_to_load
                            ):
                                st.session_state.current_mapping_name = ""  # 削除されたマッピングが現在選択中のものならクリア
                                st.session_state.column_map = {}
                            st.rerun()
                        else:
                            st.error(message)
            else:
                st.info("保存されているマッピングはありません。")

else:
    st.warning(
        "カラムマッピング機能を利用するには、まずサイドバーからメタデータDBに接続してください。"
    )


from db_utils import (  # インポートを整理
    test_postgres_connection,
    test_sqlite_connection,
    get_db_engine,
    get_table_names,
    get_table_columns,
    create_metadata_tables_if_not_exists,
    save_column_mapping,
    get_mapping_config_names,
    load_column_mapping,
    delete_column_mapping,
    migrate_data,  # 追加
    generate_insert_statement,
    insert_record,
)
import pandas as pd

# ... (既存のセッション状態など) ...


# ... (既存のUIコード: サイドバー、接続設定、DB情報表示、カラムマッピング) ...
# カラムマッピング機能の最後尾に st.rerun() がある場合、それより下に書かれたコードは
# rerun時に実行されないことがあるので注意。データ操作セクションはマッピングセクションの外に配置。


# --- データ移行機能 ---
st.header("データ操作")

if not st.session_state.get("metadata_engine"):
    st.warning("データ操作を行うには、まずメタデータDBに接続してください。")
elif not st.session_state.get("source_engine"):
    st.warning("データ操作を行うには、まず「接続1 (ソースDB)」に接続してください。")
elif not st.session_state.get("source_selected_table"):
    st.warning("データ操作を行うには、ソーステーブルを選択してください。")
elif not st.session_state.column_map:
    st.warning("データ操作を行うには、カラムマッピングを設定・適用してください。")
else:
    # データ移行UI
    st.subheader("データ移行")

    # ターゲットDBとテーブルが選択されているか確認
    ready_for_migration = True
    if not st.session_state.get("target_engine"):
        st.warning(
            "データ移行を行うには、「接続2 (ターゲットDB)」にも接続してください。"
        )
        ready_for_migration = False

    if not st.session_state.get("target_selected_table"):
        st.warning("データ移行を行うには、ターゲットテーブルも選択してください。")
        ready_for_migration = False

    # マッピングされたターゲットカラムがターゲットテーブルに存在するか簡易チェック
    # (より厳密にはデータ型なども見るべきだが、ここでは存在のみ)
    mapped_target_cols = list(st.session_state.column_map.values())
    actual_target_cols = [
        col["name"] for col in st.session_state.get("target_columns", [])
    ]

    missing_cols_in_target = [
        mtc for mtc in mapped_target_cols if mtc not in actual_target_cols
    ]
    if (
        missing_cols_in_target and ready_for_migration
    ):  # ターゲットDB/テーブルが選択されている場合のみチェック
        st.warning(
            f"警告: マッピングされたターゲットカラム {missing_cols_in_target} が、選択されたターゲットテーブル '{st.session_state.target_selected_table}' のカラムに存在しません。移行前に確認してください。"
        )
        # ready_for_migration = False # 警告に留め、実行は可能にするか、ここでFalseにするか選択

    st.markdown(f"""
    以下の設定でデータを移行します:
    - **ソースDB:** `{st.session_state.source_engine.url if st.session_state.source_engine else "未接続"}`
    - **ソーステーブル:** `{st.session_state.source_selected_table}`
    - **ターゲットDB:** `{st.session_state.target_engine.url if st.session_state.target_engine else "未接続"}`
    - **ターゲットテーブル:** `{st.session_state.target_selected_table if st.session_state.target_selected_table else "未選択"}`
    - **適用中のカラムマッピング:**
    """)
    if st.session_state.column_map:
        st.json(st.session_state.column_map)  # JSON形式で見やすく表示
    else:
        st.write("なし")

    chunk_size = st.number_input(
        "一度に処理する行数 (チャンクサイズ)",
        min_value=100,
        max_value=10000,
        value=1000,
        step=100,
        key="chunk_size_migration",
    )

    if st.button(
        "データ移行実行",
        disabled=not ready_for_migration,
        type="primary",
        key="execute_migration_button",
    ):
        with st.spinner("データ移行を実行中..."):
            success, message = migrate_data(
                st.session_state.source_engine,
                st.session_state.target_engine,
                st.session_state.source_selected_table,
                st.session_state.target_selected_table,
                st.session_state.column_map,
                chunksize=chunk_size,
            )
        if success:
            st.success(message)
        else:
            st.error(message)

    st.markdown("---")  # データ移行とINSERT文発行の区切り

    # --- INSERT文発行機能 ---
    st.subheader("単一レコードのINSERT")

    if not st.session_state.get(
        "source_engine"
    ):  # INSERTは主にソースDBに対して行う想定
        st.info(
            "INSERT文を発行するには、「接続1 (ソースDB)」に接続し、テーブルを選択してください。"
        )
    elif not st.session_state.get("source_selected_table"):
        st.info("INSERT対象のテーブルを「接続1 (ソースDB)」から選択してください。")
    else:
        source_table_for_insert = st.session_state.source_selected_table
        source_columns_for_insert = st.session_state.get(
            "source_columns", []
        )  # name, typeを持つ辞書のリスト

        if not source_columns_for_insert:
            st.warning(
                f"テーブル '{source_table_for_insert}' のカラム情報が読み込まれていません。"
            )
        else:
            st.markdown(
                f"**テーブル `{source_table_for_insert}` にデータを挿入します。**"
            )

            # ユーザー入力フォームの作成
            # st.form を使うと、複数の入力ウィジェットをグループ化し、一度に送信できる
            with st.form(key="insert_form"):
                insert_data = {}
                for col_info in source_columns_for_insert:
                    col_name = col_info["name"]
                    col_type = col_info[
                        "type"
                    ]  # 型情報も表示（入力バリデーションは簡略化）

                    # Streamlitの入力ウィジェットは型にある程度対応するが、
                    # DBの厳密な型とは異なる場合があるので注意。ここでは主にテキスト入力。
                    # TODO: より高度な型チェックや専用ウィジェットの利用 (日付ピッカーなど)
                    if "INT" in col_type.upper() or "SERIAL" in col_type.upper():
                        insert_data[col_name] = st.number_input(
                            f"{col_name} ({col_type})",
                            value=None,
                            step=1,
                            key=f"insert_{col_name}",
                        )
                    elif "BOOL" in col_type.upper():
                        insert_data[col_name] = st.selectbox(
                            f"{col_name} ({col_type})",
                            options=[None, True, False],
                            index=0,
                            key=f"insert_{col_name}",
                        )
                    elif "DATE" in col_type.upper() or "TIMESTAMP" in col_type.upper():
                        # Streamlitの日付/時刻入力は naive なオブジェクトを返すことがあるので、DBのタイムゾーン設定と合わせる注意が必要
                        # ここでは簡略化のためテキスト入力として扱うか、st.date_input/st.time_input を使う
                        # value=None が st.date_input/st.time_input でエラーになることがあるので注意
                        user_val = st.text_input(
                            f"{col_name} ({col_type}) - 例: YYYY-MM-DD HH:MM:SS",
                            key=f"insert_{col_name}",
                        )
                        insert_data[col_name] = (
                            user_val if user_val else None
                        )  # 空ならNone
                    else:  # VARCHAR, TEXTなど
                        insert_data[col_name] = st.text_input(
                            f"{col_name} ({col_type})", key=f"insert_{col_name}"
                        )

                submitted = st.form_submit_button("INSERT文生成と実行")

            if submitted:
                # Noneや空文字のデータをフィルタリング（DB側でNOT NULL制約などがある場合を考慮）
                # 実際の挙動はDBスキーマに依存。ここでは全てのカラムを対象とする。
                # ユーザーが意図的にNULLを入力したい場合もあるので、ここではNoneを許容する。
                # ただし、空文字がDBでどのように扱われるかはDB製品による。
                # 例えばPostgreSQLでは空文字は空文字として扱われる。
                final_insert_data = {
                    k: v for k, v in insert_data.items()
                }  # valueがNoneの場合もそのまま渡す

                if not any(
                    v is not None and v != "" for v in final_insert_data.values()
                ):  # 何も入力されていないかチェック
                    st.warning("挿入するデータが入力されていません。")
                else:
                    # 生成されたINSERT文の表示 (確認用)
                    generated_sql, params = generate_insert_statement(
                        source_table_for_insert, final_insert_data
                    )
                    if generated_sql:
                        st.subheader("生成されたINSERT文 (確認用)")
                        st.code(generated_sql, language="sql")
                        st.write("パラメータ:")
                        st.json(params)

                        # 実行確認
                        if st.button(
                            "このINSERT文を実行する", key="confirm_execute_insert"
                        ):
                            with st.spinner(
                                f"テーブル '{source_table_for_insert}' にレコードを挿入中..."
                            ):
                                success, message = insert_record(
                                    st.session_state.source_engine,
                                    source_table_for_insert,
                                    final_insert_data,  # パラメータ化されたクエリのためSQLインジェクションに強い
                                )
                            if success:
                                st.success(message)
                            else:
                                st.error(message)
                    else:
                        st.error(
                            f"INSERT文の生成に失敗しました: {params}"
                        )  # paramsはエラーメッセージのはず


# Streamlitアプリの実行コマンド: streamlit run app.py
if __name__ == "__main__":
    pass
