import streamlit as st
from db_utils import (
    test_postgres_connection,  # PostgreSQL接続テストに使用
    get_db_engine,             # DBエンジン取得に使用
    get_table_names,           # 接続成功時にテーブル名一覧を取得するために使用
    save_connection_info,      # 接続情報の保存
    get_connection_names,      # 保存された接続名一覧の取得
    load_connection_info,      # 保存された接続情報の読み込み
    update_connection_info,    # 接続情報の更新
    delete_connection_info     # 接続情報の削除
)

def render_db_connection_form(conn_key_prefix: str):
    """
    指定された接続キープレフィックス (例: "source", "target") に基づいて、
    データベース接続用のフォームUIコンポーネントを描画します。
    ユーザーが入力した接続情報は st.session_state に保存されます。
    接続テストボタンが押されると、実際にDBへの接続を試み、結果を表示します。
    接続に成功した場合、テーブル情報も取得してセッション状態を更新します。

    Args:
        conn_key_prefix (str): "source" または "target"。セッション状態のキープレフィックスとして使用。
    """
    st.subheader(f"{conn_key_prefix.capitalize()} データベース設定") # "Source" または "Target"

    # セッション状態から対応するエンジンと接続パラメータのキーを生成
    db_engine_key = f"{conn_key_prefix}_engine" # 例: "source_engine"
    db_params_key = f"{conn_key_prefix}_postgres_conn_params"  # 現状はPostgreSQLのみ対応

    # 現在選択されているDBタイプ (st.session_state.db_type) に基づいてUIを構築
    if st.session_state.db_type == "postgresql":
        # state.pyで初期化された接続パラメータを取得。キーが存在しない場合のデフォルト値も設定。
        # これにより、state.pyでの初期化が不完全でもエラーになりにくいが、基本的にはstate.pyで初期化されている前提。
        pg_params = st.session_state.get(db_params_key, {
            "host": "localhost", "port": "5432",
            "db_name": f"{conn_key_prefix}_db", "user": "user", "password": "password"
        })

        # --- PostgreSQL接続フォーム ---
        pg_params["host"] = st.text_input(
            f"ホスト [{conn_key_prefix}]",
            value=pg_params.get("host", "localhost"), # .get()でキーが存在しなくてもエラーにならないようにする
            key=f"conn_ui_{conn_key_prefix}_pg_host", # 他のUI部品とキーが衝突しないようにプレフィックスを追加
        )
        pg_params["port"] = st.text_input(
            f"ポート [{conn_key_prefix}]",
            value=pg_params.get("port", "5432"),
            key=f"conn_ui_{conn_key_prefix}_pg_port",
        )
        pg_params["db_name"] = st.text_input(
            f"データベース名 [{conn_key_prefix}]",
            value=pg_params.get("db_name", f"{conn_key_prefix}_db"), # デフォルトDB名を設定
            key=f"conn_ui_{conn_key_prefix}_pg_dbname",
        )
        pg_params["user"] = st.text_input(
            f"ユーザー名 [{conn_key_prefix}]",
            value=pg_params.get("user", "user"),
            key=f"conn_ui_{conn_key_prefix}_pg_user",
        )
        pg_params["password"] = st.text_input(
            f"パスワード [{conn_key_prefix}]",
            type="password", # パスワード入力フィールド
            value=pg_params.get("password", "password"),
            key=f"conn_ui_{conn_key_prefix}_pg_password",
        )

        # ユーザーが入力した最新の接続パラメータをセッション状態に保存
        st.session_state[db_params_key] = pg_params

        # 「接続テスト/接続」ボタン
        if st.button(
            f"接続テスト/接続 ({conn_key_prefix.capitalize()} - PostgreSQL)",
            key=f"conn_ui_{conn_key_prefix}_pg_connect_button", # ユニークなキー
        ):
            try:
                # db_utilsのget_db_engineを使用してエンジンを取得 (内部で接続テストも実行)
                engine = get_db_engine("postgresql", pg_params)
                if engine:
                    st.session_state[db_engine_key] = engine # 成功したらエンジンをセッション状態に保存
                    st.success(f"PostgreSQL ({conn_key_prefix.capitalize()}) への接続に成功しました。")

                    # 接続成功後、テーブル名一覧を取得してセッション状態を更新
                    try:
                        table_names = get_table_names(engine)
                        if conn_key_prefix == "source":
                            st.session_state.source_tables = table_names
                            st.session_state.source_selected_table = None # テーブル選択をリセット
                            st.session_state.source_columns = []         # カラム情報もリセット
                        elif conn_key_prefix == "target":
                            st.session_state.target_tables = table_names
                            st.session_state.target_selected_table = None # テーブル選択をリセット
                            st.session_state.target_columns = []         # カラム情報もリセット
                        # この下に st.rerun() を追加
                        st.rerun() # ★ 追記
                    except RuntimeError as e_tables: # get_table_namesでエラーが発生した場合
                        st.error(f"テーブル一覧の取得に失敗しました: {e_tables}")
                        # テーブルリストを空にする
                        if conn_key_prefix == "source": st.session_state.source_tables = []
                        elif conn_key_prefix == "target": st.session_state.target_tables = []
                        # エラー発生時も再描画するならここにも st.rerun() を追加検討
                        st.rerun() # ★ 追記 (エラー時も状態をUIに反映するため)
                else:
                    # get_db_engine が None を返した場合 (通常は例外が発生するが念のため)
                    st.session_state[db_engine_key] = None
                    st.error(f"PostgreSQL ({conn_key_prefix.capitalize()}) への接続に失敗しました。")
            except Exception as e_connect: # get_db_engine で例外が発生した場合
                st.session_state[db_engine_key] = None
                st.error(f"PostgreSQL ({conn_key_prefix.capitalize()}) 接続エラー: {e_connect}")

        # --- 接続情報の保存 ---
        # st.session_state.metadata_engine は app.py のメイン処理で初期化される想定
        metadata_engine = st.session_state.get("metadata_engine")
        if metadata_engine:
            save_name = st.text_input(
                "接続設定名として保存 (空の場合は保存されません)",
                key=f"conn_ui_{conn_key_prefix}_save_as_name",
                placeholder="例: my_dev_db"
            )
            if st.button(
                f"現在の接続情報を「{save_name}」として保存",
                key=f"conn_ui_{conn_key_prefix}_save_button",
                disabled=not save_name # 保存名が空ならボタンを無効化
            ):
                if save_name: # ボタンが押された時点で再度確認 (disabledでも押せてしまう場合への対策)
                    # 現在のフォームの接続パラメータを取得
                    current_params = st.session_state.get(db_params_key, {})
                    success, message = save_connection_info(
                        metadata_engine,
                        save_name,
                        st.session_state.db_type, # 現在選択中のDBタイプ
                        current_params
                    )
                    if success:
                        st.success(message)
                    else:
                        st.error(message)
                else:
                    st.warning("保存するには接続設定名を入力してください。")
        else:
            st.info("メタデータDBが初期化されていないため、接続情報の保存機能は利用できません。")

    # --- 他のデータベースタイプ (SQLiteなど) の接続フォームはここに elif で追加 ---
    # elif st.session_state.db_type == "sqlite":
    #    # SQLite用の接続フォームUIをここに記述
    #    pass

    # --- 接続状態の表示 ---
    current_engine = st.session_state.get(db_engine_key) # .get()で安全にエンジンを取得
    if current_engine:
        st.success(f"{conn_key_prefix.capitalize()} DBに接続済み (URL: {current_engine.url})")
    else:
        st.warning(f"{conn_key_prefix.capitalize()} DBに接続されていません。")


def render_connection_tabs():
    """
    「接続1 (ソース/ターゲット)」と「接続2 (ターゲットDB - 任意)」のタブを作成し、
    各タブ内にデータベース接続フォーム (render_db_connection_form) を描画します。
    """
    st.header("接続先データベース") # メインエリアのヘッダー

    # タブの作成
    conn_tab1, conn_tab2 = st.tabs(
        ["接続1 (ソース/ターゲット)", "接続2 (ターゲットDB - 任意)"]
    )

    # 「接続1」タブの内容
    with conn_tab1:
        st.write("データ移行のソースDB、または単一DB操作時のターゲットDBとして使用します。")

        st.subheader("保存済み接続の利用")
        metadata_engine = st.session_state.get("metadata_engine")
        if metadata_engine:
            conn_names = get_connection_names(metadata_engine)
            if conn_names:
                selected_conn_name = st.selectbox(
                    "保存済み接続を選択:",
                    options=[""] + conn_names, # 先頭に空の選択肢
                    key="conn_ui_source_load_connection_select"
                )
                if st.button("選択した情報をフォームに復元 (Source)", key="conn_ui_source_load_button", disabled=not selected_conn_name):
                    loaded_info = load_connection_info(metadata_engine, selected_conn_name)
                    if loaded_info:
                        # 読み込んだ情報でフォームの値を更新
                        # 現状はPostgreSQLのみを想定
                        if loaded_info["db_type"] == "postgresql":
                            st.session_state.source_postgres_conn_params = {
                                "host": loaded_info.get("host", ""),
                                "port": loaded_info.get("port", ""),
                                "db_name": loaded_info.get("db_name", ""),
                                "user": loaded_info.get("user", ""),
                                "password": loaded_info.get("password", "")
                            }
                            st.success(f"接続情報「{selected_conn_name}」をフォームに復元しました。")
                            st.rerun() # フォームの表示を更新
                        else:
                            st.warning(f"このUIは現在PostgreSQL接続のみをサポートしています。選択された接続タイプ: {loaded_info['db_type']}")
                    else:
                        st.error(f"接続情報「{selected_conn_name}」の読み込みに失敗しました。")
            else:
                st.info("保存されている接続情報はありません。")
        else:
            st.info("メタデータDBが初期化されていないため、保存済み接続の利用機能は利用できません。") # メッセージを修正

        st.markdown("---") # 区切り線

        # state.pyで `source_postgres_conn_params` が初期化されているため、
        # ここでの追加の初期化処理は不要。
        render_db_connection_form("source") # "source" プレフィックスで接続フォームを描画


    # 「接続2」タブの内容
    with conn_tab2:
        st.write("データ移行のターゲットDBとして使用します。接続1と同じDBを指定することも可能です。")

        st.subheader("保存済み接続の利用")
        metadata_engine = st.session_state.get("metadata_engine")
        if metadata_engine:
            conn_names = get_connection_names(metadata_engine)
            if conn_names:
                selected_conn_name_target = st.selectbox(
                    "保存済み接続を選択:",
                    options=[""] + conn_names, # 先頭に空の選択肢
                    key="conn_ui_target_load_connection_select"
                )
                if st.button("選択した情報をフォームに復元 (Target)", key="conn_ui_target_load_button", disabled=not selected_conn_name_target):
                    loaded_info_target = load_connection_info(metadata_engine, selected_conn_name_target)
                    if loaded_info_target:
                        if loaded_info_target["db_type"] == "postgresql":
                            st.session_state.target_postgres_conn_params = {
                                "host": loaded_info_target.get("host", ""),
                                "port": loaded_info_target.get("port", ""),
                                "db_name": loaded_info_target.get("db_name", ""),
                                "user": loaded_info_target.get("user", ""),
                                "password": loaded_info_target.get("password", "")
                            }
                            st.success(f"接続情報「{selected_conn_name_target}」をフォームに復元しました。")
                            st.rerun()
                        else:
                            st.warning(f"このUIは現在PostgreSQL接続のみをサポートしています。選択された接続タイプ: {loaded_info_target['db_type']}")
                    else:
                        st.error(f"接続情報「{selected_conn_name_target}」の読み込みに失敗しました。")
            else:
                st.info("保存されている接続情報はありません。")
        else:
            st.info("メタデータDBが初期化されていないため、保存済み接続の利用機能は利用できません。") # メッセージを修正

        st.markdown("---") # 区切り線

        # state.pyで `target_postgres_conn_params` が初期化されているため、
        # ここでの追加の初期化処理は不要。
        render_db_connection_form("target") # "target" プレフィックスで接続フォームを描画

    # 開発者向けメモ:
    # 以前のapp.pyにあった render_db_connection_form の古いバージョンや重複した定義は、
    # このモジュールへの移行と state.py でのセッション状態の一元管理によって整理されました。
    pass
