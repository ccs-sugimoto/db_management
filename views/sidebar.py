import streamlit as st
from db_utils import (
    get_db_engine,
    create_metadata_tables_if_not_exists,
    get_mapping_config_names,
    load_connection_info,      # 追加
    update_connection_info,    # 追加
    delete_connection_info,     # 追加
    get_connection_names       # 追加
)

def render_sidebar():
    """
    Streamlitアプリケーションのサイドバーを描画します。
    データベースタイプ選択、メタデータDBへの接続設定と接続状態表示、
    そして保存済み接続の管理機能を提供します。
    """
    st.sidebar.header("接続設定")

    # --- データベースタイプ選択 ---
    db_type_options = ["postgresql", "他のDB (未実装)"]
    current_db_type_index = 0
    if "db_type" in st.session_state and st.session_state.db_type in db_type_options:
        current_db_type_index = db_type_options.index(st.session_state.db_type)

    st.session_state.db_type = st.sidebar.selectbox(
        "データベースタイプ",
        db_type_options,
        index=current_db_type_index,
        key="sidebar_db_type_selectbox"
    )

    # --- メタデータ保存用SQLite DBの接続 ---
    st.sidebar.subheader("メタデータDB (SQLite)")
    sqlite_path_input = st.session_state.sqlite_conn_params.get("db_path", "metadata.db")

    sqlite_path = st.sidebar.text_input(
        "SQLite データベースファイルパス",
        value=sqlite_path_input,
        key="sidebar_metadata_sqlite_path",
    )
    st.session_state.sqlite_conn_params["db_path"] = sqlite_path

    if st.sidebar.button("メタデータDB接続テスト/接続", key="sidebar_connect_metadata_db_button"):
        if sqlite_path:
            try:
                engine = get_db_engine("sqlite", {"db_path": sqlite_path})
                if engine:
                    st.session_state.metadata_engine = engine
                    create_metadata_tables_if_not_exists(st.session_state.metadata_engine)
                    st.sidebar.success(f"メタデータDB ({sqlite_path}) への接続とテーブル準備が完了しました。")
                    st.session_state.saved_mappings = get_mapping_config_names(st.session_state.metadata_engine)
                else:
                    st.sidebar.error(f"メタデータDB ({sqlite_path}) への接続に失敗しました。")
                    st.session_state.metadata_engine = None
            except Exception as e:
                st.sidebar.error(f"メタデータDB処理エラー: {e}")
                st.session_state.metadata_engine = None
        else:
            st.sidebar.warning("SQLiteデータベースのパスを入力してください。")

    # --- メタデータDBの接続状態表示 ---
    if st.session_state.get("metadata_engine"):
        display_path = st.session_state.sqlite_conn_params.get("db_path", "N/A")
        st.sidebar.success(f"メタデータDB ({display_path}) に接続済み")
    else:
        st.sidebar.warning("メタデータDBに接続してください。")

    # --- 保存済み接続の管理 ---
    st.sidebar.subheader("保存済み接続の管理")
    metadata_engine = st.session_state.get("metadata_engine")
    if metadata_engine:
        conn_names = get_connection_names(metadata_engine)
        if conn_names:
            manage_selected_conn_name_key = "sidebar_conn_manage_selected_name"

            if manage_selected_conn_name_key not in st.session_state:
                st.session_state[manage_selected_conn_name_key] = ""

            st.session_state[manage_selected_conn_name_key] = st.sidebar.selectbox(
                "管理する接続を選択:",
                options=[""] + conn_names,
                key="sidebar_conn_manage_selectbox",
                index=([""] + conn_names).index(st.session_state[manage_selected_conn_name_key]) if st.session_state[manage_selected_conn_name_key] in ([""] + conn_names) else 0
            )
            selected_manage_conn_name = st.session_state[manage_selected_conn_name_key]

            if selected_manage_conn_name:
                loaded_manage_info = load_connection_info(metadata_engine, selected_manage_conn_name)
                if loaded_manage_info and loaded_manage_info["db_type"] == "postgresql": # 現状PostgreSQLのみ対応
                    st.sidebar.write(f"「{selected_manage_conn_name}」を編集中...")

                    edit_params = {}
                    edit_params["new_name"] = st.sidebar.text_input(
                        "新しい接続名",
                        value=loaded_manage_info.get("name", ""),
                        key="sidebar_conn_manage_edit_name"
                    )
                    edit_params["host"] = st.sidebar.text_input(
                        "ホスト (編集)", value=loaded_manage_info.get("host", ""),
                        key="sidebar_conn_manage_edit_host"
                    )
                    edit_params["port"] = st.sidebar.text_input(
                        "ポート (編集)", value=loaded_manage_info.get("port", ""),
                        key="sidebar_conn_manage_edit_port"
                    )
                    edit_params["db_name"] = st.sidebar.text_input(
                        "データベース名 (編集)", value=loaded_manage_info.get("db_name", ""),
                        key="sidebar_conn_manage_edit_dbname"
                    )
                    edit_params["user"] = st.sidebar.text_input(
                        "ユーザー名 (編集)", value=loaded_manage_info.get("user", ""),
                        key="sidebar_conn_manage_edit_user"
                    )
                    edit_params["password"] = st.sidebar.text_input(
                        "パスワード (編集)", type="password", value=loaded_manage_info.get("password", ""),
                        key="sidebar_conn_manage_edit_password"
                    )
                    edit_params["schema_name"] = st.sidebar.text_input(
                        "スキーマ名 (編集)", value=loaded_manage_info.get("schema_name", "public"),
                        key="sidebar_conn_manage_edit_schema_name"
                    )

                    if st.sidebar.button("変更を保存", key="sidebar_conn_manage_update_button"):
                        update_success, update_msg = update_connection_info(
                            metadata_engine,
                            original_name=selected_manage_conn_name,
                            new_name=edit_params["new_name"],
                            db_type="postgresql",
                            params={
                                "host": edit_params["host"], "port": edit_params["port"],
                                "db_name": edit_params["db_name"], "user": edit_params["user"],
                                "password": edit_params["password"],
                                "schema_name": edit_params["schema_name"],
                            }
                        )
                        if update_success:
                            st.sidebar.success(update_msg)
                            st.session_state[manage_selected_conn_name_key] = edit_params["new_name"] if edit_params["new_name"] in get_connection_names(metadata_engine) else ""
                            st.rerun()
                        else:
                            st.sidebar.error(update_msg)

                    st.sidebar.markdown("---")
                    show_confirm_delete_key = "sidebar_conn_manage_show_confirm_delete"
                    if show_confirm_delete_key not in st.session_state:
                        st.session_state[show_confirm_delete_key] = False

                    if st.sidebar.button(f"「{selected_manage_conn_name}」を削除", key="sidebar_conn_manage_delete_button"):
                        st.session_state[show_confirm_delete_key] = True

                    if st.session_state[show_confirm_delete_key]:
                        st.sidebar.warning(f"本当に「{selected_manage_conn_name}」を削除しますか？この操作は取り消せません。")
                        col1, col2 = st.sidebar.columns(2)
                        with col1:
                            if st.button("はい、削除します", type="primary", key="sidebar_conn_manage_confirm_delete_button"):
                                delete_success, delete_msg = delete_connection_info(metadata_engine, selected_manage_conn_name)
                                if delete_success:
                                    st.sidebar.success(delete_msg)
                                    st.session_state[manage_selected_conn_name_key] = ""
                                    st.session_state[show_confirm_delete_key] = False
                                    st.rerun()
                                else:
                                    st.sidebar.error(delete_msg)
                                    st.session_state[show_confirm_delete_key] = False
                        with col2:
                            if st.button("キャンセル", key="sidebar_conn_manage_cancel_delete_button"):
                                st.session_state[show_confirm_delete_key] = False
                                st.rerun()

                elif loaded_manage_info and loaded_manage_info["db_type"] != "postgresql":
                    st.sidebar.warning(f"このUIは現在PostgreSQL接続の管理のみをサポートしています。選択された接続タイプ: {loaded_manage_info['db_type']}")
                elif not selected_manage_conn_name:
                    pass
                else:
                    st.sidebar.error(f"接続情報「{selected_manage_conn_name}」の読み込みに失敗したか、存在しません。")
        else:
             st.sidebar.info("管理対象の保存済み接続情報はありません。")
    else:
        st.sidebar.info("メタデータDBに接続すると、保存済み接続の管理機能が利用できます。")

    pass
