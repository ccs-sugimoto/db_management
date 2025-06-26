import streamlit as st
from db_utils import (
    # test_sqlite_connection, # get_db_engine内で接続テストが行われるため、直接の使用は不要になった
    get_db_engine,
    create_metadata_tables_if_not_exists, # メタデータDB接続時にテーブルを作成・確認するために使用
    get_mapping_config_names # メタデータDB接続時に保存済みマッピング一覧を読み込むために使用
)

def render_sidebar():
    """
    Streamlitアプリケーションのサイドバーを描画します。
    データベースタイプ選択、メタデータDBへの接続設定と接続状態表示を行います。
    """
    st.sidebar.header("接続設定") # サイドバーのヘッダー

    # --- データベースタイプ選択 ---
    db_type_options = ["postgresql", "他のDB (未実装)"]  # 将来的な拡張を見据えた選択肢
    # st.session_state.db_type は state.py で初期化されている想定
    # db_type_options.index() は、st.session_state.db_type が options 内に存在しない場合にエラーとなるため、
    # 安全のために存在確認とフォールバックを行う。
    current_db_type_index = 0 # デフォルトは先頭の選択肢
    if "db_type" in st.session_state and st.session_state.db_type in db_type_options:
        current_db_type_index = db_type_options.index(st.session_state.db_type)

    st.session_state.db_type = st.sidebar.selectbox(
        "データベースタイプ",
        db_type_options,
        index=current_db_type_index,
        key="sidebar_db_type_selectbox" # 他のselectboxとのキー競合を避けるためのユニークなキー
    )

    # --- メタデータ保存用SQLite DBの接続 ---
    st.sidebar.subheader("メタデータDB (SQLite)")
    # st.session_state.sqlite_conn_params は state.py で初期化されている想定
    sqlite_path_input = st.session_state.sqlite_conn_params.get("db_path", "metadata.db")

    sqlite_path = st.sidebar.text_input(
        "SQLite データベースファイルパス",
        value=sqlite_path_input,
        key="sidebar_metadata_sqlite_path", # ユニークなキー
    )
    # 入力されたパスをセッション状態に保存 (リアルタイム更新)
    st.session_state.sqlite_conn_params["db_path"] = sqlite_path

    # 「メタデータDB接続テスト/接続」ボタン
    if st.sidebar.button("メタデータDB接続テスト/接続", key="sidebar_connect_metadata_db_button"):
        if sqlite_path: # パスが入力されている場合のみ処理
            try:
                # get_db_engine 関数でSQLiteエンジンを取得 (内部で接続テストも行われる)
                engine = get_db_engine("sqlite", {"db_path": sqlite_path})
                if engine:
                    st.session_state.metadata_engine = engine # エンジンをセッション状態に保存
                    # メタデータテーブルが存在しない場合は作成
                    create_metadata_tables_if_not_exists(st.session_state.metadata_engine)
                    st.sidebar.success(f"メタデータDB ({sqlite_path}) への接続とテーブル準備が完了しました。")
                    # 保存済みのカラムマッピング設定名一覧を読み込み、セッション状態に保存
                    st.session_state.saved_mappings = get_mapping_config_names(st.session_state.metadata_engine)
                else:
                    # get_db_engine が None を返した場合 (通常は例外が発生するはずだが念のため)
                    st.sidebar.error(f"メタデータDB ({sqlite_path}) への接続に失敗しました。")
                    st.session_state.metadata_engine = None
            except Exception as e: # get_db_engine やその他の処理で例外が発生した場合
                st.sidebar.error(f"メタデータDB処理エラー: {e}")
                st.session_state.metadata_engine = None
        else:
            st.sidebar.warning("SQLiteデータベースのパスを入力してください。")

    # --- メタデータDBの接続状態表示 ---
    # st.session_state.metadata_engine が存在し、接続済みかどうかに基づいてメッセージを表示
    if st.session_state.get("metadata_engine"):
        # 表示するパスはセッション状態から取得 (ユーザーが入力フィールドを変更した場合も追従するため)
        display_path = st.session_state.sqlite_conn_params.get("db_path", "N/A")
        st.sidebar.success(f"メタデータDB ({display_path}) に接続済み")
    else:
        st.sidebar.warning("メタデータDBに接続してください。")

    # 以前の app.py にあった重複する、あるいはここに統合されたコメントアウトされたコードは削除済み。
    # この render_sidebar 関数でサイドバーに関するすべてのUIとロジックが集約されている。
    pass
