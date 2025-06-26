import streamlit as st
import pandas as pd # st.dataframe を使用するためにインポート
from db_utils import get_table_names, get_table_columns # DB操作ユーティリティ関数

def display_db_info(engine, tables_key: str, selected_table_key: str, columns_key: str, db_label: str):
    """
    指定されたデータベース接続の情報を表示します。
    テーブル一覧の表示、テーブル選択、選択されたテーブルのカラム情報を表示します。

    Args:
        engine (sqlalchemy.engine.Engine): 表示対象のデータベースエンジン。
        tables_key (str): セッション状態にテーブル名リストを保存するためのキー。
        selected_table_key (str): セッション状態に選択されたテーブル名を保存するためのキー。
        columns_key (str): セッション状態にカラム情報を保存するためのキー。
        db_label (str): UIに表示するデータベースのラベル (例: "接続1 (ソース)")。
    """
    if engine: # エンジンがNoneでない（接続が確立されている）場合のみ処理
        st.subheader(f"{db_label} 情報") # 例: "接続1 (ソース) 情報"

        # セッション状態からテーブルリストを取得、なければ空リスト
        tables = st.session_state.get(tables_key, [])

        # テーブルリストが空で、かつエンジンが存在する場合、テーブル情報を再取得試行
        # (例: 初回表示時や、接続後にまだテーブル情報を読み込んでいない場合)
        if not tables and engine:
            try:
                tables = get_table_names(engine)
                st.session_state[tables_key] = tables # 取得したテーブルリストをセッション状態に保存
            except RuntimeError as e:
                st.error(f"{db_label} のテーブル一覧取得に失敗: {e}")
                st.session_state[tables_key] = []  # エラー時は空リストを設定
                tables = [] # ローカル変数も更新

        if tables: # 表示するテーブルがある場合
            # 現在選択されているテーブルをセッション状態から取得
            current_selected_table = st.session_state.get(selected_table_key)

            # selectboxのデフォルト選択インデックスを計算
            select_idx = 0 # デフォルトは未選択 (optionsの先頭 "")
            if current_selected_table and current_selected_table in tables:
                try:
                    select_idx = tables.index(current_selected_table) + 1 # +1 は先頭の未選択オプション分
                except ValueError:
                    # tablesリストにcurrent_selected_tableが存在しない場合(キャッシュ等で古い値が残っているなど)
                    st.session_state[selected_table_key] = None # 選択をリセット

            # テーブル選択のselectbox
            selected_table = st.selectbox(
                f"{db_label} テーブル一覧",
                options=[""] + tables,  # 先頭に空の選択肢（未選択状態）を追加
                index=select_idx,
                key=f"db_info_ui_{db_label}_table_select", # ユニークキー
            )
            # 選択されたテーブル名をセッション状態に保存
            st.session_state[selected_table_key] = selected_table if selected_table else None

            # テーブルが選択されている場合、そのカラム情報を表示
            if st.session_state.get(selected_table_key):
                try:
                    # 選択されたテーブルのカラム情報を取得
                    columns = get_table_columns(engine, st.session_state.get(selected_table_key))
                    st.session_state[columns_key] = columns # カラム情報をセッション状態に保存

                    if columns:
                        st.write(f"テーブル '{st.session_state.get(selected_table_key)}' のカラム:")
                        st.dataframe(columns, use_container_width=True) # カラム情報をデータフレームで表示
                    else:
                        st.info(f"テーブル '{st.session_state.get(selected_table_key)}' にカラムが見つかりません。")
                except RuntimeError as e:
                    st.error(f"{db_label} のテーブル '{st.session_state.get(selected_table_key)}' のカラム情報取得に失敗: {e}")
                    st.session_state[columns_key] = [] # エラー時はカラム情報を空にする
            else:
                # テーブルが選択されていない場合はカラム情報をクリア
                st.session_state[columns_key] = []
        else: # 表示するテーブルがない場合
            if engine: # エンジンはあるがテーブルが0件の場合
                st.info(f"{db_label} にテーブルが見つかりません。")
            # エンジン自体がない場合は、この関数の冒頭の if engine: でブロックされているはず
    else: # エンジンがNoneの場合 (未接続)
        st.warning(f"{db_label} に接続されていません。")


def render_database_info_columns():
    """
    データベース情報表示のためのUIコンポーネント（2カラムレイアウト）を描画します。
    左カラムに接続1(ソース)、右カラムに接続2(ターゲット)の情報を表示します。
    """
    st.header("データベース情報") # セクションヘッダー

    # 2カラムレイアウトを作成
    info_col1, info_col2 = st.columns(2)

    # 左カラム: 接続1 (ソース) のデータベース情報
    with info_col1:
        display_db_info(
            st.session_state.get("source_engine"), # ソースDBのエンジン (存在すれば)
            "source_tables",                      # ソーステーブルリストのセッションキー
            "source_selected_table",              # ソースで選択中テーブルのセッションキー
            "source_columns",                     # ソーステーブルカラム情報のセッションキー
            "接続1 (ソース)",                       # UI表示用ラベル
        )

    # 右カラム: 接続2 (ターゲット) のデータベース情報
    with info_col2:
        if st.session_state.get("target_engine"):  # ターゲットDBエンジンが存在する場合のみ表示
            display_db_info(
                st.session_state.get("target_engine"), # ターゲットDBのエンジン
                "target_tables",                       # ターゲットテーブルリストのセッションキー
                "target_selected_table",               # ターゲットで選択中テーブルのセッションキー
                "target_columns",                      # ターゲットテーブルカラム情報のセッションキー
                "接続2 (ターゲット)",                    # UI表示用ラベル
            )
        else:
            st.info("接続2 (ターゲットDB) は接続されていません。") # 未接続時のメッセージ

    # 開発者向けメモ:
    # このモジュールは、app.pyからデータベース情報の表示ロジックを分離したものです。
    # display_db_info関数が実際の情報表示を行い、render_database_info_columns関数が
    # そのレイアウトを定義しています。
    pass
