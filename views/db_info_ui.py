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
    schema_name_key = f"{tables_key}_schema_name"
    if engine: # エンジンがNoneでない（接続が確立されている）場合のみ処理
        st.subheader(f"{db_label} 情報") # 例: "接続1 (ソース) 情報"

        # セッション状態からテーブル情報リスト (辞書のリスト) を取得、なければ空リスト
        tables_info_list = st.session_state.get(tables_key, [])

        # スキーマ名を決定するロジック
        schema_to_use = "public" # デフォルト値
        # PostgreSQLの場合のみ、セッション状態からスキーマ名を取得試行
        if engine.dialect.name == "postgresql":
            if db_label == "接続1 (ソース)":
                schema_to_use = st.session_state.get("source_postgres_conn_params", {}).get("schema_name", "public")
            elif db_label == "接続2 (ターゲット)":
                schema_to_use = st.session_state.get("target_postgres_conn_params", {}).get("schema_name", "public")

        # スキーマ名が空文字やNoneの場合は 'public' にフォールバック
        if not schema_to_use:
            schema_to_use = "public"
        # スキーマ名をセッションに保存
        st.session_state[schema_name_key] = schema_to_use

        # テーブル情報リストが空で、かつエンジンが存在する場合、テーブル情報を再取得試行
        if not tables_info_list and engine:
            try:
                tables_info_list = get_table_names(engine, schema_name=schema_to_use) # 動的に取得したスキーマ名を使用
                st.session_state[tables_key] = tables_info_list # 取得したリストをセッション状態に保存
            except RuntimeError as e:
                st.error(f"{db_label} のテーブル一覧取得に失敗: {e}")
                st.session_state[tables_key] = []  # エラー時は空リストを設定
                tables_info_list = [] # ローカル変数も更新

        if tables_info_list: # 表示するテーブル情報がある場合
            # 物理名のリストを作成 (selectboxのoptions用)
            physical_table_names = [table_info["name"] for table_info in tables_info_list]

            # format_func を定義
            def format_table_name(physical_name):
                if not physical_name: # 空の選択肢の場合
                    return "選択してください"
                # tables_info_list から該当するテーブル情報を検索
                table_info = next((t for t in tables_info_list if t["name"] == physical_name), None)
                if table_info:
                    comment = table_info.get("comment")
                    return f"{physical_name} ({comment})" if comment else physical_name
                return physical_name # 見つからなければ物理名のみ

            # 現在選択されている物理テーブル名をセッション状態から取得
            current_selected_physical_table = st.session_state.get(selected_table_key)

            # selectboxのデフォルト選択インデックスを計算
            select_idx = 0 # デフォルトは未選択 (optionsの先頭 "")
            if current_selected_physical_table and current_selected_physical_table in physical_table_names:
                try:
                    select_idx = physical_table_names.index(current_selected_physical_table) + 1 # +1 は先頭の未選択オプション分
                except ValueError:
                    st.session_state[selected_table_key] = None # 選択をリセット

            # テーブル選択のselectbox
            selected_physical_table = st.selectbox(
                f"{db_label} テーブル一覧",
                options=[""] + physical_table_names,  # 先頭に空の選択肢を追加
                index=select_idx,
                format_func=format_table_name, # 表示名整形関数
                key=f"db_info_ui_{db_label}_table_select", # ユニークキー
            )
            # 選択された物理テーブル名をセッション状態に保存
            st.session_state[selected_table_key] = selected_physical_table if selected_physical_table else None

            # 物理テーブル名が選択されている場合、そのカラム情報を表示
            if st.session_state.get(selected_table_key):
                selected_physical_table_name = st.session_state.get(selected_table_key) # 物理名を取得

                # 物理名をコピーする機能
                st.markdown("##### 選択中テーブルの物理名") # 小見出し
                copy_col1, copy_col2 = st.columns([0.3, 0.7]) # ボタンと表示エリアの列分け
                with copy_col1:
                    if st.button("📋 コピー用表示", key=f"copy_table_name_button_{db_label}"):
                        # ボタンが押されたら、コピー用の物理名をセッションに保存 (または直接表示)
                        # ここでは押されたことのフラグを立てる代わりに、直接表示する
                        # (実際には st.code を使って表示するが、 Streamlit の制約でボタンコールバック内で即時表示更新が難しい場合があるため、
                        #  状態管理やst.rerun()を適切に使うか、以下のようにメッセージとst.codeで案内する)
                        st.session_state[f"show_copy_name_{db_label}"] = selected_physical_table_name

                with copy_col2:
                    # ボタンが押された場合に物理名を表示 (実際にはこの制御はもう少し工夫が必要かも)
                    # ボタンクリックで表示状態をトグルする例
                    if f"show_copy_name_{db_label}" in st.session_state and st.session_state[f"show_copy_name_{db_label}"] == selected_physical_table_name:
                        # st.info("以下の物理名をコピーしてください:") # 案内メッセージはボタンの文言で兼ねる
                        st.code(selected_physical_table_name, language=None) # language=Noneでプレーンテキストとして表示
                        # 表示後はフラグをリセットするか、ユーザーが閉じるまで表示し続けるか設計による
                        # ここでは一度表示したら残す（ボタン再押下で同じものが表示される）

                st.markdown("---") # 区切り線

                try:
                    # 選択されたテーブルのカラム情報を取得
                    columns = get_table_columns(engine, selected_physical_table_name, st.session_state.get(schema_name_key))
                    st.session_state[columns_key] = columns # カラム情報 (list[dict]) をセッション状態に保存

                    if columns:
                        st.write(f"テーブル '{selected_physical_table_name}' のカラム:")
                        # DataFrameに変換し、表示する列を指定・整形
                        df_columns = pd.DataFrame(columns)
                        # 表示用に'comment'がNoneの場合は空文字に置換
                        df_columns["comment"] = df_columns["comment"].fillna("")
                        # 表示するカラムの順序と名前を定義
                        display_df = df_columns[["name", "type", "comment"]]
                        display_df = display_df.rename(columns={"name": "物理名", "type": "型", "comment": "論理名"})
                        display_df.index = display_df.index + 1
                        st.dataframe(display_df, use_container_width=True)
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
