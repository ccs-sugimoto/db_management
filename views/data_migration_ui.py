import streamlit as st
import pandas as pd # st.json やデータ操作のUIで間接的に使用される可能性を考慮
from db_utils import (
    migrate_data,              # データ移行処理
    generate_insert_statement, # INSERT文生成処理
    insert_record              # 単一レコード挿入処理
)

def render_data_migration_ui():
    """
    データ移行および単一レコードINSERT機能のためのUIコンポーネントを描画します。
    前提条件（メタデータDB接続、ソースDB接続、ソーステーブル選択、カラムマッピング設定）を
    チェックし、満たされている場合に各操作UIを表示します。
    """
    st.header("データ操作") # セクションヘッダー

    # --- 前提条件のチェック ---
    # 機能を利用するための必須条件が満たされているかを確認し、不足していれば警告を表示
    if not st.session_state.get("metadata_engine"):
        st.warning("データ操作を行うには、まずサイドバーからメタデータDBに接続してください。")
        return # 条件未達のため、以降のUIは描画しない
    if not st.session_state.get("source_engine"):
        st.warning("データ操作を行うには、まず「接続1 (ソースDB)」に接続し、テーブルを選択してください。")
        return
    if not st.session_state.get("source_selected_table"):
        st.warning("データ操作を行うには、「接続1 (ソースDB)」でテーブルを選択してください。")
        return
    if not st.session_state.get("column_map"): # カラムマッピングが空または未設定でないか
        st.warning("データ操作を行うには、まずカラムマッピングを設定・適用してください。")
        return

    # --- データ移行UI ---
    st.subheader("データ移行")

    # データ移行実行の可否フラグ (ターゲットDB/テーブルの選択状態による)
    ready_for_migration = True
    if not st.session_state.get("target_engine"):
        st.warning("データ移行を行うには、「接続2 (ターゲットDB)」にも接続してください。")
        ready_for_migration = False
    if not st.session_state.get("target_selected_table"):
        st.warning("データ移行を行うには、「接続2 (ターゲットDB)」でターゲットテーブルも選択してください。")
        ready_for_migration = False

    # ターゲットDB/テーブルが選択されている場合のみ、マッピングされたカラムの存在チェックを行う
    if ready_for_migration:
        # 現在のカラムマッピングで指定されているターゲットカラム名を取得
        mapped_target_cols = list(st.session_state.column_map.values())
        # 実際に選択されているターゲットテーブルのカラム名リストを取得
        actual_target_cols = [col["name"] for col in st.session_state.get("target_columns", [])]

        # マッピングされているが存在しないターゲットカラムを検出
        missing_cols_in_target = [mtc for mtc in mapped_target_cols if mtc not in actual_target_cols]
        if missing_cols_in_target:
            st.warning(
                f"警告: マッピングされたターゲットカラム {missing_cols_in_target} が、選択されたターゲットテーブル '{st.session_state.target_selected_table}' のカラムに存在しません。移行前にカラムマッピング設定を確認してください。"
            )
            # ready_for_migration = False # 警告に留め、実行自体は可能にするか、ここでFalseにするか選択（現在は警告のみ）

    # 移行設定のサマリー表示
    st.markdown(f"""
    以下の設定でデータを移行します:
    - **ソースDB:** `{st.session_state.source_engine.url if st.session_state.get("source_engine") else "未接続"}`
    - **ソーステーブル:** `{st.session_state.get("source_selected_table", "未選択")}`
    - **ターゲットDB:** `{st.session_state.target_engine.url if st.session_state.get("target_engine") else "未接続"}`
    - **ターゲットテーブル:** `{st.session_state.get("target_selected_table", "未選択")}`
    - **適用中のカラムマッピング:**
    """)
    if st.session_state.get("column_map"):
        st.json(st.session_state.column_map) # カラムマッピングをJSON形式で表示
    else:
        st.write("なし")

    # データ移行時のチャンクサイズ入力
    chunk_size = st.number_input(
        "一度に処理する行数 (チャンクサイズ)",
        min_value=100, max_value=10000, value=1000, step=100,
        key="data_migration_ui_chunk_size", # ユニークキー
        help="データ移行時に一度に読み書きする行数を指定します。メモリ使用量に影響します。"
    )

    # 「データ移行実行」ボタン
    if st.button("データ移行実行", disabled=not ready_for_migration, type="primary", key="data_migration_ui_execute_button"):
        with st.spinner("データ移行を実行中..."): # 処理中にスピナーを表示
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

    st.markdown("---") # 区切り線

    # --- 単一レコードINSERT機能 ---
    st.subheader("単一レコードのINSERT")

    # 必要な情報 (ソーステーブル名、ソースカラム情報) をセッション状態から取得
    source_table_for_insert = st.session_state.source_selected_table
    source_columns_for_insert = st.session_state.get("source_columns", [])

    if not source_columns_for_insert: # ソースカラム情報がなければ警告
        st.warning(
            f"テーブル '{source_table_for_insert}' のカラム情報が読み込まれていません。まず「データベース情報」セクションでテーブルを選択し直してください。"
        )
    else:
        st.markdown(f"**テーブル `{source_table_for_insert}` にデータを挿入します。**")

        # レコード挿入用フォーム
        with st.form(key="data_migration_ui_insert_form"):
            insert_data = {} # ユーザーが入力するデータを格納する辞書
            # ソーステーブルの各カラムに対応する入力フィールドを生成
            for col_info in source_columns_for_insert:
                col_name = col_info["name"]
                col_type_str = str(col_info.get("type", "不明")) # 型情報があれば表示

                # カラムの型情報に基づいて適切な入力ウィジェットを選択 (簡易的な型判定)
                # TODO: より厳密な型判定とウィジェット選択、バリデーションの追加
                if "INT" in col_type_str.upper() or "SERIAL" in col_type_str.upper():
                    insert_data[col_name] = st.number_input(
                        f"{col_name} ({col_type_str})", value=None, step=1, key=f"insert_form_{col_name}"
                    )
                elif "BOOL" in col_type_str.upper():
                    insert_data[col_name] = st.selectbox(
                        f"{col_name} ({col_type_str})", options=[None, True, False], index=0, key=f"insert_form_{col_name}"
                    )
                elif "DATE" in col_type_str.upper() or "TIMESTAMP" in col_type_str.upper():
                    user_val = st.text_input(
                        f"{col_name} ({col_type_str}) - 例: YYYY-MM-DD HH:MM:SS", key=f"insert_form_{col_name}"
                    )
                    insert_data[col_name] = user_val if user_val else None # 空入力はNoneとして扱う
                else: # VARCHAR, TEXTなど、その他の型はテキスト入力として扱う
                    insert_data[col_name] = st.text_input(f"{col_name} ({col_type_str})", key=f"insert_form_{col_name}")

            submitted = st.form_submit_button("INSERT文生成と実行") # フォーム送信ボタン

        if submitted: # フォームが送信されたら処理開始
            # 入力データからNoneや空文字を除外するかどうかは要件による。ここではそのまま渡す。
            final_insert_data = {k: v for k, v in insert_data.items()}

            if not any(v is not None and str(v).strip() != "" for v in final_insert_data.values()):
                st.warning("挿入するデータが入力されていません。")
            else:
                # INSERT文を生成 (確認用)
                generated_sql, params = generate_insert_statement(source_table_for_insert, final_insert_data)
                if generated_sql:
                    st.subheader("生成されたINSERT文 (確認用)")
                    st.code(generated_sql, language="sql")
                    st.write("パラメータ (実際の実行時):")
                    st.json(params) # パラメータをJSON形式で表示

                    # 「このINSERT文を実行する」ボタン (実際にDBに書き込む最終確認)
                    if st.button("このINSERT文を実行する", key="data_migration_ui_confirm_execute_insert"):
                        with st.spinner(f"テーブル '{source_table_for_insert}' にレコードを挿入中..."):
                            success, message = insert_record(
                                st.session_state.source_engine, source_table_for_insert, final_insert_data
                            )
                        if success:
                            st.success(message)
                        else:
                            st.error(message)
                else: # INSERT文生成に失敗した場合 (通常は data_dict が空の場合など)
                    st.error(f"INSERT文の生成に失敗しました: {params}") # params にエラーメッセージが入っている想定
    pass # render_data_migration_ui 関数の終わり
