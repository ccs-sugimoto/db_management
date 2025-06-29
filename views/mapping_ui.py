import streamlit as st
import pandas as pd # カラムマッピング表示に st.dataframe を使用
from db_utils import (
    save_column_mapping,      # カラムマッピング設定を保存
    get_mapping_config_names, # 保存済みのマッピング設定名を取得
    load_column_mapping,      # 保存済みマッピング設定を読み込み
    delete_column_mapping     # 保存済みマッピング設定を削除
)

def render_mapping_ui():
    """
    カラムマッピング設定のためのUIコンポーネントを描画します。
    ソーステーブルのカラムとターゲットテーブルのカラム（または手入力）との対応付け、
    マッピング設定の保存、読み込み、削除機能を提供します。
    """
    st.header("カラムマッピング設定") # セクションヘッダー

    # メタデータDBへの接続が確立されていることが前提
    if not st.session_state.get("metadata_engine"):
        st.warning("カラムマッピング機能を利用するには、まずサイドバーからメタデータDBに接続してください。")
        return # メタデータDB未接続時はここで処理を中断

    # ソースDBへの接続とソーステーブルの選択が完了しているか確認
    if not st.session_state.get("source_engine"):
        st.warning("カラムマッピングを行うには、まず「接続1 (ソース)」を設定・接続してください。")
        return
    if not st.session_state.get("source_selected_table"):
        st.warning("ソースDBに接続後、ソーステーブルを選択してください。")
        return

    # ターゲットテーブルの選択は任意とする
    # (例: INSERT文生成のみが目的の場合など、ターゲットテーブルが不要なケースも考慮)

    # --- UI構築 ---
    # ソーステーブルのカラムリストを取得 (state.pyで初期化、db_info_ui.pyで設定される)
    source_cols = [col["name"] for col in st.session_state.get("source_columns", [])]

    # ターゲットカラムの選択肢を準備
    target_cols_options = [""]  # 「未選択」または手動入力を許容するための空文字オプション
    if st.session_state.get("target_engine") and st.session_state.get("target_selected_table"):
        # ターゲットDBに接続済みでターゲットテーブルも選択されている場合、そのカラム一覧を追加
        target_cols_options.extend(
            [col["name"] for col in st.session_state.get("target_columns", [])]
        )

    # UIを2カラムに分割 (左: マッピング編集、右: 保存・読み込み)
    map_col1, map_col2 = st.columns([2, 1]) # 左カラムを広めに

    with map_col1: # --- 左カラム: 現在のマッピング編集 ---
        st.subheader("現在のマッピング")
        if not source_cols:
            st.info("ソーステーブルのカラム情報を読み込んでください。")
        else:
            new_mapping = {} # ユーザーがUIで編集したマッピングを一時的に格納する辞書

            # 各ソースカラムに対応するターゲットカラムを選択するselectboxを表示
            for i, src_col in enumerate(source_cols):
                col1, col2 = st.columns([2, 3]) # 左カラム幅2、右カラム幅3 (比率は適宜調整)

                with col1:
                    st.markdown(f"`{src_col}` (ソース)")
                    # 必要であれば、ここにカラムの型情報などを追加表示することも可能
                    # src_col_type = next((col["type"] for col in st.session_state.get("source_columns", []) if col["name"] == src_col), "")
                    # st.caption(src_col_type)


                with col2:
                    # 現在のセッションに保存されているマッピング情報を取得 (なければ空文字)
                    current_tgt_col = st.session_state.get("column_map", {}).get(src_col, "")

                    # ターゲットカラムの選択肢に、現在マッピングされているカラム名が含まれていない場合、
                    # 一時的に選択肢に追加する (ロードしたマッピングが現在のターゲットテーブルのカラムにない場合など)
                    temp_target_cols_options = list(target_cols_options) # コピーして変更
                    if current_tgt_col and current_tgt_col not in temp_target_cols_options:
                        temp_target_cols_options.append(current_tgt_col)

                    # selectboxのデフォルト選択インデックスを決定
                    try:
                        idx = temp_target_cols_options.index(current_tgt_col)
                    except ValueError: # マッピング値が選択肢にない場合 (通常は上記で追加されるはず)
                        idx = 0 # 未選択にする

                    selected_tgt_col = st.selectbox(
                        label="ターゲットカラム →", # ラベルを簡潔に、または label_visibility="collapsed" も検討可
                        options=temp_target_cols_options,
                        index=idx,
                        key=f"mapping_ui_map_{src_col}_to_{i}", # ユニークキー
                        help=f"ソースカラム '{src_col}' に対応するターゲットカラムを選択または入力してください。"
                    )
                    if selected_tgt_col: # 空文字でなければマッピング対象とする
                        new_mapping[src_col] = selected_tgt_col

            # 「このマッピングを適用」ボタン
            if st.button("このマッピングを適用", key="mapping_ui_apply_button"):
                st.session_state.column_map = new_mapping # セッション状態を更新
                st.success("現在のマッピングを更新しました。")
                # 注意: st.rerun() はここでは不要。適用後のマッピングは下の st.dataframe で自動的に再表示される。

            # 適用中のマッピングを表示 (存在する場合)
            if st.session_state.get("column_map"):
                st.write("適用中のマッピング:")
                st.dataframe(
                    pd.DataFrame(
                        list(st.session_state.column_map.items()),
                        columns=["ソースカラム", "ターゲットカラム"],
                    ),
                    use_container_width=True,
                )

    with map_col2: # --- 右カラム: マッピングの保存と読み込み ---
        st.subheader("マッピングの保存と読み込み")

        # 保存済みマッピング名の一覧をメタデータDBから取得してセッション状態に格納
        # (この処理はサイドバーのメタデータDB接続時にも行われるが、UIの整合性のためにここでも行う)
        if st.session_state.get("metadata_engine"):
            st.session_state.saved_mappings = get_mapping_config_names(
                st.session_state.metadata_engine
            )

        # マッピング名入力フィールド
        current_mapping_name_input = st.text_input(
            "マッピング名",
            value=st.session_state.get("current_mapping_name", ""), # セッションから現在の名前を取得
            key="mapping_ui_name_input",
            help="保存または読み込むマッピング設定の名前を入力します。"
        )
        st.session_state.current_mapping_name = current_mapping_name_input # 入力値をセッションに保存

        # 「現在のマッピングを保存」ボタン
        if st.button("現在のマッピングを保存", key="mapping_ui_save_button"):
            # バリデーションチェック
            if not st.session_state.current_mapping_name:
                st.error("マッピング名を入力してください。")
            elif not st.session_state.get("column_map"):
                st.error("保存するカラムマッピングがありません。「このマッピングを適用」ボタンを押してください。")
            elif not st.session_state.get("source_selected_table"): # ソーステーブルは必須
                st.error("ソーステーブルが選択されていません。")
            else:
                # DB接続URLを文字列として取得 (存在しない場合は空文字)
                source_db_url_str = str(st.session_state.source_engine.url) if st.session_state.get("source_engine") else ""
                target_db_url_str = str(st.session_state.target_engine.url) if st.session_state.get("target_engine") else ""

                # マッピング保存処理の実行
                success, message = save_column_mapping(
                    st.session_state.metadata_engine,
                    st.session_state.current_mapping_name,
                    source_db_url_str,
                    target_db_url_str,
                    st.session_state.source_selected_table,
                    st.session_state.get("target_selected_table", ""), # ターゲットテーブルは任意なので空文字許容
                    st.session_state.column_map,
                )
                if success:
                    st.success(message)
                    # 保存成功後、保存済みマッピングリストを再読み込み
                    st.session_state.saved_mappings = get_mapping_config_names(st.session_state.metadata_engine)
                else:
                    st.error(message)

        st.markdown("---") # 区切り線

        # --- 保存済みマッピングの読み込みと削除 ---
        if st.session_state.get("saved_mappings"): # 保存済みマッピングが存在する場合
            selected_map_to_load = st.selectbox(
                "保存済みマッピングを選択",
                options=[""] + st.session_state.saved_mappings, # 先頭に未選択オプション
                key="mapping_ui_load_select",
                help="読み込む、または削除するマッピング設定を選択します。"
            )

            # 読み込みボタンと削除ボタンを横並びに配置
            col_load, col_delete = st.columns(2)
            with col_load:
                if st.button("選択したマッピングを読み込み", disabled=not selected_map_to_load, key="mapping_ui_load_button"):
                    config_details, mappings = load_column_mapping(st.session_state.metadata_engine, selected_map_to_load)
                    if config_details and mappings is not None: # mappingsは空の辞書である可能性があるので is not None でチェック
                        st.session_state.current_mapping_name = config_details["name"]
                        st.session_state.column_map = mappings
                        st.info(f"マッピング '{selected_map_to_load}' を読み込みました。")
                        st.info(f"保存時の情報 - ソーステーブル: {config_details['source_table']}, ターゲットテーブル: {config_details['target_table'] or 'N/A'}")
                        # TODO: 読み込んだマッピングのDB情報やテーブル名に基づいて、現在の接続やテーブル選択を自動で更新する機能も検討可能
                        st.rerun() # UIを再描画して、読み込んだマッピングを「現在のマッピング」セクションに反映
                    else:
                        st.error(f"マッピング '{selected_map_to_load}' の読み込みに失敗しました。")

            with col_delete:
                if st.button("選択したマッピングを削除", type="secondary", disabled=not selected_map_to_load, key="mapping_ui_delete_button"):
                    success, message = delete_column_mapping(st.session_state.metadata_engine, selected_map_to_load)
                    if success:
                        st.success(message)
                        # 削除成功後、保存済みマッピングリストと現在のマッピング情報を更新
                        st.session_state.saved_mappings = get_mapping_config_names(st.session_state.metadata_engine)
                        if st.session_state.current_mapping_name == selected_map_to_load:
                            st.session_state.current_mapping_name = ""
                            st.session_state.column_map = {}
                        st.rerun() # UIを再描画
                    else:
                        st.error(message)
        else: # 保存済みマッピングがない場合
            st.info("保存されているマッピングはありません。")

    pass # render_mapping_ui 関数の終わり
