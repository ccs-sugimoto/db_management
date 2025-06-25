import streamlit as st
import psycopg2
import sqlite3
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError


def get_postgres_connection_string(db_name, user, password, host, port):
    """PostgreSQLの接続文字列を生成する"""
    return f"postgresql://{user}:{password}@{host}:{port}/{db_name}"


def get_sqlite_connection_string(db_path):
    """SQLiteの接続文字列を生成する"""
    return f"sqlite:///{db_path}"


def test_postgres_connection(db_name, user, password, host, port):
    """PostgreSQLへの接続をテストする"""
    try:
        conn_str = get_postgres_connection_string(db_name, user, password, host, port)
        engine = create_engine(conn_str)
        with engine.connect() as connection:
            connection.execute(text("SELECT 1"))
        return True, "PostgreSQLへの接続に成功しました。"
    except SQLAlchemyError as e:
        return False, f"PostgreSQLへの接続に失敗しました: {e}"
    except Exception as e:
        return False, f"予期せぬエラーが発生しました: {e}"


def test_sqlite_connection(db_path):
    """SQLiteへの接続をテストする"""
    try:
        conn_str = get_sqlite_connection_string(db_path)
        engine = create_engine(conn_str)
        with engine.connect() as connection:
            connection.execute(text("SELECT 1"))
        return True, "SQLiteへの接続に成功しました。"
    except SQLAlchemyError as e:
        return False, f"SQLiteへの接続に失敗しました: {e}"
    except Exception as e:
        return False, f"予期せぬエラーが発生しました: {e}"


from sqlalchemy import inspect  # 追加


def get_db_engine(db_type, connection_params):
    """指定されたDBタイプのエンジンを取得する"""
    if db_type == "postgresql":
        conn_str = get_postgres_connection_string(**connection_params)
    elif db_type == "sqlite":
        conn_str = get_sqlite_connection_string(**connection_params)
    else:
        raise ValueError("未対応のデータベースタイプです。")

    try:
        engine = create_engine(conn_str)
        # 接続テスト
        with engine.connect() as connection:
            connection.execute(text("SELECT 1"))
        return engine
    except SQLAlchemyError as e:
        # st.error は Streamlit コンテキストでのみ有効なため、呼び出し元で処理する
        # st.error(f"{db_type}への接続に失敗しました: {e}")
        raise  # エラーを再送出して呼び出し元でキャッチできるようにする
    except Exception as e:
        # st.error(f"予期せぬエラーが発生しました: {e}")
        raise


def get_table_names(engine):
    """データベースエンジンからテーブル名の一覧を取得する"""
    try:
        inspector = inspect(engine)
        return inspector.get_table_names()
    except Exception as e:
        # st.error(f"テーブル一覧の取得に失敗しました: {e}") # 呼び出し元で処理
        raise RuntimeError(f"テーブル一覧の取得に失敗しました: {e}")


def get_table_columns(engine, table_name):
    """指定されたテーブルのカラム情報を取得する"""
    try:
        inspector = inspect(engine)
        columns = inspector.get_columns(table_name)
        return [{"name": col["name"], "type": str(col["type"])} for col in columns]
    except Exception as e:
        # st.error(f"テーブル '{table_name}' のカラム情報取得に失敗しました: {e}") # 呼び出し元で処理
        raise RuntimeError(
            f"テーブル '{table_name}' のカラム情報取得に失敗しました: {e}"
        )


# --- メタデータDB (SQLite) 関連の関数 ---
def create_metadata_tables_if_not_exists(engine):
    """メタデータDBに必要なテーブルを作成する"""
    with engine.connect() as connection:
        try:
            # カラムマッピング設定テーブル
            connection.execute(
                text("""
                CREATE TABLE IF NOT EXISTS mapping_configs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT UNIQUE NOT NULL,
                    source_db_url TEXT,
                    target_db_url TEXT,
                    source_table TEXT NOT NULL,
                    target_table TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            )
            # カラムマッピング詳細テーブル
            connection.execute(
                text("""
                CREATE TABLE IF NOT EXISTS column_mappings (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    config_id INTEGER NOT NULL,
                    source_column TEXT NOT NULL,
                    target_column TEXT NOT NULL,
                    FOREIGN KEY (config_id) REFERENCES mapping_configs(id) ON DELETE CASCADE
                )
            """)
            )
            connection.commit()
            # st.success("メタデータテーブルの準備ができました。") # Streamlitコンポーネントは呼び出し元で
        except Exception as e:
            connection.rollback()
            # st.error(f"メタデータテーブル作成エラー: {e}")
            raise RuntimeError(f"メタデータテーブル作成エラー: {e}")


def save_column_mapping(
    engine,
    mapping_name,
    source_db_url,
    target_db_url,
    source_table,
    target_table,
    mappings,
):
    """カラムマッピングをSQLiteに保存する"""
    with engine.connect() as connection:
        try:
            # 既存のマッピング設定を名前で検索（あれば更新、なければ新規作成）
            result = connection.execute(
                text("SELECT id FROM mapping_configs WHERE name = :name"),
                {"name": mapping_name},
            ).fetchone()

            if result:  # 既存設定があれば、まず古いマッピングを削除
                config_id = result[0]
                connection.execute(
                    text("DELETE FROM column_mappings WHERE config_id = :config_id"),
                    {"config_id": config_id},
                )
                # mapping_configs も更新する（DB URLなどが変わる可能性があるため）
                connection.execute(
                    text("""
                        UPDATE mapping_configs 
                        SET source_db_url = :source_db_url, target_db_url = :target_db_url,
                            source_table = :source_table, target_table = :target_table
                        WHERE id = :config_id
                    """),
                    {
                        "source_db_url": source_db_url,
                        "target_db_url": target_db_url,
                        "source_table": source_table,
                        "target_table": target_table,
                        "config_id": config_id,
                    },
                )
            else:  # 新規作成
                insert_config = text("""
                    INSERT INTO mapping_configs (name, source_db_url, target_db_url, source_table, target_table)
                    VALUES (:name, :source_db_url, :target_db_url, :source_table, :target_table)
                """)
                result = connection.execute(
                    insert_config,
                    {
                        "name": mapping_name,
                        "source_db_url": source_db_url,
                        "target_db_url": target_db_url,
                        "source_table": source_table,
                        "target_table": target_table,
                    },
                )
                config_id = result.lastrowid

            # 新しいカラムマッピングを挿入
            if config_id:
                insert_mapping = text("""
                    INSERT INTO column_mappings (config_id, source_column, target_column)
                    VALUES (:config_id, :source_column, :target_column)
                """)
                for src_col, tgt_col in mappings.items():
                    if src_col and tgt_col:  # 両方のカラムが指定されている場合のみ保存
                        connection.execute(
                            insert_mapping,
                            {
                                "config_id": config_id,
                                "source_column": src_col,
                                "target_column": tgt_col,
                            },
                        )

            connection.commit()
            return True, "カラムマッピングを保存しました。"
        except Exception as e:
            connection.rollback()
            return False, f"カラムマッピングの保存に失敗しました: {e}"


def get_mapping_config_names(engine):
    """保存されているマッピング設定名の一覧を取得する"""
    try:
        with engine.connect() as connection:
            result = connection.execute(
                text("SELECT name FROM mapping_configs ORDER BY name")
            )
            return [row[0] for row in result.fetchall()]
    except Exception as e:
        # st.error(f"マッピング設定名の取得に失敗しました: {e}")
        return []


def load_column_mapping(engine, mapping_name):
    """指定された名前のマッピング設定と詳細を読み込む"""
    try:
        with engine.connect() as connection:
            config_result = connection.execute(
                text(
                    "SELECT id, source_db_url, target_db_url, source_table, target_table FROM mapping_configs WHERE name = :name"
                ),
                {"name": mapping_name},
            ).fetchone()

            if not config_result:
                return None, None  # 設定が見つからない

            config_id, source_db_url, target_db_url, source_table, target_table = (
                config_result
            )

            mappings_result = connection.execute(
                text(
                    "SELECT source_column, target_column FROM column_mappings WHERE config_id = :config_id"
                ),
                {"config_id": config_id},
            )
            mappings = {row[0]: row[1] for row in mappings_result.fetchall()}

            config_details = {
                "name": mapping_name,
                "source_db_url": source_db_url,
                "target_db_url": target_db_url,
                "source_table": source_table,
                "target_table": target_table,
            }
            return config_details, mappings
    except Exception as e:
        # st.error(f"マッピング '{mapping_name}' の読み込みに失敗しました: {e}")
        return None, None


def delete_column_mapping(engine, mapping_name):
    """指定された名前のマッピング設定を削除する"""
    try:
        with engine.connect() as connection:
            # 関連する column_mappings も CASCADE DELETE で削除される想定
            result = connection.execute(
                text("DELETE FROM mapping_configs WHERE name = :name"),
                {"name": mapping_name},
            )
            connection.commit()
            if result.rowcount > 0:
                return True, f"マッピング '{mapping_name}' を削除しました。"
            else:
                return False, f"マッピング '{mapping_name}' が見つかりません。"
    except Exception as e:
        connection.rollback()
        return False, f"マッピング '{mapping_name}' の削除に失敗しました: {e}"


if __name__ == "__main__":
    # 簡単なテストコード (Streamlit環境外での実行用)
    # PostgreSQL接続テスト
    # ... (既存のテストコードは変更なし) ...

    # SQLite接続テスト (カレントディレクトリに metadata_test.db を作成)
    sqlite_conn_params_test = {"db_path": "metadata_test.db"}

    try:
        sqlite_engine_test = get_db_engine("sqlite", sqlite_conn_params_test)
        if sqlite_engine_test:
            print("SQLiteテストエンジン取得成功")
            create_metadata_tables_if_not_exists(sqlite_engine_test)
            print("メタデータテーブル作成/確認完了")

            # テストデータの保存
            test_mappings = {
                "id": "customer_id",
                "name": "full_name",
                "email": "email_address",
            }
            success, msg = save_column_mapping(
                sqlite_engine_test,
                "TestMap1",
                "postgresql://user:pass@host1/src_db",
                "postgresql://user:pass@host2/tgt_db",
                "source_users",
                "target_customers",
                test_mappings,
            )
            print(f"保存テスト1: {success} - {msg}")

            success, msg = save_column_mapping(
                sqlite_engine_test,
                "TestMap2",
                "postgresql://user:pass@host1/src_db",
                "sqlite:///path/to/other.db",
                "products",
                "inventory",
                {"product_id": "item_id", "quantity": "stock_count"},
            )
            print(f"保存テスト2: {success} - {msg}")

            # マッピング名一覧取得テスト
            names = get_mapping_config_names(sqlite_engine_test)
            print(f"保存されたマッピング名: {names}")

            # マッピング読み込みテスト
            if names:
                config, mappings = load_column_mapping(sqlite_engine_test, names[0])
                if config:
                    print(f"読み込んだ設定 ({names[0]}): {config}")
                    print(f"読み込んだマッピング ({names[0]}): {mappings}")

            # マッピング更新テスト
            updated_mappings = {
                "id": "user_id",
                "name": "user_name",
                "email": "contact_email",
                "created_at": "join_date",
            }
            success, msg = save_column_mapping(
                sqlite_engine_test,
                "TestMap1",  # 既存の名前で保存 = 更新
                "postgresql://user:pass@host1/src_db_updated",
                "postgresql://user:pass@host2/tgt_db_updated",
                "source_users_v2",
                "target_customers_v2",
                updated_mappings,
            )
            print(f"更新テスト (TestMap1): {success} - {msg}")
            config, mappings = load_column_mapping(sqlite_engine_test, "TestMap1")
            if config:
                print(f"更新後の設定 (TestMap1): {config}")
                print(f"更新後のマッピング (TestMap1): {mappings}")

            # マッピング削除テスト
            if len(names) > 1:
                success, msg = delete_column_mapping(sqlite_engine_test, names[1])
                print(f"削除テスト ({names[1]}): {success} - {msg}")
                names_after_delete = get_mapping_config_names(sqlite_engine_test)
                print(f"削除後のマッピング名: {names_after_delete}")

    except Exception as e:
        print(f"SQLiteテスト中にエラー: {e}")
    finally:
        # テスト用DBファイルを削除する場合はここで行う
        # import os
        # if os.path.exists("metadata_test.db"):
        #     os.remove("metadata_test.db")
        #     print("テスト用DBファイル削除")
        pass


# --- データ操作関連 ---
def migrate_data(
    source_engine, target_engine, source_table, target_table, column_map, chunksize=1000
):
    """
    指定されたマッピングに基づいてソーステーブルからターゲットテーブルへデータを移行する。
    Pandas DataFrameを介して処理する。
    """
    try:
        # 既存のターゲットテーブルのデータをどう扱うか？ (ここでは追記を想定)
        # 必要に応じて、移行前にターゲットテーブルをクリアするオプションなどを追加できる

        # SELECT文の構築 (ソースカラムのみ選択)
        source_columns_to_select = list(column_map.keys())
        if not source_columns_to_select:
            return False, "マッピングされるソースカラムがありません。"

        select_query = (
            f"SELECT {', '.join(source_columns_to_select)} FROM {source_table}"
        )

        total_rows_migrated = 0

        # ソースからデータをチャンクで読み込み
        for chunk_df in pd.read_sql_query(
            select_query, source_engine, chunksize=chunksize
        ):
            if chunk_df.empty:
                continue

            # カラム名をターゲットテーブル用にリネーム
            renamed_chunk_df = chunk_df.rename(columns=column_map)

            # ターゲットテーブルにデータを挿入
            # to_sqlのif_existsは 'append', 'replace', 'fail' がある
            # ここでは 'append' を使用
            renamed_chunk_df.to_sql(
                target_table, target_engine, if_exists="append", index=False
            )
            total_rows_migrated += len(renamed_chunk_df)

        return (
            True,
            f"{total_rows_migrated}行のデータを {source_table} から {target_table} へ移行しました。",
        )

    except Exception as e:
        return False, f"データ移行中にエラーが発生しました: {e}"


def generate_insert_statement(table_name, data_dict):
    """
    テーブル名とデータの辞書からINSERT文を生成する。
    実際の値はプレースホルダーにするか、適切にエスケープする必要があるが、
    SQLAlchemyのtextとパラメータ結合を使えばエンジン側で処理される。
    この関数は主に確認用や手動実行用。
    """
    if not data_dict:
        return None, "データが空です。"

    columns = ", ".join(data_dict.keys())
    # 値はSQLインジェクションを防ぐため、そのまま文字列結合しない
    # SQLAlchemyで実行する際はパラメータとして渡すので :key 形式のプレースホルダを使う
    placeholders = ", ".join(f":{key}" for key in data_dict.keys())

    statement = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
    return statement, data_dict  # 文とパラメータの辞書を返す


def insert_record(engine, table_name, data_dict):
    """
    指定されたテーブルに1レコードを挿入する。
    data_dict: {'column_name': value, ...}
    """
    if not data_dict:
        return False, "挿入するデータがありません。"

    statement, params = generate_insert_statement(table_name, data_dict)
    if not statement:
        return False, params  # params here is the error message

    try:
        with engine.connect() as connection:
            connection.execute(text(statement), params)
            connection.commit()
        return True, f"テーブル '{table_name}' に1レコードを挿入しました。"
    except Exception as e:
        return False, f"レコード挿入中にエラーが発生しました: {e}"
