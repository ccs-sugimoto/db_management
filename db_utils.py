# データベース操作に関連するユーティリティ関数群
# import streamlit as st # Streamlit固有の機能はここでは使用しない (UIから分離するため)
import psycopg2 # PostgreSQL接続に必要 (SQLAlchemy経由だが、エラー型などで参照される可能性)
import sqlite3  # SQLite接続に必要 (SQLAlchemy経由だが、エラー型などで参照される可能性)
from sqlalchemy import create_engine, text, inspect # SQLAlchemyの主要コンポーネント
from sqlalchemy.exc import SQLAlchemyError # SQLAlchemyの例外クラス
import pandas as pd # データ移行時に使用

# --- 接続文字列生成 ---

def get_postgres_connection_string(db_name, user, password, host, port, **kwargs):
    """PostgreSQLデータベースへの接続文字列を生成します。"""
    return f"postgresql://{user}:{password}@{host}:{port}/{db_name}"


def get_sqlite_connection_string(db_path, **kwargs):
    """SQLiteデータベースへの接続文字列を生成します。"""
    return f"sqlite:///{db_path}"

# --- 接続テスト関数 ---

def test_postgres_connection(db_name, user, password, host, port):
    """PostgreSQLデータベースへの接続をテストします。

    Args:
        db_name (str): データベース名。
        user (str): ユーザー名。
        password (str): パスワード。
        host (str): ホスト名。
        port (str): ポート番号。

    Returns:
        tuple: (bool, str) 接続の成否とメッセージ。
    """
    try:
        conn_str = get_postgres_connection_string(db_name, user, password, host, port)
        engine = create_engine(conn_str)
        with engine.connect() as connection:
            connection.execute(text("SELECT 1")) # 簡単なクエリを実行して接続を確認
        return True, "PostgreSQLへの接続に成功しました。"
    except SQLAlchemyError as e:
        return False, f"PostgreSQLへの接続に失敗しました: {e}"
    except Exception as e: # その他の予期せぬエラー
        return False, f"予期せぬエラーが発生しました: {e}"


def test_sqlite_connection(db_path):
    """SQLiteデータベースへの接続をテストします。

    Args:
        db_path (str): SQLiteデータベースファイルへのパス。

    Returns:
        tuple: (bool, str) 接続の成否とメッセージ。
    """
    try:
        conn_str = get_sqlite_connection_string(db_path)
        engine = create_engine(conn_str)
        with engine.connect() as connection:
            connection.execute(text("SELECT 1")) # 簡単なクエリを実行して接続を確認
        return True, "SQLiteへの接続に成功しました。"
    except SQLAlchemyError as e:
        return False, f"SQLiteへの接続に失敗しました: {e}"
    except Exception as e: # その他の予期せぬエラー
        return False, f"予期せぬエラーが発生しました: {e}"

# --- データベースエンジンと情報の取得 ---

def get_db_engine(db_type, connection_params):
    """指定されたデータベースタイプに応じたSQLAlchemyエンジンを取得します。
    接続テストも内部で行います。

    Args:
        db_type (str): データベースタイプ ("postgresql" または "sqlite")。
        connection_params (dict): 接続に必要なパラメータの辞書。
                                  (例: PostgreSQL -> {"host": "localhost", ...}, SQLite -> {"db_path": "file.db"})

    Returns:
        sqlalchemy.engine.Engine: 成功した場合はSQLAlchemyエンジン。

    Raises:
        ValueError: 未対応のデータベースタイプが指定された場合。
        SQLAlchemyError: データベース接続に失敗した場合。
        Exception: その他の予期せぬエラーが発生した場合。
    """
    if db_type == "postgresql":
        conn_str = get_postgres_connection_string(**connection_params)
    elif db_type == "sqlite":
        conn_str = get_sqlite_connection_string(**connection_params)
    else:
        raise ValueError(f"未対応のデータベースタイプです: {db_type}")

    try:
        engine = create_engine(conn_str)
        # 接続テストとして簡単なクエリを実行
        with engine.connect() as connection:
            connection.execute(text("SELECT 1"))
        return engine
    except SQLAlchemyError as e:
        # UI関連のエラー表示(st.errorなど)は呼び出し元で行うため、ここではエラーを再送出する
        raise SQLAlchemyError(f"{db_type}へのエンジン取得・接続テストに失敗しました: {e}")
    except Exception as e:
        raise Exception(f"エンジン取得中に予期せぬエラーが発生しました: {e}")


def get_table_names(engine, schema_name="public"):
    """データベースエンジンからテーブル名のリストを取得します。

    Args:
        engine (sqlalchemy.engine.Engine): SQLAlchemyエンジン。
        schema_name (str, optional): スキーマ名。デフォルトは "public"。

    Returns:
        list: テーブル名とコメントを含む辞書のリスト (例: [{"name": "table1", "comment": "comment1"}, ...])。

    Raises:
        RuntimeError: テーブル一覧の取得に失敗した場合。
    """
    try:
        if engine.dialect.name == "postgresql":
            # PostgreSQLの場合、指定されたスキーマのテーブル名とコメントを取得するクエリ
            query = text("""
                SELECT
                    t.table_name AS name,
                    pg_catalog.obj_description(pc.oid, 'pg_class') AS comment
                FROM
                    information_schema.tables t
                JOIN
                    pg_catalog.pg_class pc ON pc.relname = t.table_name
                JOIN
                    pg_catalog.pg_namespace pn ON pn.oid = pc.relnamespace
                WHERE
                    t.table_schema = :schema_name_param
                    AND pn.nspname = :schema_name_param
                    AND t.table_type = 'BASE TABLE'
                ORDER BY
                    t.table_name;
            """)
            with engine.connect() as connection:
                result = connection.execute(query, {"schema_name_param": schema_name})
                return [{"name": row.name, "comment": row.comment or ""} for row in result]
        else:
            # PostgreSQL以外の場合
            inspector = inspect(engine)
            # schema_nameがNoneまたは空文字列の場合、デフォルトのスキーマ（または全スキーマ）を期待
            # SQLAlchemy inspectorは schema=None を適切に処理する
            # "public" が指定された場合も、それがデフォルトスキーマとして扱われることを期待
            # もし明示的に「全てのスキーマ」を意図する場合は、この関数のインターフェース変更が必要
            effective_schema_name = schema_name if schema_name and schema_name.strip() else None

            # 既存のget_table_namesはコメントを返さないため、PostgreSQL以外ではコメントはNoneとする
            table_names_list = inspector.get_table_names(schema=effective_schema_name)
            return [{"name": name, "comment": None} for name in table_names_list]
    except Exception as e:
        # UI関連のエラー表示は呼び出し元で行う
        raise RuntimeError(f"テーブル一覧の取得に失敗しました (スキーマ: {schema_name}): {e}")


def get_table_columns(engine, table_name, schema_name="public"):
    """指定されたテーブルのカラム情報を取得します。

    Args:
        engine (sqlalchemy.engine.Engine): SQLAlchemyエンジン。
        table_name (str): カラム情報を取得するテーブル名。
        schema_name (str, optional): スキーマ名。デフォルトは "public"。

    Returns:
        list: カラム情報の辞書のリスト (例: [{"name": "col1", "type": "VARCHAR"}, ...])。

    Raises:
        RuntimeError: カラム情報の取得に失敗した場合。
    """
    try:
        if engine.dialect.name == "postgresql":
            # PostgreSQLの場合、カラム名、型、コメントを取得するクエリ
            # INFORMATION_SCHEMA.COLUMNS と pg_catalog.pg_description を使用
            # スキーマ名を 'public' に固定せず、テーブル名にスキーマが含まれている可能性を考慮
            # table_name が "schema.table" の形式である場合に対応
            schema_name, actual_table_name = table_name.split('.') if '.' in table_name else (schema_name, table_name)

            query = text("""
                SELECT
                    c.column_name AS name,
                    c.data_type AS type,
                    pg_catalog.col_description(pc.oid, c.ordinal_position::int) AS comment
                FROM
                    information_schema.columns c
                JOIN
                    pg_catalog.pg_class pc ON pc.relname = c.table_name
                JOIN
                    pg_catalog.pg_namespace pn ON pn.oid = pc.relnamespace
                WHERE
                    c.table_schema = :schema_name_param
                    AND pn.nspname = :schema_name_param
                    AND c.table_name = :table_name_param
                ORDER BY
                    c.ordinal_position;
            """)
            with engine.connect() as connection:
                result = connection.execute(query, {"schema_name_param": schema_name, "table_name_param": actual_table_name})
                return [{"name": row.name, "type": row.type, "comment": row.comment or ""} for row in result]
        else:
            # PostgreSQL以外の場合は既存の処理
            inspector = inspect(engine)
            columns = inspector.get_columns(table_name)
            # カラム名と型(文字列として)を抽出してリスト化
            return [{"name": col["name"], "type": str(col["type"]), "comment": None} for col in columns]
    except Exception as e:
        # UI関連のエラー表示は呼び出し元で行う
        raise RuntimeError(
            f"テーブル '{table_name}' のカラム情報取得に失敗しました: {e}"
        )


# --- メタデータDB (SQLite) 関連の関数 ---

def create_metadata_tables_if_not_exists(engine):
    """カラムマッピング設定などを保存するためのメタデータテーブルをSQLite DB内に作成します。
    テーブルが存在しない場合のみ作成処理が実行されます。

    Args:
        engine (sqlalchemy.engine.Engine): メタデータDB (SQLite) のSQLAlchemyエンジン。

    Raises:
        RuntimeError: テーブル作成に失敗した場合。
    """
    with engine.connect() as connection:
        try:
            # カラムマッピング設定のヘッダー情報を保存するテーブル
            connection.execute(
                text("""
                CREATE TABLE IF NOT EXISTS mapping_configs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, -- 自動採番の主キー
                    name TEXT UNIQUE NOT NULL,            -- マッピング設定名 (ユニーク)
                    source_db_url TEXT,                   -- ソースDBの接続URL (参考情報)
                    target_db_url TEXT,                   -- ターゲットDBの接続URL (参考情報)
                    source_table TEXT NOT NULL,           -- ソーステーブル名
                    target_table TEXT NOT NULL,           -- ターゲットテーブル名
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP -- 作成日時
                )
            """)
            )
            # 保存された接続情報を格納するテーブル
            connection.execute(
                text("""
                CREATE TABLE IF NOT EXISTS saved_connections (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT UNIQUE NOT NULL,
                    db_type TEXT NOT NULL,
                    host TEXT,
                    port TEXT,
                    db_name TEXT,
                    user TEXT,
                    password TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            )
            # 個々のカラムマッピング詳細を保存するテーブル
            connection.execute(
                text("""
                CREATE TABLE IF NOT EXISTS column_mappings (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, -- 自動採番の主キー
                    config_id INTEGER NOT NULL,           -- mapping_configsテーブルへの外部キー
                    source_column TEXT NOT NULL,          -- ソースカラム名
                    target_column TEXT NOT NULL,          -- ターゲットカラム名
                    FOREIGN KEY (config_id) REFERENCES mapping_configs(id) ON DELETE CASCADE -- 親レコード削除時に子も削除
                )
            """)
            )
            connection.commit() # トランザクションをコミット
            # StreamlitのUIコンポーネント(st.successなど)は呼び出し元で表示する
        except Exception as e:
            connection.rollback() # エラー発生時はロールバック
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
    """カラムマッピング設定をメタデータDB (SQLite) に保存します。
    同名の設定が存在する場合は更新し、存在しない場合は新規作成します。

    Args:
        engine (sqlalchemy.engine.Engine): メタデータDBのエンジン。
        mapping_name (str): 保存するマッピング設定の名前。
        source_db_url (str): ソースDBのURL（参考情報）。
        target_db_url (str): ターゲットDBのURL（参考情報）。
        source_table (str): ソーステーブル名。
        target_table (str): ターゲットテーブル名。
        mappings (dict): カラムマッピング情報 ({"ソースカラム名": "ターゲットカラム名", ...})。

    Returns:
        tuple: (bool, str) 保存の成否とメッセージ。
    """
    with engine.connect() as connection:
        try:
            # トランザクション開始 (SQLAlchemy 2.0以降では Connection が自動的にトランザクションを開始する場合があるが、明示的にすることも可能)
            # 既存のマッピング設定を名前で検索
            result = connection.execute(
                text("SELECT id FROM mapping_configs WHERE name = :name"),
                {"name": mapping_name},
            ).fetchone()

            if result:  # 既存設定がある場合
                config_id = result[0]
                # 既存のカラムマッピング詳細を一度削除 (更新のため)
                connection.execute(
                    text("DELETE FROM column_mappings WHERE config_id = :config_id"),
                    {"config_id": config_id},
                )
                # mapping_configs テーブルのレコードを更新 (URLやテーブル名が変わる可能性があるため)
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
            else:  # 新規作成の場合
                insert_config_sql = text("""
                    INSERT INTO mapping_configs (name, source_db_url, target_db_url, source_table, target_table)
                    VALUES (:name, :source_db_url, :target_db_url, :source_table, :target_table)
                """)
                cursor_result = connection.execute(
                    insert_config_sql,
                    {
                        "name": mapping_name,
                        "source_db_url": source_db_url,
                        "target_db_url": target_db_url,
                        "source_table": source_table,
                        "target_table": target_table,
                    },
                )
                config_id = cursor_result.lastrowid # 挿入されたレコードのIDを取得

            # 新しいカラムマッピング詳細を挿入
            if config_id: # config_id が正常に取得できた場合のみ実行
                insert_mapping_sql = text("""
                    INSERT INTO column_mappings (config_id, source_column, target_column)
                    VALUES (:config_id, :source_column, :target_column)
                """)
                for src_col, tgt_col in mappings.items():
                    if src_col and tgt_col:  # ソースとターゲットの両カラム名が存在する場合のみ保存
                        connection.execute(
                            insert_mapping_sql,
                            {
                                "config_id": config_id,
                                "source_column": src_col,
                                "target_column": tgt_col,
                            },
                        )

            connection.commit() # 全ての操作が成功したらコミット
            return True, "カラムマッピングを保存しました。"
        except Exception as e:
            connection.rollback() # エラー発生時はロールバック
            return False, f"カラムマッピングの保存に失敗しました: {e}"


def get_mapping_config_names(engine):
    """保存されている全てのマッピング設定の名前をリストで取得します。

    Args:
        engine (sqlalchemy.engine.Engine): メタデータDBのエンジン。

    Returns:
        list: マッピング設定名のリスト。エラー時は空リスト。
    """
    try:
        with engine.connect() as connection:
            result = connection.execute(
                text("SELECT name FROM mapping_configs ORDER BY name") # 名前順で取得
            )
            return [row[0] for row in result.fetchall()]
    except Exception as e:
        # UIへのエラー表示は呼び出し元で行う
        print(f"マッピング設定名の取得中にエラー: {e}") # ログ出力は行う
        return []


def load_column_mapping(engine, mapping_name):
    """指定された名前のマッピング設定（ヘッダー情報）と詳細（カラム対応）を読み込みます。

    Args:
        engine (sqlalchemy.engine.Engine): メタデータDBのエンジン。
        mapping_name (str): 読み込むマッピング設定の名前。

    Returns:
        tuple: (dict, dict) or (None, None)
                 成功時: (設定詳細辞書, カラムマッピング辞書)
                 失敗時または設定なし: (None, None)
    """
    try:
        with engine.connect() as connection:
            # マッピング設定のヘッダー情報を取得
            config_result = connection.execute(
                text(
                    "SELECT id, source_db_url, target_db_url, source_table, target_table FROM mapping_configs WHERE name = :name"
                ),
                {"name": mapping_name},
            ).fetchone()

            if not config_result:
                return None, None  # 指定された名前の設定が見つからない

            config_id, source_db_url, target_db_url, source_table, target_table = config_result

            # カラムマッピング詳細を取得
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
        # UIへのエラー表示は呼び出し元で行う
        print(f"マッピング '{mapping_name}' の読み込み中にエラー: {e}") # ログ出力
        return None, None


def delete_column_mapping(engine, mapping_name):
    """指定された名前のマッピング設定をメタデータDBから削除します。
    関連するカラムマッピング詳細もCASCADE DELETE制約により自動的に削除されます。

    Args:
        engine (sqlalchemy.engine.Engine): メタデータDBのエンジン。
        mapping_name (str): 削除するマッピング設定の名前。

    Returns:
        tuple: (bool, str) 削除の成否とメッセージ。
    """
    try:
        with engine.connect() as connection:
            result = connection.execute(
                text("DELETE FROM mapping_configs WHERE name = :name"),
                {"name": mapping_name},
            )
            connection.commit()
            if result.rowcount > 0: # 削除された行数を確認
                return True, f"マッピング '{mapping_name}' を削除しました。"
            else:
                return False, f"マッピング '{mapping_name}' が見つかりません。"
    except Exception as e:
        connection.rollback()
        return False, f"マッピング '{mapping_name}' の削除に失敗しました: {e}"


# --- 接続情報管理関数 ---

def save_connection_info(engine, name, db_type, params):
    """接続情報をメタデータDB (SQLite) に保存します。
    同名の接続情報が存在する場合は更新し、存在しない場合は新規作成します。

    Args:
        engine (sqlalchemy.engine.Engine): メタデータDBのエンジン。
        name (str): 保存する接続設定の名前。
        db_type (str): データベースタイプ ("postgresql" または "sqlite")。
        params (dict): 接続に必要なパラメータの辞書。
                       (例: {"host": "localhost", "port": "5432", ...})

    Returns:
        tuple: (bool, str) 保存の成否とメッセージ。
    """
    with engine.connect() as connection:
        try:
            # 既存の接続情報を名前で検索
            result = connection.execute(
                text("SELECT id FROM saved_connections WHERE name = :name"),
                {"name": name},
            ).fetchone()

            # params 辞書から各値を取得 (存在しないキーの場合は None)
            host = params.get("host")
            port = params.get("port")
            db_name = params.get("db_name")
            user = params.get("user")
            password = params.get("password") # 平文で保存

            if result:  # 既存設定がある場合
                config_id = result[0]
                # saved_connections テーブルのレコードを更新
                connection.execute(
                    text("""
                        UPDATE saved_connections
                        SET db_type = :db_type, host = :host, port = :port,
                            db_name = :db_name, user = :user, password = :password
                        WHERE id = :config_id
                    """),
                    {
                        "db_type": db_type,
                        "host": host,
                        "port": port,
                        "db_name": db_name,
                        "user": user,
                        "password": password,
                        "config_id": config_id,
                    },
                )
            else:  # 新規作成の場合
                insert_sql = text("""
                    INSERT INTO saved_connections (name, db_type, host, port, db_name, user, password)
                    VALUES (:name, :db_type, :host, :port, :db_name, :user, :password)
                """)
                connection.execute(
                    insert_sql,
                    {
                        "name": name,
                        "db_type": db_type,
                        "host": host,
                        "port": port,
                        "db_name": db_name,
                        "user": user,
                        "password": password,
                    },
                )
            connection.commit()
            return True, f"接続情報 '{name}' を保存しました。"
        except Exception as e:
            connection.rollback()
            return False, f"接続情報 '{name}' の保存に失敗しました: {e}"


def get_connection_names(engine):
    """保存されている全ての接続設定の名前をリストで取得します。

    Args:
        engine (sqlalchemy.engine.Engine): メタデータDBのエンジン。

    Returns:
        list: 接続設定名のリスト。エラー時は空リスト。
    """
    try:
        with engine.connect() as connection:
            result = connection.execute(
                text("SELECT name FROM saved_connections ORDER BY name") # 名前順で取得
            )
            return [row[0] for row in result.fetchall()]
    except Exception as e:
        print(f"接続設定名の取得中にエラー: {e}") # ログ出力は行う
        return []


def load_connection_info(engine, name):
    """指定された名前の接続情報を読み込みます。

    Args:
        engine (sqlalchemy.engine.Engine): メタデータDBのエンジン。
        name (str): 読み込む接続設定の名前。

    Returns:
        dict: 接続情報の辞書 (例: {"name": "my_pg", "db_type": "postgresql", ...})。
              見つからない場合やエラー時は None。
    """
    try:
        with engine.connect() as connection:
            result = connection.execute(
                text("SELECT name, db_type, host, port, db_name, user, password FROM saved_connections WHERE name = :name"),
                {"name": name},
            ).fetchone()

            if result:
                # カラム名と値を対応付けた辞書を作成
                columns = ["name", "db_type", "host", "port", "db_name", "user", "password"]
                return dict(zip(columns, result))
            else:
                return None # 指定された名前の設定が見つからない
    except Exception as e:
        print(f"接続情報 '{name}' の読み込み中にエラー: {e}") # ログ出力
        return None


def delete_connection_info(engine, name):
    """指定された名前の接続情報をメタデータDBから削除します。

    Args:
        engine (sqlalchemy.engine.Engine): メタデータDBのエンジン。
        name (str): 削除する接続設定の名前。

    Returns:
        tuple: (bool, str) 削除の成否とメッセージ。
    """
    try:
        with engine.connect() as connection:
            result = connection.execute(
                text("DELETE FROM saved_connections WHERE name = :name"),
                {"name": name},
            )
            connection.commit()
            if result.rowcount > 0: # 削除された行数を確認
                return True, f"接続情報 '{name}' を削除しました。"
            else:
                return False, f"接続情報 '{name}' が見つかりません。"
    except Exception as e:
        connection.rollback()
        return False, f"接続情報 '{name}' の削除に失敗しました: {e}"


def update_connection_info(engine, original_name, new_name, db_type, params):
    """接続情報を更新します。名前の変更も可能です。

    Args:
        engine (sqlalchemy.engine.Engine): メタデータDBのエンジン。
        original_name (str): 更新対象の元の接続設定の名前。
        new_name (str): 新しい接続設定の名前。
        db_type (str): 新しいデータベースタイプ。
        params (dict): 新しい接続パラメータ。

    Returns:
        tuple: (bool, str) 更新の成否とメッセージ。
    """
    with engine.connect() as connection:
        try:
            # まず、元の名前の接続情報が存在するか確認
            check_result = connection.execute(
                text("SELECT id FROM saved_connections WHERE name = :original_name"),
                {"original_name": original_name}
            ).fetchone()

            if not check_result:
                return False, f"接続情報 '{original_name}' が見つかりません。"

            config_id = check_result[0]

            # 新しい名前が元の名前と異なり、かつ新しい名前が既に存在するか確認 (ユニーク制約違反を避けるため)
            if original_name != new_name:
                name_check_result = connection.execute(
                    text("SELECT id FROM saved_connections WHERE name = :new_name"),
                    {"new_name": new_name}
                ).fetchone()
                if name_check_result:
                    return False, f"新しい接続名 '{new_name}' は既に使用されています。"

            # params 辞書から各値を取得
            host = params.get("host")
            port = params.get("port")
            db_name = params.get("db_name")
            user = params.get("user")
            password = params.get("password")

            # saved_connections テーブルのレコードを更新
            connection.execute(
                text("""
                    UPDATE saved_connections
                    SET name = :new_name, db_type = :db_type, host = :host, port = :port,
                        db_name = :db_name, user = :user, password = :password
                    WHERE id = :config_id
                """),
                {
                    "new_name": new_name,
                    "db_type": db_type,
                    "host": host,
                    "port": port,
                    "db_name": db_name,
                    "user": user,
                    "password": password,
                    "config_id": config_id,
                },
            )
            connection.commit()
            return True, f"接続情報 '{original_name}' を '{new_name}' に更新しました。"
        except Exception as e:
            connection.rollback()
            return False, f"接続情報 '{original_name}' の更新に失敗しました: {e}"


if __name__ == "__main__":
    # このスクリプトが直接実行された場合のテストコード
    # Streamlit環境外での簡易的な動作確認やデバッグに使用します。

    # PostgreSQL接続テストの例 (実際には接続情報を設定する必要あり)
    # print("--- PostgreSQL 接続テスト ---")
    # pg_params_test = {"db_name": "test_db", "user": "user", "password": "password", "host": "localhost", "port": "5432"}
    # pg_success, pg_msg = test_postgres_connection(**pg_params_test)
    # print(f"PostgreSQL 接続結果: {pg_success} - {pg_msg}")

    print("\n--- SQLite メタデータDBテスト ---")
    # SQLite接続テスト (カレントディレクトリに test_metadata.db を作成してテスト)
    sqlite_test_db_path = "test_metadata.db"
    sqlite_conn_params_test = {"db_path": sqlite_test_db_path}

    try:
        sqlite_engine_test = get_db_engine("sqlite", sqlite_conn_params_test)
        if sqlite_engine_test:
            print(f"SQLiteテストエンジン ({sqlite_test_db_path}) 取得成功")
            create_metadata_tables_if_not_exists(sqlite_engine_test)
            print("メタデータテーブル作成/確認完了")

            # 1. テストデータの保存
            print("\n1. マッピング保存テスト:")
            test_mappings1 = {"id": "customer_id", "name": "full_name", "email": "email_address"}
            s1_success, s1_msg = save_column_mapping(
                sqlite_engine_test, "顧客情報マッピング", "source_pg_url", "target_sqlite_url",
                "users_table", "customers_table", test_mappings1
            )
            print(f"  保存結果1: {s1_success} - {s1_msg}")

            test_mappings2 = {"product_code": "item_sku", "price": "unit_price"}
            s2_success, s2_msg = save_column_mapping(
                sqlite_engine_test, "商品情報マッピング", "source_oracle_url", "target_mysql_url",
                "products_master", "inventory_items", test_mappings2
            )
            print(f"  保存結果2: {s2_success} - {s2_msg}")

            # 2. マッピング名一覧取得テスト
            print("\n2. マッピング名一覧取得テスト:")
            names = get_mapping_config_names(sqlite_engine_test)
            print(f"  保存されたマッピング名: {names}")

            # 3. マッピング読み込みテスト
            print("\n3. マッピング読み込みテスト:")
            if names:
                first_map_name = names[0]
                config, mappings = load_column_mapping(sqlite_engine_test, first_map_name)
                if config:
                    print(f"  読み込んだ設定 ({first_map_name}): {config}")
                    print(f"  読み込んだマッピング ({first_map_name}): {mappings}")
                else:
                    print(f"  '{first_map_name}' の読み込みに失敗しました。")
            else:
                print("  読み込むマッピングがありません。")

            # 4. マッピング更新テスト
            print("\n4. マッピング更新テスト:")
            if names:
                map_to_update = names[0]
                updated_mappings = {"id": "user_id", "name": "username", "email": "user_email", "status": "active_status"}
                u_success, u_msg = save_column_mapping(
                    sqlite_engine_test, map_to_update, "source_pg_url_v2", "target_sqlite_url_v2",
                    "users_table_v2", "customers_table_v2", updated_mappings
                )
                print(f"  更新結果 ({map_to_update}): {u_success} - {u_msg}")
                u_config, u_mappings = load_column_mapping(sqlite_engine_test, map_to_update)
                if u_config:
                    print(f"  更新後の設定 ({map_to_update}): {u_config}")
                    print(f"  更新後のマッピング ({map_to_update}): {u_mappings}")
            else:
                print("  更新するマッピングがありません。")

            # 5. マッピング削除テスト
            print("\n5. マッピング削除テスト:")
            if len(names) > 1: # 2つ以上あれば2つ目を削除
                map_to_delete = names[1]
                d_success, d_msg = delete_column_mapping(sqlite_engine_test, map_to_delete)
                print(f"  削除結果 ({map_to_delete}): {d_success} - {d_msg}")
                names_after_delete = get_mapping_config_names(sqlite_engine_test)
                print(f"  削除後のマッピング名: {names_after_delete}")
            elif names: # 1つだけならそれを削除
                map_to_delete = names[0]
                d_success, d_msg = delete_column_mapping(sqlite_engine_test, map_to_delete)
                print(f"  削除結果 ({map_to_delete}): {d_success} - {d_msg}")
                names_after_delete = get_mapping_config_names(sqlite_engine_test)
                print(f"  削除後のマッピング名: {names_after_delete}")
            else:
                print("  削除するマッピングがありません。")

    except Exception as e:
        print(f"SQLiteテスト中に予期せぬエラーが発生しました: {e}")
    finally:
        # テスト後に生成されたDBファイルを削除する場合は、以下のコメントを解除
        import os
        if os.path.exists(sqlite_test_db_path):
            os.remove(sqlite_test_db_path)
            print(f"\nテスト用DBファイル '{sqlite_test_db_path}' を削除しました。")
        pass


# --- データ操作関連 ---
def migrate_data(
    source_engine, target_engine, source_table, target_table, column_map, chunksize=1000
):
    """
    指定されたカラムマッピングに基づいて、ソーステーブルからターゲットテーブルへデータを移行します。
    Pandas DataFrame を介してチャンクごとに処理を行います。

    Args:
        source_engine (sqlalchemy.engine.Engine): ソースデータベースのエンジン。
        target_engine (sqlalchemy.engine.Engine): ターゲットデータベースのエンジン。
        source_table (str): ソーステーブル名。
        target_table (str): ターゲットテーブル名。
        column_map (dict): {"ソースカラム名": "ターゲットカラム名", ...} の形式の辞書。
        chunksize (int, optional): 一度に処理する行数。デフォルトは1000。

    Returns:
        tuple: (bool, str) 移行の成否とメッセージ。
    """
    try:
        # 注意: この関数はターゲットテーブルの既存データを考慮しません (追記モード 'append' のみ)。
        # 必要に応じて、移行前にターゲットテーブルをクリアするなどの事前処理を検討してください。

        source_columns_to_select = list(column_map.keys())
        if not source_columns_to_select:
            return False, "マッピング定義にソースカラムが含まれていません。"

        # ソーステーブルから指定されたカラムのみを選択するSELECT文を構築
        select_query = f"SELECT {', '.join(source_columns_to_select)} FROM {source_table}"
        total_rows_migrated = 0

        # ソースデータベースからデータをチャンク単位で読み込み処理
        for chunk_df in pd.read_sql_query(select_query, source_engine, chunksize=chunksize):
            if chunk_df.empty: # チャンクが空ならスキップ
                continue

            # DataFrameのカラム名をマッピング定義に基づいてターゲットテーブル用に変更
            renamed_chunk_df = chunk_df.rename(columns=column_map)

            # ターゲットテーブルにデータを挿入 (既存データがある場合は追記)
            renamed_chunk_df.to_sql(
                target_table,
                target_engine,
                if_exists="append", # 'append', 'replace', 'fail' から選択
                index=False # DataFrameのインデックスはDBに書き込まない
            )
            total_rows_migrated += len(renamed_chunk_df)

        return True, f"{total_rows_migrated}行のデータをテーブル'{source_table}'から'{target_table}'へ移行しました。"
    except Exception as e:
        return False, f"データ移行中にエラーが発生しました: {e}"


def generate_insert_statement(table_name, data_dict):
    """
    指定されたテーブル名とデータの辞書から、SQLAlchemyで使用可能な
    パラメータ化されたINSERT文とそのパラメータを生成します。
    主に確認用や、SQLを手動で実行したい場合に使用します。

    Args:
        table_name (str): 挿入先のテーブル名。
        data_dict (dict): {"カラム名": 値, ...} の形式のデータ辞書。

    Returns:
        tuple: (str, dict) or (None, str)
                 成功時: (INSERT文文字列, パラメータ辞書)
                 失敗時: (None, エラーメッセージ)
    """
    if not data_dict:
        return None, "挿入データが空です。"

    columns = ", ".join(data_dict.keys())
    # SQLAlchemyのtext()で使用する :key 形式のプレースホルダを生成
    placeholders = ", ".join(f":{key}" for key in data_dict.keys())

    statement = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
    return statement, data_dict # 生成されたSQL文と、そのまま渡せるパラメータ辞書を返す


def insert_record(engine, table_name, data_dict):
    """
    指定されたテーブルに1レコードを挿入します。
    内部で generate_insert_statement を使用し、SQLAlchemy経由で実行します。

    Args:
        engine (sqlalchemy.engine.Engine): データベースエンジン。
        table_name (str): 挿入先のテーブル名。
        data_dict (dict): {"カラム名": 値, ...} の形式のデータ辞書。

    Returns:
        tuple: (bool, str) 挿入の成否とメッセージ。
    """
    if not data_dict:
        return False, "挿入するデータがありません。"

    statement, params = generate_insert_statement(table_name, data_dict)
    if not statement: # generate_insert_statementがエラーメッセージをparamsに返す場合がある
        return False, params

    try:
        with engine.connect() as connection:
            connection.execute(text(statement), params) # パラメータ化クエリとして実行
            connection.commit()
        return True, f"テーブル'{table_name}'に1レコードを挿入しました。"
    except Exception as e:
        return False, f"レコード挿入中にエラーが発生しました: {e}"
