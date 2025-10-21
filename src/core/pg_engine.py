from typing import Annotated, Any
from pathlib import Path
import psycopg2
import psycopg2.extras
from loguru import logger
from pydantic import BaseModel, Field
import os

# -------------------------------
# 環境設定讀取
# -------------------------------
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
ENV_PATH = PROJECT_ROOT / ".env"

# Lambda 上不存在 .env 就不 raise
if ENV_PATH.exists():
    from dotenv import load_dotenv
    load_dotenv(dotenv_path=ENV_PATH)

# 讀取環境變數（Lambda 或本機 .env 都能用）
POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_SERVER = os.getenv("POSTGRES_SERVER")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", 5432))

# -------------------------------
# PostgreSQL Engine
# -------------------------------
class PsqlEngine(BaseModel):
    dbname: Annotated[str, Field(default=POSTGRES_DB)]
    user: Annotated[str, Field(default=POSTGRES_USER)]
    password: Annotated[str, Field(default=POSTGRES_PASSWORD)]
    host: Annotated[str, Field(default=POSTGRES_SERVER)]
    port: Annotated[int, Field(default=POSTGRES_PORT)]
    conn: Annotated[Any, Field(default=None)]
    cursor: Annotated[Any, Field(default=None)]

    def model_post_init(self, context: Any):
        self.connect_db()

    def connect_db(self):
        self.conn = psycopg2.connect(
            dbname=self.dbname,
            user=self.user,
            password=self.password,
            host=self.host,
            port=self.port,
        )
        self.conn.set_session(autocommit=False)
        return self

    def re_connect(self):
        self.connect_db()

    def execute_cmd(
        self,
        stmt: str,
        params: tuple = None,
        cursor_factory=psycopg2.extras.NamedTupleCursor
    ) -> None:
        try:
            if not self.conn:
                self.re_connect()
            if not self.cursor:
                self.cursor = self.conn.cursor(cursor_factory=cursor_factory)
            if params:
                self.cursor.execute(stmt, params)
            else:
                self.cursor.execute(stmt)
            self.conn.commit()
        except Exception as e:
            logger.error(e)
            logger.error(f"Error sql statement: {stmt}")
            self.conn.rollback()
        finally:
            self.close_connect()

    def execute_query(
        self,
        stmt: str,
        cursor_factory=psycopg2.extras.NamedTupleCursor,
        first: bool = False,
    ) -> list[Any]:
        result = []
        try:
            if not self.conn:
                self.re_connect()
            if not self.cursor:
                self.cursor = self.conn.cursor(cursor_factory=cursor_factory)
            self.cursor.execute(stmt)
            result = self.cursor.fetchall()
            self.conn.commit()
        except Exception as e:
            logger.error(e)
            logger.error(f"Error sql statement: {stmt}")
            self.conn.rollback()
        finally:
            self.close_connect()
        return result[0] if first and result else result

    def insert_mogrify(self, table_name: str, values: list[tuple[Any, ...]]) -> None:
        try:
            if not self.conn:
                self.re_connect()
            if not self.cursor:
                self.cursor = self.conn.cursor()
            placeholders = ",".join(["%s"] * len(values[0]))
            args_str = ",".join(
                self.cursor.mogrify(f"({placeholders})", value).decode("utf-8")
                for value in values
            )
            self.cursor.execute(f"insert into {table_name} values {args_str} ON CONFLICT DO NOTHING;;")
            self.conn.commit()
        except Exception as e:
            # logger.error(e)
            logger.error(
                f"Error sql statement: insert into {table_name} values {args_str};"
            )
            self.conn.rollback()
        finally:
            self.close_connect()

    def close_connect(self) -> None:
        try:
            if self.cursor:
                self.cursor.close()
            if self.conn:
                self.conn.close()
            self.cursor = None
            self.conn = None
        except Exception as e:
            logger.error(e)
