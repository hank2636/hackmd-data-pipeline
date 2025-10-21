import os
import json
from psycopg2.extras import Json
import gzip
import io
import boto3
import uuid
import yaml
import logging
from datetime import datetime, timezone
from src.core.db import get_pg
from src.core.pg_engine import PsqlEngine

BUCKET_NAME = os.getenv("BUCKET_NAME")
AWS_LAMBDA_FUNCTION_NAME = os.getenv("AWS_LAMBDA_FUNCTION_ETL")

s3 = boto3.client("s3")

def load_config(bucket_name: str):
    key = "config/config.yaml"
    local_path = "/tmp/config.yaml"
    try:
        s3.download_file(bucket_name, key, local_path)
        with open(local_path, "r") as f:
            config = yaml.safe_load(f)
        return config
    except Exception as e:
        logging.error(f"Failed to load config from S3: {e}")
        raise

cfg = load_config(BUCKET_NAME)

PENDING_GZ_BATCH = cfg['etl']['pending_gz_batch']
ETL_BATCH_SIZE = cfg['etl']['etl_batch_size']


pg = get_pg()

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

def get_pending_gz(pg: PsqlEngine, num: int):
    stmt = f"""
        UPDATE etl.raw_batches
        SET etl_status = 'processing',
            etl_started_at = now()
        WHERE batch_id IN (
            SELECT batch_id
            FROM etl.raw_batches
            WHERE etl_status = 'pending'
            ORDER BY batch_id
            FOR UPDATE SKIP LOCKED
            LIMIT {num}
        )
        RETURNING s3_path, category;
    """
    return pg.execute_query(stmt)

def parse_record(record: dict, s3_key: str):
    published_date = record.get("published")
    updated_date = record.get("updated")
    if published_date:
        published_date = datetime.fromisoformat(published_date).date()
    if updated_date:
        updated_date = datetime.fromisoformat(updated_date).date()
    return (
        record.get("entry_id"),
        record.get("title"),
        record.get("authors", []),
        json.dumps({}),
        record.get("summary"),
        record.get("primary_category"),
        record.get("categories", []),
        record.get("published"),
        record.get("updated"),
        record.get("journal_ref"),
        record.get("doi"),
        json.dumps({}),
        published_date,
        updated_date,
        datetime.now(timezone.utc),
        1,
        [],
        None,
        s3_key
    )

def parse_history_record(record: dict, s3_key: str, operation: str, etl_stage: str):
    summary = (record.get("summary") or "").replace('\x00', '').replace('\n', ' ').replace('\r', ' ')
    return (
        str(uuid.uuid4()),
        record.get("entry_id"),
        datetime.now(timezone.utc).timestamp(),
        datetime.now(timezone.utc),
        etl_stage,
        record.get("title"),
        record.get("authors", []),
        Json({}),
        summary,
        record.get("primary_category"),
        record.get("categories", []),
        record.get("published"),
        record.get("updated"),
        record.get("journal_ref"),
        record.get("doi"),
        Json({}),
        [],
        None,
        s3_key,
        operation
    )

def safe_insert(table: str, batch: list):
    try:
        pg.insert_mogrify(table, batch)
    except Exception as e:
        logger.error(f"Batch insert failed: {e}")
        for row in batch:
            try:
                pg.insert_mogrify(table, [row])
            except Exception as e2:
                logger.error(f"Single insert failed for row {row[0]}: {e2}")
        return False
    return True

def update_etl_status(pg: PsqlEngine, s3_path: str, status: str, started_at=None, finished_at=None, error_msg=None):
    stmt = """
        update etl.raw_batches
        set etl_status = %s,
            etl_started_at = coalesce(%s, etl_started_at),
            etl_finished_at = coalesce(%s, etl_finished_at),
            error_msg = %s
        where s3_path = %s;
    """
    params = (status, started_at, finished_at, error_msg, s3_path)
    pg.execute_cmd(stmt, params)

def load_s3_gzip_to_pg(bucket: str, s3_key: str, etl_stage: str = "initial_load"):
    obj = s3.get_object(Bucket=bucket, Key=s3_key)
    batch, batch_history = [], []

    with gzip.GzipFile(fileobj=io.BytesIO(obj["Body"].read()), mode="rb") as f:
        for line in f:
            record = json.loads(line.decode("utf-8"))
            batch.append(parse_record(record, s3_key))
            batch_history.append(parse_history_record(record, s3_key, "insert", etl_stage))
            if len(batch) >= ETL_BATCH_SIZE:
                safe_insert("arxiv_papers", batch)
                safe_insert("arxiv_papers_history", batch_history)
                batch, batch_history = [], []

    if batch:
        safe_insert("arxiv_papers", batch)
        safe_insert("arxiv_papers_history", batch_history)
    return datetime.now(timezone.utc)

def invoke_next_lambda():
    """Call Lambda 把剩下的做完"""
    lambda_client = boto3.client("lambda")
    try:
        response = lambda_client.invoke(
            FunctionName=AWS_LAMBDA_FUNCTION_NAME,
            InvocationType="Event",
            Payload=json.dumps({"trigger": "auto"}).encode()
        )
        logger.info(f"Successfully invoked next Lambda: {response['StatusCode']}")
    except Exception as e:
        logger.error(f"Failed to invoke next Lambda: {e}", exc_info=True)


def get_pending_gz_count(pg: PsqlEngine):
    """取得還沒做完的 GZ 檔案數"""
    stmt = """
        SELECT COUNT(*) AS cnt
        FROM etl.raw_batches
        WHERE etl_status = 'pending';
    """
    result = pg.execute_query(stmt)
    return result[0].cnt if result else 0

def run_lambda():
    processed = []
    pending_gz = get_pending_gz(pg, PENDING_GZ_BATCH) # 狀態會改為 "processing"
    pending_gz = [r.__dict__ if hasattr(r, "__dict__") else dict(r._asdict()) for r in pending_gz]

    for pending_gz_dict in pending_gz:
        key = pending_gz_dict['s3_path']
        logger.info(f"Processing {key}")
        try:
            finished_at = load_s3_gzip_to_pg(BUCKET_NAME, key)
            update_etl_status(pg, key, "finished", finished_at=finished_at)
            processed.append(key)
        except Exception as e:
            logger.error(f"Error processing {key}: {e}", exc_info=True)
            update_etl_status(pg, key, "failed", finished_at=datetime.now(timezone.utc), error_msg=str(e))
    remaining = get_pending_gz_count(pg)
    logger.info(f"剩餘待處理 GZ 數量: {remaining}")
    if remaining > 0:
        logger.info("還有檔案沒抓取，觸發下一個 Lambda")
        invoke_next_lambda()
    else:
        logger.info("已完成所有檔案")

    return processed
