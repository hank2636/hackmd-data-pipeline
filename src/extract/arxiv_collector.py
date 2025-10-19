import arxiv
import json
import time
import logging
import os
import gzip
import io
import boto3
import yaml
from datetime import datetime, timezone
from src.core.db import get_pg
from src.core.pg_engine import PsqlEngine

pg = get_pg()

logging.getLogger("boto3").setLevel(logging.WARNING)
logging.getLogger("botocore").setLevel(logging.WARNING)
logging.getLogger("arxiv").setLevel(logging.WARNING)

s3 = boto3.client("s3")

def load_config():
    bucket_name = "hackmd-paper-bucket"
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

cfg = load_config()
    
MAX_RESULTS_GOAL = cfg["source_papers"]["max_results_goal"]
BATCH_SIZE = cfg["source_papers"]["batch_size"]
S3_BUCKET = cfg["aws"]["s3_bucket"]
MAX_ATTEMPTS = cfg["source_papers"]["s3_max_attempts"]
INITIAL_DELAY_SECONDS = cfg["source_papers"]["initial_delay_seconds"]
LOOKBACK_MONTHS = cfg["source_papers"]["lookback_months"]

client = arxiv.Client(
    page_size=MAX_RESULTS_GOAL, 
    delay_seconds=3,
    num_retries=3
)

pg_batch = []




def flatten_categories(cfg):
    """所有領域的 list"""
    cats = []
    for _, sub in cfg["categories"].items():
        cats.extend(sub)
    return cats

def select_next_categories(pending_categories, per_run):
    """
    從待抓領域列表中選出本次要抓的 batch
    pending_categories: list, 還沒完成的領域
    per_run: int, 每次 Lambda 要抓多少個領域
    return : batch_list
    """
    if not pending_categories:
        return []
    return pending_categories[:per_run]

def get_existing_categories():
    """讀取 DB 中已存在的所有領域"""
    stmt = "SELECT category_name, status FROM papers.category_progress"
    rows = pg.execute_query(stmt)
    return {r[0]: r[1] for r in rows}

def insert_new_categories(new_cats):
    """把 YAML 新增的領域寫進 DB"""
    if not new_cats:
        return
    values = [(cat, '') for cat in new_cats]
    pg.insert_mogrify("papers.category_progress", values)

def get_pending_categories():
    """取得還沒做完的領域"""
    stmt = "SELECT category_name FROM papers.category_progress WHERE status != 'Finished'"
    rows = pg.execute_query(stmt)
    return [r[0] for r in rows]

def mark_category_finished(category):
    """完成一個領域後更新狀態"""
    stmt = """
        UPDATE papers.category_progress
        SET status = 'Finished', updated_at = NOW()
        WHERE category_name = %s
    """
    pg.execute_cmd(stmt, (category,))
  
def insert_category_stats(category_stats):
    """
    將 category_stats 批次寫入 papers.category_run_stats
    使用 insert_mogrify
    """
    if not category_stats:
        return

    utc_now = datetime.now(timezone.utc)
    values = []
    for cat, stats in category_stats.items():
        values.append((
            cat,
            stats["time_sec"],
            stats["s3_count"],
            stats["pg_count"],
            utc_now
        ))

    pg.insert_mogrify("papers.category_run_stats", values)
    
def load_existing_ids(months: int = LOOKBACK_MONTHS):
    """
    只抓最近 N 個月內的 entry_id, 防 lambda 記憶體不足
    預設 6 個月, 在 config.yaml 裡面有設
    """
    stmt = f"""
        SELECT entry_id
        FROM papers.downloaded_papers
        WHERE last_attempt >= NOW() - INTERVAL '{months} months'
    """
    rows = pg.execute_query(stmt)
    return set(r[0] for r in rows)


def add_to_pg_batch(pg_batch, entry_id, category, status, etl_status, etl_batch_id=None, error_msg=""):
    now_utc = datetime.now(timezone.utc)
    pg_batch.append((entry_id, category, status, now_utc, error_msg, etl_status, etl_batch_id))


def flush_pg_batch(pg_batch):
    if not pg_batch:
        return
    try:
        pg.insert_mogrify("papers.downloaded_papers", pg_batch)
    except Exception as e:
        logging.error(f"Failed to insert batch into Postgres: {e}")
    finally:
        pg_batch = []
        
def add_raw_batches_to_pg(batch_id, category, s3_path, record_count, ):
    stmt = """
        INSERT INTO etl.raw_batches (batch_id, category, s3_path, record_count)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (batch_id) DO NOTHING;
    """
    try:
        pg.execute_cmd(stmt, (batch_id, category, s3_path, record_count))
    except Exception as e:
        logging.error(f"Failed to insert into raw_batches: {e}")

def upload_batch_to_s3(s3_prefix, batch_data, batch_num, category):
    if not batch_data:
        return
    jsonl_content = "\n".join([json.dumps(paper, ensure_ascii=False) for paper in batch_data])
    buffer = io.BytesIO()
    with gzip.GzipFile(fileobj=buffer, mode='wb') as f:
        f.write(jsonl_content.encode('utf-8'))
    gzip_bytes = buffer.getvalue()
    
    utc_now = datetime.now(timezone.utc)
    today_str = utc_now.strftime("%Y-%m-%d")
    utc_timestamp = int(utc_now.timestamp())
    s3_key = f"{s3_prefix}{today_str}/{category.replace('.','_')}_batch_{batch_num}_{utc_timestamp}.jsonl.gz"
    
    last_exception = None
    for attempt in range(MAX_ATTEMPTS):
        try:
            s3.put_object(
                Bucket=S3_BUCKET,
                Key=s3_key,
                Body=gzip_bytes,
                ContentType='application/json',
                ContentEncoding='gzip'
            )
            break
        except Exception as e:
            last_exception = e
            if attempt < MAX_ATTEMPTS - 1:
                time.sleep(INITIAL_DELAY_SECONDS * (2 ** attempt))
            else:
                raise last_exception
    return s3_key


def run_lambda():
    """
    Lambda 入口
    """
    
    num_per_run = cfg["lambda"]["num_categories_per_run"]
    all_categories = flatten_categories(cfg)  # YAML 裡所有領域
    existing_cats = get_existing_categories()

    new_cats = [cat for cat in all_categories if cat not in existing_cats]
    if new_cats:
        insert_new_categories(new_cats)
        logging.info(f"新增領域到 DB: {new_cats}")


    pending_cats = get_pending_categories()
    if not pending_cats:
        logging.info("All categories are finished.")
        return {"status": "finished"}

    category_list = select_next_categories(pending_cats, num_per_run)

    logging.info(f"這次執行的領域：{category_list}")
        
    existing_ids = load_existing_ids()
    category_stats = {}
    for category in category_list:
        start_time = time.time()
        s3_count = 0
        pg_count = 0
        try:
            S3_PREFIX = f"raw/{category.replace('.','_')}/"
            search = arxiv.Search(
                query=f'cat:{category}',
                max_results=MAX_RESULTS_GOAL,
                sort_by=arxiv.SortCriterion.SubmittedDate,
                sort_order=arxiv.SortOrder.Descending
            )
            logging.info(f"{category}: Started")
            batch = []
            batch_ids = set()
            pg_batch = []
            batch_count = 0
            total_count = 0
            while True:
                try:
                    results_generator = client.results(search, offset=total_count)
                    for paper_result in results_generator:
                        entry_id = paper_result.entry_id
                        if entry_id in existing_ids or entry_id in batch_ids:
                            # logging.info(f"Skipping duplicate: {entry_id}")
                            continue
                        existing_ids.add(entry_id)
                        batch_ids.add(entry_id)
                        paper_data = {
                            "entry_id": entry_id,
                            "title": paper_result.title,
                            "authors": [a.name for a in paper_result.authors],
                            "summary": paper_result.summary,
                            "primary_category": paper_result.primary_category,
                            "categories": paper_result.categories,
                            "published": paper_result.published.isoformat(),
                            "updated": paper_result.updated.isoformat(),
                            "journal_ref": paper_result.journal_ref,
                            "doi": paper_result.doi
                        }
                        utc_now = datetime.now(timezone.utc)
                        today_str = utc_now.strftime("%Y-%m-%d")
                        batch.append(paper_data)
                        total_count += 1
                        etl_batch_id = f"{category.replace('.','_')}_{today_str}_batch_{batch_count}"
                        
                        add_to_pg_batch(pg_batch, entry_id, category, "pending", "pending", etl_batch_id)
                        pg_count += 1
                        if len(batch) >= BATCH_SIZE:
                            # 上傳至 S3
                            now_s3_key = upload_batch_to_s3(S3_PREFIX, batch, batch_count, category)
                            s3_count += len(batch)
                            for i in range(len(pg_batch)):
                                pg_batch[i] = (pg_batch[i][0], pg_batch[i][1], "uploaded", pg_batch[i][3], pg_batch[i][4], pg_batch[i][5], pg_batch[i][6])
                                
                            # 寫入 ETL raw_batches 表
                            add_raw_batches_to_pg(etl_batch_id, category, now_s3_key, len(batch))
                            # 批次推送到 PG
                            flush_pg_batch(pg_batch)
                            batch = []
                            pg_batch = []
                            batch_count += 1
                    break
                except arxiv.UnexpectedEmptyPageError as e:
                    logging.error(f"Error fetching results at offset {total_count}, ignore..., detail: {e}")
                    total_count += 1
                    continue
            if batch:
                now_s3_key = upload_batch_to_s3(S3_PREFIX, batch, batch_count, category)
                s3_count += len(batch)
                for i in range(len(pg_batch)):
                    pg_batch[i] = (pg_batch[i][0], pg_batch[i][1], "uploaded", pg_batch[i][3], pg_batch[i][4], pg_batch[i][5], pg_batch[i][6])
                add_raw_batches_to_pg(etl_batch_id, category, now_s3_key, len(batch))
                flush_pg_batch(pg_batch)
                logging.info(etl_batch_id)
            elapsed = time.time() - start_time
            category_stats[category] = {"time_sec": elapsed, "s3_count": s3_count, "pg_count": pg_count}
        except Exception as e:
            logging.error(f"Error during category {category}: {e}")
        mark_category_finished(category)
        logging.info(f"{category} -> Finished")

    for cat, stats in category_stats.items():
        logging.info(f"{cat} -> Time: {stats['time_sec']:.2f}s, S3: {stats['s3_count']}, PostgreSQL: {stats['pg_count']}")

    # 批次寫入各領域統計資料
    insert_category_stats(category_stats)

    remaining = get_pending_categories()
    if remaining:
        logging.info("尚有領域未完成，觸發下一個 Lambda")
    else:
        logging.info("所有領域完成，Lambda 不再觸發")