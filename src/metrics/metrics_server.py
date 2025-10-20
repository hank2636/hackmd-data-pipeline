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

# --- 日誌設定 ---
# 移除其他函式庫的冗長日誌，專注於應用程式日誌
logging.getLogger("boto3").setLevel(logging.WARNING)
logging.getLogger("botocore").setLevel(logging.WARNING)
logging.getLogger("arxiv").setLevel(logging.WARNING)
# 取得根日誌記錄器
logger = logging.getLogger()
logger.setLevel(logging.INFO)


# --- 初始化 (保持不變) ---
s3 = boto3.client("s3")

def load_config():
    logger.info("開始從 S3 載入 config.yaml...")
    bucket_name = "arvix-paper-bucket"
    key = "config/config.yaml"
    local_path = "/tmp/config.yaml"
    try:
        s3.download_file(bucket_name, key, local_path)
        with open(local_path, "r") as f:
            config = yaml.safe_load(f)
        logger.info("成功從 S3 載入設定。")
        return config
    except Exception as e:
        logger.exception(f"從 S3 載入設定失敗: {e}")
        raise

# --- 全域變數和函式 (加上日誌) ---

try:
    logger.info("正在初始化設定...")
    pg = get_pg()
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
    logger.info("設定和 arXiv 客戶端初始化完成。")
except Exception as e:
    logger.exception("在全域初始化階段發生致命錯誤。")
    # 讓 Lambda 因初始化失敗而終止
    raise

# (此處省略了其他輔助函式，它們的邏輯保持不變，但在被呼叫時會有日誌)
# flatten_categories, select_next_categories 等...

def get_existing_categories():
    logger.info("正在從 DB 讀取已存在的領域...")
    stmt = "SELECT category_name, status FROM papers.category_progress"
    rows = pg.execute_query(stmt)
    logger.info(f"從 DB 找到 {len(rows)} 個領域。")
    return {r[0]: r[1] for r in rows}

def insert_new_categories(new_cats):
    if not new_cats: return
    logger.info(f"正在將新領域寫入 DB: {new_cats}")
    values = [(cat, '') for cat in new_cats]
    pg.insert_mogrify("papers.category_progress", values)
    logger.info("新領域寫入完成。")

def get_pending_categories():
    logger.info("正在從 DB 讀取待處理的領域...")
    stmt = "SELECT category_name FROM papers.category_progress WHERE status != 'Finished'"
    rows = pg.execute_query(stmt)
    logger.info(f"找到 {len(rows)} 個待處理的領域。")
    return [r[0] for r in rows]

def mark_category_finished(category):
    logger.info(f"正在將領域 '{category}' 標記為完成...")
    stmt = "UPDATE papers.category_progress SET status = 'Finished', updated_at = NOW() WHERE category_name = %s"
    pg.execute_cmd(stmt, (category,))
    logger.info(f"領域 '{category}' 狀態更新完成。")

def load_existing_ids(months: int = LOOKBACK_MONTHS):
    logger.info(f"正在從 DB 載入過去 {months} 個月的論文 ID...")
    stmt = f"SELECT entry_id FROM papers.downloaded_papers WHERE last_attempt >= NOW() - INTERVAL '{months} months'"
    rows = pg.execute_query(stmt)
    existing_ids = set(r[0] for r in rows)
    logger.info(f"載入 {len(existing_ids)} 個現存論文 ID。")
    return existing_ids

def flush_pg_batch(pg_batch):
    if not pg_batch: return
    logger.info(f"正在將 {len(pg_batch)} 筆記錄批次寫入 PostgreSQL...")
    try:
        pg.insert_mogrify("papers.downloaded_papers", pg_batch)
        logger.info("PostgreSQL 批次寫入成功。")
    except Exception as e:
        logger.exception(f"PostgreSQL 批次寫入失敗: {e}")
    finally:
        pg_batch.clear() # 使用 clear() 更安全

def upload_batch_to_s3(s3_prefix, batch_data, batch_num, category):
    if not batch_data: return None
    batch_size = len(batch_data)
    logger.info(f"[{category}] 正在將第 {batch_num} 批次 ({batch_size} 筆記錄) 上傳到 S3...")
    # ... (上傳邏輯保持不變) ...
    # ...
    s3_key = f"{s3_prefix}{datetime.now(timezone.utc).strftime('%Y-%m-%d')}/{category.replace('.','_')}_batch_{batch_num}_{int(datetime.now(timezone.utc).timestamp())}.jsonl.gz"
    # ... (重試邏輯保持不變) ...
    # ...
    logger.info(f"[{category}] 第 {batch_num} 批次成功上傳到 S3 Key: {s3_key}")
    return s3_key


# === 主要執行邏輯 ===
def run_lambda():
    logger.info("run_lambda: ===== 開始執行爬蟲任務 =====")
    
    num_per_run = cfg["lambda"]["num_categories_per_run"]
    
    # 步驟 1: 同步 YAML 和 DB 中的領域列表
    all_categories = flatten_categories(cfg)
    existing_cats = get_existing_categories()
    new_cats = [cat for cat in all_categories if cat not in existing_cats]
    if new_cats:
        insert_new_categories(new_cats)

    # 步驟 2: 取得本次要處理的領域
    pending_cats = get_pending_categories()
    if not pending_cats:
        logger.info("所有領域皆已完成，任務結束。")
        return {"status": "finished"}
    
    category_list = select_next_categories(pending_cats, num_per_run)
    logger.info(f"本次執行的領域列表: {category_list}")
    
    # 步驟 3: 載入已存在的論文 ID 以便去重
    existing_ids = load_existing_ids()
    category_stats = {}
    
    # 步驟 4: 遍歷並處理每個領域
    for category in category_list:
        logger.info(f"[{category}] ===== 開始處理領域 =====")
        start_time = time.time()
        s3_count, pg_count, total_count = 0, 0, 0
        pg_batch = []
        
        try:
            S3_PREFIX = f"raw/{category.replace('.','_')}/"
            logger.info(f"[{category}] 準備 arXiv 搜尋條件...")
            search = arxiv.Search(
                query=f'cat:{category}',
                max_results=MAX_RESULTS_GOAL,
                sort_by=arxiv.SortCriterion.SubmittedDate,
                sort_order=arxiv.SortOrder.Descending
            )
            
            logger.info(f"[{category}] 搜尋條件準備完成，開始從 API 獲取結果...")
            batch_num = 0
            
            # 使用 offset 進行分頁，而非 while True
            for offset in range(0, MAX_RESULTS_GOAL, BATCH_SIZE):
                logger.info(f"[{category}] 正在呼叫 client.results()，offset: {offset}")
                
                # !!! Timeout 最可能發生在這裡 !!!
                results_generator = client.results(search, offset=offset)
                
                logger.info(f"[{category}] API 呼叫成功，開始處理回傳的論文...")
                
                batch_data = []
                batch_ids = set()
                
                # 只處理一個 batch 的量
                papers_in_page = 0
                for paper_result in results_generator:
                    entry_id = paper_result.entry_id
                    if entry_id in existing_ids or entry_id in batch_ids:
                        continue
                    
                    # (處理 paper_data 的邏輯保持不變)
                    # ...
                    
                    batch_data.append(paper_data)
                    batch_ids.add(entry_id)
                    existing_ids.add(entry_id) # 全域去重
                    pg_count += 1
                    
                    papers_in_page += 1
                    if papers_in_page >= BATCH_SIZE:
                        break # 已滿一個 batch，跳出內層迴圈

                if not batch_data:
                    logger.info(f"[{category}] 在 offset {offset} 未找到新論文，結束此領域。")
                    break # 如果 API 回傳空頁面，結束此領域的處理

                logger.info(f"[{category}] 處理完畢 {len(batch_data)} 篇論文，準備上傳和寫入 DB。")
                
                # 上傳 S3
                now_s3_key = upload_batch_to_s3(S3_PREFIX, batch_data, batch_num, category)
                if now_s3_key:
                    s3_count += len(batch_data)
                    # (更新 pg_batch 狀態和寫入 raw_batches 的邏輯保持不變)
                    # ...
                
                # 寫入 PG
                flush_pg_batch(pg_batch)
                
                batch_num += 1

            # 處理完一個領域
            mark_category_finished(category)
            elapsed = time.time() - start_time
            category_stats[category] = {"time_sec": elapsed, "s3_count": s3_count, "pg_count": pg_count}
            logger.info(f"[{category}] ===== 領域處理完成，耗時: {elapsed:.2f} 秒 =====")

        except Exception as e:
            logger.exception(f"[{category}] 處理過程中發生錯誤: {e}")
            # 即使出錯，也繼續處理下一個領域
            continue
            
    # 步驟 5: 記錄統計數據並結束
    if category_stats:
        logger.info("正在將本次執行的統計資料寫入 DB...")
        # insert_category_stats(category_stats) # 假設此函式存在
        logger.info("統計資料寫入完成。")
        for cat, stats in category_stats.items():
            logger.info(f"統計 -> {cat} | 耗時: {stats['time_sec']:.2f}s, S3 筆數: {stats['s3_count']}, PG 筆數: {stats['pg_count']}")

    logger.info("run_lambda: ===== 所有任務完成 =====")
