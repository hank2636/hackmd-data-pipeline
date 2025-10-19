from fastapi import FastAPI, Response
from prometheus_client import Gauge, generate_latest, CollectorRegistry
from datetime import datetime
from src.core.db import get_pg
from loguru import logger

# === 初始化 FastAPI ===
app = FastAPI(title="ETL Metrics Server", version="1.0")

# === 初始化 DB 連線 ===
pg = get_pg()

# === 定義 Prometheus Metrics Registry ===
registry = CollectorRegistry()

etl_time_gauge = Gauge(
    "etl_category_time_sec",
    "每個領域執行所花的時間（秒）",
    ["category"],
    registry=registry,
)

etl_s3_gauge = Gauge(
    "etl_category_s3_count",
    "每個領域上傳到 S3 的資料筆數",
    ["category"],
    registry=registry,
)

etl_pg_gauge = Gauge(
    "etl_category_pg_count",
    "每個領域寫入 PostgreSQL 的資料筆數",
    ["category"],
    registry=registry,
)

etl_last_updated = Gauge(
    "etl_category_last_updated_timestamp",
    "每個領域最後更新時間（UNIX timestamp）",
    ["category"],
    registry=registry,
)


# === 從資料庫讀取最新指標 ===
def load_category_stats_from_db():
    stmt = """
        SELECT category_name, time_sec, s3_count, pg_count, updated_at
        FROM papers.category_run_stats
        ORDER BY updated_at DESC;
    """
    rows = []
    try:
        rows = pg.execute_query(stmt)
    except Exception as e:
        logger.error(f"Failed to query category_run_stats: {e}")
    return rows


# === /metrics endpoint ===
@app.get("/metrics")
def metrics():
    """
    提供 Prometheus 可抓取的指標
    """
    # 每次請求都重新載入最新資料
    registry.clear()

    rows = load_category_stats_from_db()
    if not rows:
        logger.warning("No metrics data found in DB")

    for row in rows:
        category = row.category_name
        time_sec = row.time_sec or 0
        s3_count = row.s3_count or 0
        pg_count = row.pg_count or 0
        updated_at = row.updated_at or datetime.utcnow()

        etl_time_gauge.labels(category=category).set(time_sec)
        etl_s3_gauge.labels(category=category).set(s3_count)
        etl_pg_gauge.labels(category=category).set(pg_count)
        etl_last_updated.labels(category=category).set(updated_at.timestamp())

    data = generate_latest(registry)
    return Response(content=data, media_type="text/plain; version=0.0.4")


# === 健康檢查 ===
@app.get("/healthz")
def health_check():
    return {"status": "ok", "message": "metrics server running"}


# === 本地執行 ===
if __name__ == "__main__":
    import uvicorn

    logger.info("Starting metrics server on port 2000...")
    uvicorn.run(app, host="0.0.0.0", port=2000)
