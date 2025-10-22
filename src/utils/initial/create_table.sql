-- 建立 papers schema（若不存在則建立）
CREATE SCHEMA IF NOT EXISTS papers;

-- 建立 etl schema（若不存在則建立）
CREATE SCHEMA IF NOT EXISTS etl;

-- 建立 etl.raw_batches：用於記錄原始資料批次下載與 ETL 狀態
CREATE TABLE etl.raw_batches (
    batch_id varchar(50) PRIMARY KEY,
    category varchar(50) NOT NULL,
    s3_path text UNIQUE NOT NULL,
    record_count int NULL,
    downloaded_at timestamptz DEFAULT now(),
    etl_status varchar(20) DEFAULT 'pending',  -- pending, processing, success, failed
    etl_started_at timestamptz NULL,
    etl_finished_at timestamptz NULL,
    error_msg text NULL
);

-- 建立 papers.downloaded_papers：儲存下載成功或失敗的論文記錄
CREATE TABLE papers.downloaded_papers (
    entry_id varchar(50) PRIMARY KEY,          -- arXiv ID，例如 2501.12345v2
    category varchar(50) NOT NULL,             -- cs_AI, cs_LG, ...
    status varchar(20) NOT NULL DEFAULT 'downloaded',  -- downloaded, failed, etc.
    last_attempt timestamptz NULL,             -- 最後嘗試時間
    error_msg text NULL,                       -- 下載錯誤訊息
    etl_status varchar(20) DEFAULT 'pending',  -- pending, success, failed
    etl_batch_id varchar(50) NULL,             -- 對應 raw_batches 批次
    etl_processed_at timestamptz NULL,         -- ETL 完成時間
    CONSTRAINT downloaded_papers_etl_batch_fkey
        FOREIGN KEY (etl_batch_id)
        REFERENCES etl.raw_batches(batch_id)
        ON DELETE SET NULL
);

-- 建立 papers.category_progress：紀錄各領域的處理進度
CREATE TABLE papers.category_progress (
    category_name TEXT PRIMARY KEY,  -- 領域名稱
    status TEXT DEFAULT '',          -- NULL 或 'Finished'
    updated_at TIMESTAMP DEFAULT NOW()
);

-- 建立 papers.category_run_stats：記錄各領域執行時間與數據量統計
CREATE TABLE papers.category_run_stats (
    category_name TEXT PRIMARY KEY,
    time_sec FLOAT,
    s3_count INT,
    pg_count INT,
    updated_at TIMESTAMP DEFAULT NOW()
);

-- 建立 arxiv_papers：主表，存放論文的最新版本資訊
CREATE TABLE arxiv_papers (
    entry_id TEXT PRIMARY KEY,
    title TEXT NOT NULL,
    authors TEXT[] NOT NULL,
    affiliations JSONB,
    summary TEXT NOT NULL,
    primary_category TEXT NOT NULL,
    categories TEXT[],
    published TIMESTAMPTZ NOT NULL,
    updated TIMESTAMPTZ NOT NULL,
    journal_ref TEXT,
    doi TEXT,
    links JSONB,
    published_date DATE,
    updated_date DATE,
    etl_timestamp TIMESTAMPTZ DEFAULT now(),
    version INT DEFAULT 1,
    keywords TEXT[],       -- NLP 關鍵字
    topic TEXT,            -- 細分小領域
    s3_path TEXT
);

-- 建立索引：加速查詢 arxiv_papers
CREATE INDEX idx_category ON arxiv_papers (primary_category);
CREATE INDEX idx_published ON arxiv_papers (published_date);
CREATE INDEX idx_authors ON arxiv_papers USING GIN (authors);
CREATE INDEX idx_categories ON arxiv_papers USING GIN (categories);
CREATE INDEX idx_links ON arxiv_papers USING GIN (links);
CREATE INDEX idx_affiliations ON arxiv_papers USING GIN (affiliations);

-- 建立 arxiv_papers_history：用於紀錄歷史版本與 ETL 操作歷程
CREATE TABLE arxiv_papers_history (
    history_id TEXT PRIMARY KEY,         -- 唯一歷史紀錄ID，由 Python 生成
    entry_id TEXT NOT NULL,              -- 對應主表
    version BIGINT NOT NULL,             -- 版本
    etl_timestamp TIMESTAMPTZ DEFAULT now(),
    etl_stage TEXT,                      -- ETL 階段描述
    title TEXT,
    authors TEXT[],
    affiliations JSONB,
    summary TEXT,
    primary_category TEXT,
    categories TEXT[],
    published TIMESTAMPTZ,
    updated TIMESTAMPTZ,
    journal_ref TEXT,
    doi TEXT,
    links JSONB,
    keywords TEXT[],
    topic TEXT,
    s3_path TEXT,
    operation_type TEXT NOT NULL DEFAULT 'insert'  -- insert / update / delete
);
