"""
initial.py
系統初始化
1. 確認 .env 與 config.yaml 是否存在 (不存在就終止程式)
2. 確認 Dynomodb 是否存在指定 table
3. 若此 table 存在, 根據 ALLOW_RECREATE 參數去決定是否刪掉重建

20251022 取消該程式, 先行保留 by Hank
"""

import boto3
import yaml
import os
import logging
from botocore.exceptions import ClientError
from dotenv import load_dotenv

logger = logging.getLogger()
logger.setLevel(logging.INFO)

IS_LOCAL = os.getenv("IS_LOCAL", "true").lower() == "true"

_config_cache = None

def load_config():
    global _config_cache
    if _config_cache:
        return _config_cache

    if IS_LOCAL:
        env_path = os.path.join(os.path.dirname(__file__), "../../../.env")
        if os.path.exists(env_path):
            load_dotenv(env_path)
        config_path = os.path.join(os.path.dirname(__file__), "../../../config/config.yaml")
        if not os.path.exists(config_path):
            logger.error(f"Configuration file not found: {config_path}")
            raise FileNotFoundError(f"{config_path} not found")
        with open(config_path, "r") as f:
            _config_cache = yaml.safe_load(f) or {}
    else:
        S3_BUCKET = os.getenv("CONFIG_S3_BUCKET")
        S3_KEY = os.getenv("CONFIG_S3_KEY")
        if not S3_BUCKET or not S3_KEY:
            logger.error("CONFIG_S3_BUCKET or CONFIG_S3_KEY not set in Lambda environment variables")
            raise ValueError("Missing S3 config info")
        s3 = boto3.client("s3")
        obj = s3.get_object(Bucket=S3_BUCKET, Key=S3_KEY)
        _config_cache = yaml.safe_load(obj["Body"].read()) or {}

    return _config_cache

config = load_config()
TABLE_NAME = config.get("dynamodb_table_name", "download_paper_entry_id")
ALLOW_RECREATE = config.get("allow_recreate_table", False)

AWS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET = os.getenv("AWS_SECRET_ACCESS_KEY")
REGION = os.getenv("AWS_REGION")
dynamodb_args = {"region_name": REGION}
if AWS_KEY and AWS_SECRET:
    dynamodb_args.update({
        "aws_access_key_id": AWS_KEY,
        "aws_secret_access_key": AWS_SECRET
    })

dynamodb = boto3.resource("dynamodb", **dynamodb_args)

def lambda_handler(event, context):
    existing_tables = dynamodb.meta.client.list_tables()["TableNames"]

    if TABLE_NAME in existing_tables:
        logger.info(f"Table '{TABLE_NAME}' already exists.")
        if ALLOW_RECREATE:
            logger.info("Recreation allowed. Deleting existing table...")
            table = dynamodb.Table(TABLE_NAME)
            table.delete()
            waiter = dynamodb.meta.client.get_waiter("table_not_exists")
            waiter.wait(TableName=TABLE_NAME)
            logger.info("Existing table deleted successfully.")
        else:
            logger.info("Recreation not allowed. Keeping existing table. Exiting function.")
            return {"status": "exists", "message": "Table exists and recreation not allowed"}

    logger.info(f"Creating table '{TABLE_NAME}'...")
    table = dynamodb.create_table(
        TableName=TABLE_NAME,
        KeySchema=[
            {"AttributeName": "category", "KeyType": "HASH"},  # Partition key
            {"AttributeName": "entry_id", "KeyType": "RANGE"}  # Sort key
        ],
        AttributeDefinitions=[
            {"AttributeName": "category", "AttributeType": "S"},
            {"AttributeName": "entry_id", "AttributeType": "S"}
            ],
        BillingMode="PAY_PER_REQUEST"
    )

    waiter = dynamodb.meta.client.get_waiter("table_exists")
    waiter.wait(TableName=TABLE_NAME)
    logger.info(f"Table '{TABLE_NAME}' created successfully.")

    schema_info = (
        "Table schema:\n"
        "- Partition Key: entry_id (String)\n"
        "- Attributes:\n"
        "  - status: uploaded / failed\n"
        "  - last_attempt: ISO timestamp\n"
        "  - error_msg: optional, stores failure reason"
    )
    logger.info(schema_info)

    return {"status": "success", "table_name": TABLE_NAME}

if __name__ == "__main__":
    result = lambda_handler({}, {})
    print(result)
