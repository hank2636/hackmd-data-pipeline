import json
import logging
from src.etl import arxiv_etl

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    logger.info(f"--- ETL Lambda 執行開始 ---")
    
    try:
        arxiv_etl.run_lambda()
        
        logger.info("--- Lambda 執行成功 ---")
        return {
            "statusCode": 200,
            "body": json.dumps({"status": "ok"})
        }
    except Exception as e:
        logger.error("--- Lambda 執行有錯 ---")
        return {
            "statusCode": 500,
            "body": json.dumps({"status": "error", "message": str(e)})
        }
