import json
import logging
from src.extract import arxiv_collector

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    logger.info(f"--- Lambda 執行開始 ---")
    logger.info(f"請求 ID: {context.aws_request_id}")
    logger.info(f"收到的事件: {json.dumps(event)}")
    
    try:
        arxiv_collector.run_lambda()
        
        logger.info("--- Lambda 執行成功結束 ---")
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
