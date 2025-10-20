import json
import logging
from src.extract import arxiv_collector

# 設定日誌級別
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    """ Lambda 入口，負責觸發爬蟲任務 """
    logger.info(f"--- Lambda 執行開始 ---")
    logger.info(f"請求 ID: {context.aws_request_id}")
    logger.info(f"收到的事件: {json.dumps(event)}")
    
    try:
        # 注意：原始程式碼這裡的 run_lambda 不接受參數，但您的 handler 傳入了。
        # 這裡暫時移除參數以匹配 run_lambda() 的定義。
        # 如果您確實需要傳遞 categories，請修改 arxiv_collector.py 中的 run_lambda 定義。
        # categories = event.get("categories") 
        arxiv_collector.run_lambda()
        
        logger.info("--- Lambda 執行成功結束 ---")
        return {
            "statusCode": 200,
            "body": json.dumps({"status": "ok"})
        }
    except Exception as e:
        # 捕捉來自 arxiv_collector 的任何未處理異常
        logger.exception(f"在 handler 層捕捉到未處理的錯誤: {e}")
        logger.error("--- Lambda 執行因錯誤而終止 ---")
        return {
            "statusCode": 500,
            "body": json.dumps({"status": "error", "message": str(e)})
        }
