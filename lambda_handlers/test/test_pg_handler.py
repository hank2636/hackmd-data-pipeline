import os

import psycopg2
import json

def test_postgres_connection():
    """
    測試連線到 PostgreSQL。
    成功時返回成功訊息，失敗時返回錯誤訊息。
    """
    try:
        # 從環境變數讀取資料庫連線資訊
        host = os.environ.get("POSTGRES_SERVER")
        port = os.environ.get("POSTGRES_PORT", 5432)
        dbname = os.environ.get("POSTGRES_DB")
        user = os.environ.get("POSTGRES_USER")
        password = os.environ.get("POSTGRES_PASSWORD")

        if not all([host, dbname, user, password]):
            error_message = "Postgres Test: FAILED! Missing one or more required environment variables."
            print(error_message)
            return {"status": "FAILED", "error": error_message}
            
        print(f"Postgres Test: Attempting to connect to {user}@{host}:{port}/{dbname}")
        
        # 建立連線
        conn = psycopg2.connect(
            host=host,
            port=port,
            dbname=dbname,
            user=user,
            password=password,
            # 加上連線超時設定，避免 Lambda 等待太久
            connect_timeout=10 
        )
        
        # 關閉連線
        conn.close()
        
        success_message = "Postgres Test: SUCCESS! Connection was successful."
        print(success_message)
        return {"status": "SUCCESS", "message": success_message}
        
    except Exception as e:
        # 捕捉所有可能的錯誤，例如 DNS 解析失敗、連線超時、認證失敗等
        error_message = f"Postgres Test: FAILED! Error: {str(e)}"
        print(error_message)
        return {"status": "FAILED", "error": error_message}

def lambda_handler(event, context):
    """
    Lambda 入口函式，執行 PostgreSQL 的連線測試。
    """
    print("--- Starting Connectivity Test ---")
    postgres_result = test_postgres_connection()
    print("--- Test Finished ---")
    
    response_body = {
        "postgres_connection": postgres_result
    }
    
    return {
        "statusCode": 200,
        "body": json.dumps(response_body, indent=4)
    }