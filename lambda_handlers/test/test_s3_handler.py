import boto3
import yaml
import json

# 初始化 S3 client
s3 = boto3.client("s3")

def test_s3_connection():
    """
    測試從 S3 下載設定檔。
    成功時返回設定內容，失敗時返回錯誤訊息。
    """
    try:
        bucket_name = "arvix-paper-bucket"
        key = "config/config.yaml"
        local_path = "/tmp/config.yaml"
        
        print("S3 Test: Attempting to download s3://{}/{}".format(bucket_name, key))
        s3.download_file(bucket_name, key, local_path)
        
        with open(local_path, "r") as f:
            config = yaml.safe_load(f)
        
        print("S3 Test: Success! Config loaded.")
        return {"status": "SUCCESS", "data": config}
    except Exception as e:
        error_message = f"S3 Test: FAILED! Error: {str(e)}"
        print(error_message)
        return {"status": "FAILED", "error": error_message}

def lambda_handler(event, context):
    """
    Lambda 入口函式，依序執行 S3 和 PostgreSQL 的連線測試。
    """
    print("--- Starting Connectivity Test ---")
    s3_result = test_s3_connection()
    print("--- Test Finished ---")
    
    # 組合最終的回應
    response_body = {
        "s3_connection": s3_result,
    }
    
    return {
        "statusCode": 200,
        "body": json.dumps(response_body, indent=4)
    }