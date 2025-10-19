import os
import boto3
import yaml

s3 = boto3.client("s3")

def load_config():
    """下載 S3 上的 config.yaml 到 Lambda /tmp 目錄，並讀取內容"""
    bucket_name = "arvix-paper-bucket"
    key = "config/config.yaml"
    local_path = "/tmp/config.yaml"

    s3.download_file(bucket_name, key, local_path)

    with open(local_path, "r") as f:
        config = yaml.safe_load(f)
    
    return config

def lambda_handler(event, context):
    """Lambda 入口"""
    config = load_config()
    print("Config content:", config)
    return {
        "statusCode": 200,
        "body": config
    }
