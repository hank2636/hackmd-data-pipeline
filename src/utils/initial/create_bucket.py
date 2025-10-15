import boto3
from dotenv import load_dotenv
import os

def create_s3_bucket_and_prefix(bucket_name: str, domain: str):
    env_path = os.path.join(os.path.dirname(__file__), "../../../.env")
    if not os.path.exists(env_path):
        raise FileNotFoundError(f".env not found at {env_path}")
    
    load_dotenv(env_path)

    region = os.getenv("AWS_REGION")
    s3 = boto3.client(
        "s3",
        region_name=region,
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY")
    )

    s3.create_bucket(
        Bucket=bucket_name,
        CreateBucketConfiguration={"LocationConstraint": region}
    )

    prefix = f"raw/domain={domain}/"
    s3.put_object(Bucket=bucket_name, Key=(prefix + ".keep"))

if __name__ == "__main__":
    create_s3_bucket_and_prefix("hackmd-paper-bucket", "cs_LG")
