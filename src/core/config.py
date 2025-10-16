from pathlib import Path
from dotenv import load_dotenv
from pydantic_settings import BaseSettings
import yaml
import os


PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
ENV_PATH = PROJECT_ROOT / ".env"
load_dotenv(dotenv_path=ENV_PATH)


CONFIG_YAML_PATH = PROJECT_ROOT / "config" / "config.yaml"
with open(CONFIG_YAML_PATH, "r", encoding="utf-8") as f:
    yaml_config = yaml.safe_load(f)

class Settings(BaseSettings):
    # Postgres
    POSTGRES_DB: str = os.getenv("POSTGRES_DB")
    POSTGRES_USER: str = os.getenv("POSTGRES_USER")
    POSTGRES_PASSWORD: str = os.getenv("POSTGRES_PASSWORD")
    POSTGRES_SERVER: str = os.getenv("POSTGRES_SERVER")
    POSTGRES_PORT: int = int(os.getenv("POSTGRES_PORT", 5432))

    # AWS
    AWS_REGION: str = os.getenv("AWS_REGION")
    AWS_ACCESS_KEY_ID: str = os.getenv("AWS_ACCESS_KEY_ID")
    AWS_SECRET_ACCESS_KEY: str = os.getenv("AWS_SECRET_ACCESS_KEY")
    S3_BUCKET: str = os.getenv("S3_BUCKET", "hackmd-paper-bucket")

    DYNAMODB_TABLE_NAME: str = yaml_config.get("dynamodb_table_name", "")
    ALLOW_RECREATE_TABLE: bool = yaml_config.get("allow_recreate_table", False)

    # arXiv
    MAX_RESULTS_GOAL: int = 1000
    BATCH_SIZE: int = 100

    class Config:
        env_file = ENV_PATH
        env_file_encoding = "utf-8"


settings = Settings()
