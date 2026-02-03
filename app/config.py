from dotenv import load_dotenv
import os
from app.utils.db import convert_libpq_to_sqlalchemy
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
import os

load_dotenv()  # Load environment variables from .env
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '..', '.env'))


class Settings:
    SECRET_KEY: str = os.getenv("SECRET_KEY")
    DEBUG: bool = os.getenv("DEBUG", "false").lower() == "true"
    INVITATION_EXPIRE_HOURS: int = int(os.getenv("INVITATION_EXPIRE_HOURS", "24"))
    ACCESS_TOKEN_EXPIRE_HOURS: int = int(os.getenv("ACCESS_TOKEN_EXPIRE_HOURS", "24"))
    ACCESS_TOKEN_EXPIRE_MINUTES: int = int(os.getenv("ACCESS_TOKEN_EXPIRE_MINUTES", "30"))
    SQLALCHEMY_DATABASE_URL: str = os.getenv("SQLALCHEMY_DATABASE_URL")
    AZURE_STORAGE_CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    AZURE_STORAGE_ACCOUNT_KEY = os.getenv("AZURE_STORAGE_ACCOUNT_KEY")
    CONTAINER_NAME = os.getenv("CONTAINER_NAME")
    AZURE_STORAGE_ACCOUNT_NAME = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")
    MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
    MINIO_PUBLIC_ENDPOINT = os.getenv("MINIO_PUBLIC_ENDPOINT")
    MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
    MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
    MINIO_BUCKET_NAME = os.getenv("MINIO_BUCKET_NAME")
    CELERY_BROKER_URL = os.getenv("CELERY_BROKER_URL", "redis://redis:6379/0")
    CELERY_RESULT_BACKEND = os.getenv("CELERY_RESULT_BACKEND", "redis://redis:6379/1")
    PAYSTACK_PLAN_CORE = os.getenv("PAYSTACK_PLAN_CORE")
    PAYSTACK_PLAN_PROFESSIONAL = os.getenv("PAYSTACK_PLAN_PROFESSIONAL")
    PAYSTACK_PLAN_ENTERPRISE = os.getenv("PAYSTACK_PLAN_ENTERPRISE")


settings = Settings()
