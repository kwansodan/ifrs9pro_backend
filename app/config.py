from dotenv import load_dotenv
import os

load_dotenv()  # Load environment variables from .env

class Settings:
    SECRET_KEY: str = os.getenv("SECRET_KEY")
    SQLALCHEMY_DATABASE_URL: str = os.getenv("SQLALCHEMY_DATABASE_URL")
    DATABASE_URL: str = os.getenv("DATABASE_URL")
    DEBUG: bool = os.getenv("DEBUG", "false").lower() == "true"
    INVITATION_EXPIRE_HOURS: int = os.getenv("INVITATION_EXPIRE_HOURS")
    ACCESS_TOKEN_EXPIRE_HOURS: int = os.getenv("ACCESS_TOKEN_EXPIRE_HOURS")
    
settings = Settings()
