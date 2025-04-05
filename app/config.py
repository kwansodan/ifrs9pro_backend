from dotenv import load_dotenv
import os
from app.utils.db import convert_libpq_to_sqlalchemy

load_dotenv()  # Load environment variables from .env

class Settings:
    SECRET_KEY: str = os.getenv("SECRET_KEY")
    DEBUG: bool = os.getenv("DEBUG", "false").lower() == "true"
    INVITATION_EXPIRE_HOURS: int = int(os.getenv("INVITATION_EXPIRE_HOURS", "24"))
    ACCESS_TOKEN_EXPIRE_HOURS: int = int(os.getenv("ACCESS_TOKEN_EXPIRE_HOURS", "24"))
    
    @property
    def SQLALCHEMY_DATABASE_URL(self) -> str:
        """
        Returns the proper SQLAlchemy connection URL, converting from libpq format if needed.
        """
        db_url = os.getenv("AZURE_POSTGRESQL_CONNECTIONSTRING")
        if not db_url:
            # Fallback to the original env var if DATABASE_URL is not set
            db_url = os.getenv("SQLALCHEMY_DATABASE_URL")
            
        if db_url and (db_url.startswith("dbname=") or not db_url.startswith("postgresql://")):
            return convert_libpq_to_sqlalchemy(db_url)
        return db_url

settings = Settings()
