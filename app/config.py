from dotenv import load_dotenv
import os
import urllib.parse
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
        
        if db_url:
            # If the URL contains encoded characters, decode them
            if '%' in db_url and db_url.startswith("postgresql://"):
                # Parse the URL into components
                parsed = urllib.parse.urlparse(db_url)
                
                # Extract and decode username and password
                userinfo = parsed.netloc.split('@')[0]
                if ':' in userinfo:
                    username, password = userinfo.split(':', 1)
                    # Decode the password
                    decoded_password = urllib.parse.unquote(password)
                    
                    # Reconstruct the URL with decoded password
                    netloc_parts = parsed.netloc.split('@')
                    new_netloc = f"{username}:{decoded_password}@{netloc_parts[1]}"
                    
                    # Rebuild the URL
                    db_url = urllib.parse.urlunparse((
                        parsed.scheme,
                        new_netloc,
                        parsed.path,
                        parsed.params,
                        parsed.query,
                        parsed.fragment
                    ))
            
            # Convert libpq format if needed
            if db_url.startswith("dbname=") or not db_url.startswith("postgresql://"):
                return convert_libpq_to_sqlalchemy(db_url)
                
        return db_url

settings = Settings()
