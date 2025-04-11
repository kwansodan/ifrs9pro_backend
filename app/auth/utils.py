from datetime import datetime, timedelta
from typing import Optional
from jose import JWTError, jwt
from passlib.context import CryptContext
from fastapi import Depends, HTTPException, status, WebSocket, Query
from fastapi.security import OAuth2PasswordBearer
from sqlalchemy.orm import Session
from app.database import get_db
from app.models import User
from app.schemas import TokenData
import os
from dotenv import load_dotenv
from app import models
from app.config import settings


ALGORITHM = "HS256"

pwd_context = CryptContext(schemes=["argon2"], deprecated="auto")
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")


def verify_password(plain_password, hashed_password):
    return pwd_context.verify(plain_password, hashed_password)


def get_password_hash(password):
    return pwd_context.hash(password)


def create_access_token(data: dict, expires_delta: Optional[timedelta] = None):
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(
            minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES
        )
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, settings.SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt


def create_email_verification_token(email: str):
    to_encode = {"sub": email, "type": "email_verification"}
    expire = datetime.utcnow() + timedelta(hours=24)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, settings.SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt


def get_token_from_query_param(websocket: WebSocket) -> Optional[str]:
    """
    Extract token from WebSocket query parameters.
    Returns None if no token is found.
    """
    query_params = websocket.scope.get("query_string", b"").decode()
    if not query_params:
        return None
    
    # Parse query parameters
    params = {}
    for param in query_params.split("&"):
        if "=" in param:
            key, value = param.split("=", 1)
            params[key] = value
    
    # Look for token parameter
    return params.get("token")


def verify_token(token: str) -> dict:
    """
    Verify JWT token and return payload.
    Raises jwt.PyJWTError if token is invalid.
    """
    payload = jwt.decode(token, settings.SECRET_KEY, algorithms=[ALGORITHM])
    return payload


def create_invitation_token(email: str):
    to_encode = {"sub": email, "type": "invitation"}
    expire = datetime.utcnow() + timedelta(hours=settings.INVITATION_EXPIRE_HOURS)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, settings.SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt


def decode_token(token: str):
    try:
        payload = jwt.decode(token, settings.SECRET_KEY, algorithms=[ALGORITHM])
        email: str = payload.get("sub")
        exp: datetime = datetime.fromtimestamp(payload.get("exp"))
        token_type: str = payload.get("type", "access")

        if email is None:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token"
            )

        if datetime.utcnow() > exp:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED, detail="Token expired"
            )

        token_data = TokenData(email=email, exp=exp)
        return token_data, token_type
    except JWTError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token"
        )


def get_current_user(
    token: str = Depends(oauth2_scheme), db: Session = Depends(get_db)
):
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        token_data, token_type = decode_token(token)
        if token_type != "access":
            raise credentials_exception
    except JWTError:
        raise credentials_exception

    user = db.query(User).filter(User.email == token_data.email).first()
    if user is None:
        raise credentials_exception
    return user


def get_current_active_user(current_user: User = Depends(get_current_user)):
    if not current_user.is_active:
        raise HTTPException(status_code=400, detail="Inactive user")
    return current_user


def is_admin(current_user: User = Depends(get_current_active_user)):
    if current_user.role != models.UserRole.ADMIN:
        raise HTTPException(status_code=403, detail="Not enough permissions")
    return current_user


async def get_current_active_user_ws(
    websocket: WebSocket, 
    token: str = Query(...),
    db: Session = Depends(get_db)
):
    """
    Authenticate a WebSocket connection using a token query parameter.
    Similar to get_current_active_user but for WebSocket connections.
    """
    try:
        token_data, token_type = decode_token(token)
        if token_type != "access":
            await websocket.close(code=1008, reason="Invalid token type")
            return None
            
        user = db.query(User).filter(User.email == token_data.email).first()
        if user is None or not user.is_active:
            await websocket.close(code=1008, reason="Invalid or inactive user")
            return None
            
        return user
    except JWTError:
        await websocket.close(code=1008, reason="Invalid token")
        return None
    except Exception as e:
        await websocket.close(code=1011, reason=f"Server error: {str(e)}")
        return None
