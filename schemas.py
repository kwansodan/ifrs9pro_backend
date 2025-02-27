from pydantic import BaseModel, EmailStr, Field
from typing import Optional
from datetime import datetime
from enum import Enum

class RequestStatus(str, Enum):
    PENDING = "pending"
    APPROVED = "approved"
    DENIED = "denied"
    FLAGGED = "flagged"

class UserRole(str, Enum):
    ADMIN = "admin"
    USER = "user"

class EmailVerificationRequest(BaseModel):
    email: EmailStr

class AccessRequestSubmit(BaseModel):
    email: EmailStr
    admin_email: Optional[EmailStr] = None

class AccessRequestResponse(BaseModel):
    id: int
    email: str
    admin_email: Optional[str] = None
    status: str
    created_at: datetime
    is_email_verified: bool

    class Config:
        from_attributes = True

class AccessRequestUpdate(BaseModel):
    status: RequestStatus
    role: Optional[UserRole] = None

class PasswordSetup(BaseModel):
    password: str = Field(..., min_length=8)
    confirm_password: str

class Token(BaseModel):
    access_token: str
    token_type: str

class TokenData(BaseModel):
    email: Optional[str] = None
    exp: Optional[datetime] = None

