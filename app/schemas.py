from pydantic import BaseModel, EmailStr, Field
from typing import Optional
from datetime import datetime
from enum import Enum

# Auth schemas
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


class LoginRequest(BaseModel):
    email: str
    password: str

# Portfolio schemas

from typing import Optional, List
from datetime import datetime
from enum import Enum

class AssetType(str, Enum):
    EQUITY = "equity"
    DEBT = "debt"

class CustomerType(str, Enum):
    INDIVIDUALS = "individuals"
    INSTITUTION = "institution"
    MIXED = "mixed"

class FundingSource(str, Enum):
    PRIVATE_INVESTORS = "private investors"
    PENSION_FUND = "pension fund"
    MUTUAL_FUND = "mutual fund"
    OTHER_FUNDS = "other funds"

class DataSource(str, Enum):
    EXTERNAL_APPLICATION = "connect to external application"
    UPLOAD_DATA = "upload data"


class PortfolioCreate(BaseModel):
    name: str
    description: str
    asset_type: AssetType
    customer_type: CustomerType
    funding_source: FundingSource
    data_source: DataSource
    repayment_source: bool = False

# For updating an existing portfolio
class PortfolioUpdate(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    asset_type: Optional[AssetType] = None
    customer_type: Optional[CustomerType] = None
    funding_source: Optional[FundingSource] = None
    data_source: Optional[DataSource] = None
    repayment_source: Optional[bool] = None


class PortfolioResponse(BaseModel):
    id: int
    name: str
    description: str
    asset_type: str
    customer_type: str
    funding_source: str
    data_source: str
    repayment_source: bool
    user_id: int
    created_at: datetime
    updated_at: Optional[datetime] = None
    
    class Config:
        from_attributes = True

class PortfolioList(BaseModel):
    items: List[PortfolioResponse]
    total: int
    
    class Config:
        from_attributes = True
