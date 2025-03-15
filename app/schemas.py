from pydantic import BaseModel, EmailStr, Field
from typing import Optional, List
from datetime import datetime, date
from enum import Enum


# Auth schemas
class RequestStatus(str, Enum):
    PENDING = "pending"
    APPROVED = "approved"
    DENIED = "denied"
    FLAGGED = "flagged"


class UserRole(str, Enum):
    ADMIN = "admin"
    ANALYST = "analyst"
    REVIEWER = "reviewer"
    USER = "user"


class EmailVerificationRequest(BaseModel):
    email: EmailStr


class PasswordSetup(BaseModel):
    password: str = Field(..., min_length=8, example="string")
    confirm_password: str


class Token(BaseModel):
    access_token: str
    token_type: str


class TokenData(BaseModel):
    email: Optional[EmailStr] = None
    exp: Optional[datetime] = None


class LoginRequest(BaseModel):
    email: EmailStr
    password: str


# Portfolio schemas


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
    credit_source: Optional[str] = None
    loan_assets: Optional[str] = None
    ecl_impairment_account: Optional[str] = None


# For updating an existing portfolio
class PortfolioUpdate(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    asset_type: Optional[AssetType] = None
    customer_type: Optional[CustomerType] = None
    funding_source: Optional[FundingSource] = None
    data_source: Optional[DataSource] = None
    repayment_source: Optional[bool] = None
    credit_source: Optional[str] = None
    loan_assets: Optional[str] = None
    ecl_impairment_account: Optional[str] = None


class PortfolioResponse(BaseModel):
    id: int
    name: str
    description: str
    asset_type: str
    customer_type: str
    funding_source: str
    data_source: str
    repayment_source: bool
    credit_source: Optional[str] = None
    loan_assets: Optional[str] = None
    ecl_impairment_account: Optional[str] = None
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

class LoanResponse(BaseModel):
    id: int
    portfolio_id: int
    loan_no: str
    employee_id: str
    employee_name: Optional[str]
    employer: Optional[str]
    loan_issue_date: date
    deduction_start_period: Optional[date]
    submission_period: Optional[date]
    maturity_period: Optional[date]
    location_code: Optional[str]
    dalex_paddy: Optional[str]
    team_leader: Optional[str]
    loan_type: str
    loan_amount: float
    loan_term: int
    administrative_fees: float = 0
    total_interest: float = 0
    total_collectible: float = 0
    net_loan_amount: float = 0
    monthly_installment: float = 0
    principal_due: float = 0
    interest_due: float = 0
    total_due: float = 0
    principal_paid: float = 0
    interest_paid: float = 0
    total_paid: float = 0
    principal_paid2: float = 0
    interest_paid2: float = 0
    total_paid2: float = 0
    paid: bool = False
    cancelled: bool = False
    outstanding_loan_balance: float = 0
    accumulated_arrears: float = 0
    ndia: float = 0
    prevailing_posted_repayment: float = 0
    prevailing_due_payment: float = 0
    current_missed_deduction: float = 0
    admin_charge: float = 0
    recovery_rate: float = 0
    deduction_status: str = "PENDING"
    created_at: datetime
    updated_at: Optional[datetime]
    
    class Config:
        from_attributes = True

        
class PortfolioWithLoansResponse(BaseModel):
    id: int
    name: Optional[str]
    description: Optional[str]
    asset_type: Optional[str]
    customer_type: Optional[str]
    funding_source: Optional[str]
    created_at: datetime
    updated_at: Optional[datetime]
    loans: List[LoanResponse]
    
    class Config:
        from_attributes = True

# Access request schemas


class AccessRequestSubmit(BaseModel):
    email: EmailStr
    admin_email: Optional[EmailStr] = None


class AccessRequestResponse(BaseModel):
    id: int
    email: EmailStr
    admin_email: Optional[EmailStr] = None
    status: str
    created_at: datetime
    is_email_verified: bool

    class Config:
        from_attributes = True


class AccessRequestUpdate(BaseModel):
    status: RequestStatus
    role: Optional[UserRole] = None


# User management schemas
class UserCreate(BaseModel):
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    email: EmailStr
    recovery_email: Optional[EmailStr] = None
    role: UserRole = UserRole.USER
    is_active: bool = True
    portfolio_id: Optional[int] = None

class UserResponse(BaseModel):
    id: int
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    email: EmailStr
    recovery_email: Optional[EmailStr] = None
    role: UserRole
    is_active: bool
    created_at: datetime
    last_login: Optional[datetime] = None
    updated_at: Optional[datetime] = None


class UserUpdate(BaseModel):
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    email: Optional[EmailStr] = None
    recovery_email: Optional[EmailStr] = None
    role: Optional[UserRole] = None
    is_active: Optional[bool] = None
