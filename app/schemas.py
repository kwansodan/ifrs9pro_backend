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


class OverviewModel(BaseModel):
    total_loans: int
    total_loan_value: float
    average_loan_amount: float
    total_customers: int

    class Config:
        from_attributes = True


class CustomerSummaryModel(BaseModel):
    individual_customers: int
    institutions: int
    mixed: int
    active_customers: int

    class Config:
        from_attributes = True


class PortfolioWithSummaryResponse(BaseModel):
    id: int
    name: Optional[str]
    description: Optional[str]
    asset_type: Optional[str]
    customer_type: Optional[str]
    funding_source: Optional[str]
    created_at: datetime
    updated_at: Optional[datetime]
    overview: OverviewModel
    customer_summary: CustomerSummaryModel

    class Config:
        from_attributes = True


# ECL schemas


class ECLCategoryData(BaseModel):
    """Data for each delinquency category row in the ECL grid"""

    num_loans: int
    total_loan_value: float
    provision_amount: float


class ECLSummaryMetrics(BaseModel):
    """Summary metrics for the ECL calculation"""

    pd: float
    lgd: float
    ead: float
    total_provision: float
    provision_percentage: float


class ECLSummary(BaseModel):
    """Response schema for the ECL calculation endpoint"""

    portfolio_id: int
    calculation_date: str
    current: ECLCategoryData
    olem: ECLCategoryData
    substandard: ECLCategoryData
    doubtful: ECLCategoryData
    loss: ECLCategoryData
    summary_metrics: ECLSummaryMetrics


# Impairment schemas
class ImpairmentCategory(BaseModel):
    """Configuration for an impairment category"""

    days_range: str  # Format: "0-30", "31-90", "360+" etc.
    rate: float


class ImpairmentCategoryData(BaseModel):
    """Data for each impairment category row"""

    days_range: str
    rate: float
    total_loan_value: float
    provision_amount: float


class ImpairmentSummaryMetrics(BaseModel):
    """Summary metrics for the impairment calculation"""

    total_loans: float
    total_provision: float


class LocalImpairmentSummary(BaseModel):
    """Response schema for the local impairment calculation endpoint"""

    portfolio_id: int
    calculation_date: str
    current: ImpairmentCategoryData
    olem: ImpairmentCategoryData
    substandard: ImpairmentCategoryData
    doubtful: ImpairmentCategoryData
    loss: ImpairmentCategoryData
    summary_metrics: ImpairmentSummaryMetrics


class ImpairmentConfig(BaseModel):
    """Configuration for all impairment categories"""

    current: ImpairmentCategory
    olem: ImpairmentCategory
    substandard: ImpairmentCategory
    doubtful: ImpairmentCategory
    loss: ImpairmentCategory


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
