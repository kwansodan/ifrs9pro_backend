from typing import Annotated, Literal
from fastapi import UploadFile, File
from pydantic import BaseModel, Field, EmailStr, SecretStr, field_validator, ValidationError
from pydantic.types import StrictBool
from typing import List, Dict, Any, Optional
from datetime import datetime, date, timezone
from enum import Enum

# ==================== ENUM DEFINITIONS ====================

class RequestStatus(str, Enum):
    PENDING = "pending"
    APPROVED = "approved"
    DENIED = "denied"
    FLAGGED = "flagged"


class UserRole(str, Enum):
    SUPER_ADMIN="super_admin"
    ADMIN = "admin"
    ANALYST = "analyst"
    REVIEWER = "reviewer"
    USER = "user"


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


class ReportTypeEnum(str, Enum):
    ECL_DETAILED_REPORT = "ecl_detailed_report"
    ECL_REPORT_SUMMARISED = "ecl_report_summarised_by_stages"
    BOG_IMPAIRMENT_DETAILS_REPORT = "BOG_impairment_detailed_report"
    BOG_IMPAIRMENT_REPORT_SUMMARISED = "BOG_impairmnt_summary_by_stages"
    JOURNALS_REPORT = "journals_report"


class FeedbackStatusEnum(str, Enum):
    SUBMITTED = "submitted"
    OPEN = "open"
    CLOSED = "closed"
    RETURNED = "returned"
    IN_DEVELOPMENT = "in development"
    COMPLETED = "completed"


# ==================== TENANT REGISTRATION ====================
class TenantRegistrationRequest(BaseModel):
    # Company details
    company_name: str = Field(min_length=2, max_length=100)
    industry: str = Field(min_length=2, max_length=50)
    country: str = Field(min_length=2, max_length=2)

    preferred_accounting_standard: Literal[
        "IFRS9",
        "IFRS9_BOG",
    ]

    # Admin user
    first_name: str = Field(min_length=1, max_length=50)
    last_name: str = Field(min_length=1, max_length=50)
    email: EmailStr
    phone_number: str = Field(min_length=7, max_length=20)
    job_role: str = Field(min_length=2, max_length=50)

    password: str = Field(min_length=8, max_length=128, description="Password must be at least 8 characters")

    # Terms and Conditions
    tnd: Optional[bool] = False
    dpa: Optional[bool] = False
    
    @field_validator('password')
    @classmethod
    def validate_password_length(cls, v: str) -> str:
        if len(v) < 8:
            raise ValueError('Password must be at least 8 characters long')
        return v


# ==================== AUTH MODELS ====================

class UserModel(BaseModel):
    id: int
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    email: EmailStr
    recovery_email: Optional[EmailStr] = None
    role: UserRole
    is_active: bool
    access_request_status: Optional[RequestStatus] = None


class UserCreate(BaseModel):
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    email: EmailStr
    recovery_email: Optional[EmailStr] = None
    role: UserRole = UserRole.USER
    is_active: bool = True
    portfolio_id: Optional[int] = None


class UserUpdate(BaseModel):
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    email: Optional[EmailStr] = None
    recovery_email: Optional[EmailStr] = None
    role: Optional[UserRole] = None
    is_active: Optional[bool] = None


class UserResponse(BaseModel):
    id: int
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    email: EmailStr
    recovery_email: Optional[EmailStr] = None
    role: UserRole
    is_active: bool
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    last_login: Optional[datetime] = None
    updated_at: Optional[datetime] = None


class LoginRequest(BaseModel):
    email: EmailStr
    password: str


class LoginResponse(BaseModel):
    access_token: str
    token_type: str = Field(default="bearer", pattern="^bearer$")
    expires_in: int = Field(gt=0)
    user: UserModel


class EmailVerificationRequest(BaseModel):
    email: EmailStr


class PasswordSetup(BaseModel):
    password: str = Field(..., min_length=8, example="MyS3cur3Pwd", description="Password must be at least 8 characters")
    confirm_password: str = Field(..., min_length=8, example="MyS3cur3Pwd", description="Password must be at least 8 characters")
    
    @field_validator('password', 'confirm_password')
    @classmethod
    def validate_password_length(cls, v: str, info) -> str:
        if len(v) < 8:
            field_name = info.field_name.replace('_', ' ').title()
            raise ValueError(f'{field_name} must be at least 8 characters long')
        return v


class ForgotPasswordRequest(BaseModel):
    email: EmailStr


class ResetPasswordRequest(PasswordSetup):
    token: str


class Token(BaseModel):
    access_token: str
    token_type: str

class TenantToken(BaseModel):
    billing_token: str
    token_type: str

class TokenData(BaseModel):
    email: Optional[EmailStr] = None
    exp: Optional[datetime] = None
    tenant_id: Optional[int] = None


# ==================== ACCESS REQUEST MODELS ====================

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


# ==================== QUALITY ISSUE MODELS ====================

class QualityIssueCreate(BaseModel):
    issue_type: str
    description: str
    affected_records: List[Dict]
    severity: str = "medium"


class QualityIssueUpdate(BaseModel):
    status: Optional[str] = None
    description: Optional[str] = None
    severity: Optional[str] = None


class QualityIssueResponse(BaseModel):
    id: int
    portfolio_id: int
    issue_type: str
    description: str
    affected_records: List[Dict]
    severity: str
    status: str
    created_at: datetime
    updated_at: Optional[datetime] = None

    class Config:
        from_attributes = True


class QualityIssueCommentModel(BaseModel):  
    id: int
    quality_issue_id: int
    user_id: int
    comment: str
    created_at: datetime
    
    class Config:
        from_attributes = True  

class QualityIssueCommentCreate(BaseModel):
    comment: str



class QualityCheckSummary(BaseModel):
    """Quality check summary with counts of different types of issues."""
    duplicate_customer_ids: int = 0
    duplicate_addresses: int = 0
    duplicate_dob: int = 0 
    duplicate_loan_ids: int = 0
    unmatched_employee_ids: int = 0
    loan_customer_mismatches: int = 0
    missing_dob: int = 0
    total_issues: int = 0
    high_severity_issues: int = 0
    open_issues: int = 0
# ==================== FEEDBACK MODELS ====================

class FeedbackStatusEnum(str, Enum):
    SUBMITTED = "submitted"
    OPEN = "open"
    CLOSED = "closed"
    RETURNED = "returned"
    IN_DEVELOPMENT = "in development"
    COMPLETED = "completed"

class FeedbackBase(BaseModel):
    description: str = Field(..., min_length=3)

class FeedbackCreate(FeedbackBase):
    """
    Schema for creating new feedback
    """
    pass

class FeedbackUpdate(BaseModel):
    """
    Schema for updating existing feedback
    """
    description: Optional[str] = Field(None, min_length=3)

class FeedbackStatusUpdate(BaseModel):
    """
    Schema for updating feedback status
    """
    status: FeedbackStatusEnum

class UserBasic(BaseModel):
    """
    Basic user information for feedback response
    """
    id: int
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    email: str

    class Config:
        from_attributes = True

class FeedbackResponse(FeedbackBase):
    """
    Schema for feedback response
    """
    id: int
    status: FeedbackStatusEnum
    user_id: int
    user: Optional[UserBasic] = None
    created_at: datetime
    updated_at: Optional[datetime] = None
    is_creator: bool 
    like_count: int = 0
    is_liked_by_user: bool = False

    class Config:
        from_attributes = True

# ==================== REPORT MODELS ====================

class ReportBase(BaseModel):
    report_type: ReportTypeEnum
    report_date: date
    report_name: str


class ReportCreate(ReportBase):
    report_data: Dict[str, Any]
    portfolio_id: int


class ReportUpdate(BaseModel):
    report_name: Optional[str] = None
    report_data: Optional[Dict[str, Any]] = None


class ReportInDB(ReportBase):
    id: int
    portfolio_id: int
    created_at: datetime
    created_by: int
    report_data: Dict[str, Any]

    class Config:
        from_attributes = True


class ReportResponse(ReportInDB):
    pass


class ReportHistoryItem(BaseModel):
    id: int
    report_type: str
    report_date: date
    report_name: str
    created_at: datetime

    class Config:
        from_attributes = True


class ReportHistoryList(BaseModel):
    items: List[ReportHistoryItem]
    total: int


class ReportRequest(BaseModel):
    report_date: date
    report_type: ReportTypeEnum


class ReportSaveRequest(BaseModel):
    report_date: date
    report_type: ReportTypeEnum
    report_name: str
    report_data: Dict[str, Any]


# ==================== STAGING AND CALCULATION MODELS ====================

class StagingTypeInfo(BaseModel):
    completed: bool
    staged_at: Optional[datetime] = None
    total_loans: Optional[int] = None


class CalculationTypeInfo(BaseModel):
    completed: bool
    calculated_at: Optional[datetime] = None
    total_provision: Optional[float] = None
    provision_percentage: Optional[float] = None


class StagingStep(BaseModel):
    local_impairment: StagingTypeInfo
    ecl: StagingTypeInfo


class CalculationStep(BaseModel):
    local_impairment: CalculationTypeInfo
    ecl: CalculationTypeInfo


class CategoryData(BaseModel):
    num_loans: int
    total_loan_value: float
    provision_amount: float

class ECLCalculationDetail(BaseModel):
    """Detailed ECL calculation results for a portfolio"""
    stage_1: Optional[CategoryData] = Field(None, alias="Stage 1")
    stage_2: Optional[CategoryData] = Field(None, alias="Stage 2")
    stage_3: Optional[CategoryData] = Field(None, alias="Stage 3")
    total_provision: float = 0
    provision_percentage:Optional [float] = 0
    calculation_date: Optional[datetime] = None
    
    class Config:
        populate_by_name  = True
        json_encoders = {
            datetime: lambda v: v.isoformat() if v else None
        }

class LocalImpairmentDetail(BaseModel):
    """Detailed local impairment calculation results for a portfolio"""
    current: Optional[CategoryData] = Field(None, alias="Current")
    olem: Optional[CategoryData] = Field(None, alias="OLEM")
    substandard: Optional[CategoryData] = Field(None, alias="Substandard")
    doubtful: Optional[CategoryData] = Field(None, alias="Doubtful")
    loss: Optional[CategoryData] = Field(None, alias="Loss")
    total_provision: float = 0
    provision_percentage: float = 0
    calculation_date: Optional[datetime] = None
    
    class Config:
        populate_by_name  = True
        json_encoders = {
            datetime: lambda v: v.isoformat() if v else None
        }

class CalculationSummary(BaseModel):
    """Summary of calculation results for a portfolio"""
    ecl: Optional[ECLCalculationDetail] = None
    local_impairment: Optional[LocalImpairmentDetail] = None
    total_loan_value: float = 0


class IngestionStep(BaseModel):
    completed: bool
    total_loans: Optional[int] = None
    total_customers: Optional[int] = None
    last_ingestion_date: Optional[datetime] = None


# class CreationSteps(BaseModel):
#     creation: CreationStep
#     ingestion: IngestionStep
#     staging: StagingStep
#     calculation: CalculationStep


class StagingResultBase(BaseModel):
    staging_type: str
    config: Dict[str, Any]
    result_summary: Dict[str, Any]


class StagingResultCreate(StagingResultBase):
    portfolio_id: int


class StagingResultResponse(StagingResultBase):
    id: int
    portfolio_id: int
    created_at: datetime

    class Config:
        from_attributes = True


class CalculationResultBase(BaseModel):
    calculation_type: str
    config: Dict[str, Any]
    result_summary: Dict[str, Any]
    total_provision: float
    provision_percentage: float
    reporting_date: date


class CalculationResultCreate(CalculationResultBase):
    portfolio_id: int


class CalculationResultResponse(CalculationResultBase):
    id: int
    portfolio_id: int
    created_at: datetime

    class Config:
        from_attributes = True


class PortfolioLatestResults(BaseModel):
    latest_local_impairment_staging: Optional[StagingResultResponse] = None
    latest_ecl_staging: Optional[StagingResultResponse] = None
    latest_local_impairment_calculation: Optional[CalculationResultResponse] = None
    latest_ecl_calculation: Optional[CalculationResultResponse] = None

    class Config:
        from_attributes = True


        
# ==================== IMPAIRMENT MODELS ====================

class DaysRangeConfig(BaseModel):
    days_range: str = Field(..., example="0-30")


class ImpairmentCategory(BaseModel):
    """Configuration for an impairment category"""
    days_range: str  # Format: "0-30", "31-90", "360+" etc.
    rate: float

class ImpairmentRate(BaseModel):
    rate: float = Field(..., example=0.01)


class LocalImpairmentConfig(BaseModel):
    current: ImpairmentCategory
    olem: ImpairmentCategory
    substandard: ImpairmentCategory
    doubtful: ImpairmentCategory
    loss: ImpairmentCategory


class ImpairmentConfig(BaseModel):
    """Configuration for all impairment categories"""
    current: ImpairmentCategory
    olem: ImpairmentCategory
    substandard: ImpairmentCategory
    doubtful: ImpairmentCategory
    loss: ImpairmentCategory


class ProvisionRateConfig(BaseModel):
    current: float = Field(..., example=0.01)
    olem: float = Field(..., example=0.03)
    substandard: float = Field(..., example=0.2)
    doubtful: float = Field(..., example=0.5)
    loss: float = Field(..., example=1.0)


class ImpairmentCategoryData(BaseModel):
    """Data for each impairment category row"""
    days_range: str
    rate: float
    total_loan_value: float
    provision_amount: float
    
class LocalImpairmentCategoryData(BaseModel):
    num_loans: int
    total_loan_value: float
    provision_amount: float


class ImpairmentSummaryMetrics(BaseModel):
    """Summary metrics for the impairment calculation"""
    total_loans: float
    total_provision: float


class LocalImpairmentSummary(BaseModel):
    portfolio_id: int
    calculation_date: str
    current: CategoryData
    olem: CategoryData
    substandard: CategoryData
    doubtful: CategoryData
    loss: CategoryData
    total_provision: float
    provision_percentage: Optional [float]=0


class StagingSummaryStageData(BaseModel):
    """Data about loans in a specific stage"""
    num_loans: int
    outstanding_loan_balance: float


class ECLStagingConfig(BaseModel):
    stage_1: DaysRangeConfig
    stage_2: DaysRangeConfig
    stage_3: DaysRangeConfig


class ECLStagingSummary(BaseModel):
    """Summary of ECL staging results"""
    stage_1: Optional[StagingSummaryStageData] = Field(None, alias="Stage 1")
    stage_2: Optional[StagingSummaryStageData] = Field(None, alias="Stage 2")
    stage_3: Optional[StagingSummaryStageData] = Field(None, alias="Stage 3")
    config: Optional[ECLStagingConfig] = None
    staging_date: Optional[datetime] = None
    
    class Config:
        populate_by_name  = True
        json_encoders = {
            datetime: lambda v: v.isoformat() if v else None
        }


class LocalImpairmentStagingSummary(BaseModel):
    """Summary of local impairment staging results"""
    current: Optional[StagingSummaryStageData] = Field(None, alias="Current")
    olem: Optional[StagingSummaryStageData] = Field(None, alias="OLEM")
    substandard: Optional[StagingSummaryStageData] = Field(None, alias="Substandard")
    doubtful: Optional[StagingSummaryStageData] = Field(None, alias="Doubtful")
    loss: Optional[StagingSummaryStageData] = Field(None, alias="Loss")
    staging_date: Optional[datetime] = None
    config: Optional[LocalImpairmentConfig] = None
    
    class Config:
        populate_by_name  = True
        json_encoders = {
            datetime: lambda v: v.isoformat() if v else None
        }


# ==================== ECL MODELS ====================

class ECLCategoryData(BaseModel):
    """Data for each delinquency category row in the ECL grid"""
    num_loans: int
    total_loan_value: float
    provision_amount: float


class ECLComponentConfig(BaseModel):
    pd_factors: Dict[str, float] = Field(
        default_factory=lambda: {"stage_1": 0.01, "stage_2": 0.1, "stage_3": 0.5}
    )
    lgd_factors: Dict[str, float] = Field(
        default_factory=lambda: {"stage_1": 0.1, "stage_2": 0.3, "stage_3": 0.6}
    )
    ead_factors: Dict[str, float] = Field(
        default_factory=lambda: {"stage_1": 0.9, "stage_2": 0.95, "stage_3": 1.0}
    )


class ECLSummaryMetrics(BaseModel):
    avg_pd: float
    avg_lgd: float
    avg_ead: float
    total_provision: float
    provision_percentage: Optional [float]=0


class ECLSummary(BaseModel):
    portfolio_id: int
    calculation_date: str
    Stage_1: CategoryData = Field(alias="Stage 1")
    Stage_2: CategoryData = Field(alias="Stage 2")
    Stage_3: CategoryData = Field(alias="Stage 3")
    summary_metrics: ECLSummaryMetrics
    
    class Config:
        populate_by_name  = True


# ==================== CALCULATOR MODELS ====================

class LGDInput(BaseModel):
    loan_amount: float
    outstanding_balance: float
    securities: List[dict] = []


class EADInput(BaseModel):
    loan_amount: float
    outstanding_balance: float
    loan_issue_date: date
    maturity_date: date
    reporting_date: date


class PDInput(BaseModel):
    ndia: float
    loan_type: Optional[str] = None


class EIRInput(BaseModel):
    loan_amount: float
    monthly_installment: float
    loan_term: int


class LoanStageInfo(BaseModel):
    loan_id: int
    employee_id: str
    stage: str
    outstanding_loan_balance: float
    ndia: float
    loan_issue_date: date
    loan_amount: float
    monthly_installment: float
    loan_term: int
    accumulated_arrears: float


class StagingResponse(BaseModel):
    loans: List[LoanStageInfo]


class StagedLoans(BaseModel):
    portfolio_id: int
    loans: List[LoanStageInfo]


class CalculatorResponse(BaseModel):
    result: float
    input_data: dict

class StagingSummary(BaseModel):
    """Combined staging summary for both ECL and local impairment"""
    ecl: Optional[ECLStagingSummary] = Field(None, alias="ecl")
    local_impairment: Optional[LocalImpairmentStagingSummary] = Field(None, alias="local_impairment")
    
    class Config:
        populate_by_name  = True
        

        
# Help schemas

class HelpStatusEnum(str, Enum):
    SUBMITTED = "submitted"
    OPEN = "open"
    CLOSED = "closed"
    RETURNED = "returned"
    IN_DEVELOPMENT = "in development"
    COMPLETED = "completed"


class UserBase(BaseModel):
    id: int
    email: str
    first_name: Optional[str] = None
    last_name: Optional[str] = None


class HelpBase(BaseModel):
    description: str = Field(..., min_length=10)


class HelpCreate(HelpBase):
    pass

class HelpUpdate(BaseModel):
    description: Optional[str] = Field(None, min_length=10)


class HelpStatusUpdate(BaseModel):
    """Schema for updating the status of a help request (admin only)"""
    status: HelpStatusEnum
    
class HelpResponse(HelpBase):
    id: int
    user_id: int
    status: HelpStatusEnum
    created_at: datetime
    updated_at: Optional[datetime] = None
    user: Optional[UserBase] = None
    is_creator: bool

    class Config:
        from_attributes = True


# Notifications

class NotificationTypeEnum(str, Enum):
    SYSTEM = "SYSTEM"
    FEEDBACK = "FEEDBACK"
    HELP = "HELP"
    CALCULATION = "CALCULATION"
    REPORT = "REPORT"
    DATA_UPLOAD = "DATA_UPLOAD"
    
class NotificationResponse(BaseModel):
    id: int
    text: str
    type: NotificationTypeEnum
    time_ago: str
    created_at: datetime
    
    class Config:
        from_attributes = True

# ==================== PORTFOLIO MODELS ====================
class OverviewModel(BaseModel):
    total_loans: int
    total_loan_value: float
    average_loan_amount: float
    total_customers: int

    class Config:
        from_attributes = True


class PortfolioCreate(BaseModel):
    name: str
    description: str
    asset_type: AssetType
    customer_type: CustomerType
    funding_source: FundingSource
    data_source: DataSource
    repayment_source: StrictBool = False
    #credit_risk_reserve: Optional[str] = None
    #loan_assets: Optional[str] = None
    #ecl_impairment_account: Optional[str] = None


class PortfolioUpdate(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    asset_type: Optional[AssetType] = None
    customer_type: Optional[CustomerType] = None
    funding_source: Optional[FundingSource] = None
    data_source: Optional[DataSource] = None
    repayment_source: Optional[bool] = None
    credit_risk_reserve: Optional[str] = None
    loan_assets: Optional[str] = None
    ecl_impairment_account: Optional[str] = None
    ecl_staging_config: Optional[ECLStagingConfig] = None
    bog_staging_config: Optional[LocalImpairmentConfig] = None


class PortfolioResponseBase(BaseModel):
    id: int
    name: Optional[str] = ""
    description: Optional[str] = ""
    asset_type: Optional[str] = ""
    customer_type: Optional[str] = ""
    funding_source: Optional[str] = ""
    data_source: Optional[str] = ""
    repayment_source: Optional[bool] = False
    credit_risk_reserve: Optional[str] = None
    loan_assets: Optional[str] = None
    ecl_impairment_account: Optional[str] = None
    user_id: int
    created_at: datetime
    updated_at: Optional[datetime] = None
    ecl_staging_config: Optional[ECLStagingConfig] = None
    bog_staging_config: Optional[LocalImpairmentConfig] = None
    
    class Config:
        from_attributes = True

class PortfolioResponse(PortfolioResponseBase):
    has_ingested_data: bool = False  
    has_calculated_ecl: bool = False
    has_calculated_local_impairment: bool = False
    has_all_issues_approved: Optional[bool] = None

class PortfolioList(BaseModel):
    items: List[PortfolioResponse]
    total: int

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
    name: str
    description: Optional[str] = None
    asset_type: Optional[str] = None
    customer_type: Optional[str] = None
    funding_source: Optional[str] = None
    data_source: Optional[str] = None
    repayment_source: Optional[bool] = None
    ecl_staging_config: Optional[ECLStagingConfig] = None
    bog_staging_config: Optional[LocalImpairmentConfig] = None
    credit_risk_reserve: Optional[str] = None
    loan_assets: Optional[str] = None
    ecl_impairment_account: Optional[str] = None
    has_ingested_data: bool
    has_calculated_ecl: bool 
    has_calculated_local_impairment: bool
    has_all_issues_approved: Optional[bool] = None
    # created_at: datetime
    updated_at: Optional[datetime] = None
    overview: OverviewModel
    customer_summary: CustomerSummaryModel
    quality_check: Optional[QualityCheckSummary] = None
    quality_issues: Optional[List[QualityIssueResponse]] = None
    report_history: Optional[List[ReportHistoryItem]] = None
    calculation_summary: Optional[CalculationSummary] = None
    staging_summary: Optional[StagingSummary] = None

    class Config:
        from_attributes = True

class ColumnMappingBase(BaseModel):
    """Base class for all column mapping configurations"""
    class Config:
        from_attributes = True
        # Allows using dict-like access if needed
        extra = "forbid"  # optional: prevent unknown fields


class LoanGuaranteeColumns(ColumnMappingBase):
    loan_no: str = "loan no."
    guarantor_name: str = "guarantor name"
    guarantor_phone: str = "guarantor phone"
    guarantor_address: str = "guarantor address"
    guarantor_id: str = "guarantor id"
    relationship: str = "relationship"
    guarantee_amount: str = "guarantee amount"


class CollateralColumns(ColumnMappingBase):
    loan_no: str = "loan no."
    security_type: str = "security type"
    security_description: str = "security description"
    security_value: str = "security value"
    valuation_date: str = "valuation date"
    location: str = "location"
    registration_details: str = "registration details"
    ownership: str = "ownership"

class UploadedFileBase(BaseModel):
    file_id: str
    file_url: str
    object_name: str
    excel_columns: List[str]
    expected_columns: List[str]
    row_count: Optional[int] = None


class LoanDetailsFile(UploadedFileBase):
    pass


class ClientDataFile(UploadedFileBase):
    pass


class LoanGuaranteeDataFile(BaseModel):
    file_id: str
    file_url: str
    object_name: str
    excel_columns: List[str]
    expected_columns: List[str]


class LoanCollateralDataFile(BaseModel):
    file_id: str
    file_url: str
    object_name: str
    excel_columns: List[str]
    expected_columns: List[str]


class UploadedFiles(BaseModel):
    loan_details: Optional[LoanDetailsFile]
    client_data: Optional[ClientDataFile]
    loan_guarantee_data: Optional[LoanGuaranteeDataFile] = File(None)
    loan_collateral_data: Optional[LoanCollateralDataFile] = File(None)


class IngestAndSaveResponse(BaseModel):
    portfolio_id: int
    uploaded_files: UploadedFiles
    message: str

class FileMapping(BaseModel):
    type: str = Field(..., description="Type of the file, e.g., loan_details, client_data")
    object_name: str = Field(..., description="MinIO object key / file path")
    mapping: Dict[str, str] = Field(..., description="Excel column to model column mapping")

class IngestPayload(BaseModel):
    files: List[FileMapping] = Field(..., description="List of files to ingest with mappings")


# ==================== BILLING MODELS ====================
class CustomerCreate(BaseModel):
    first_name: str
    last_name: str
    phone: Optional[str] = None


class TransactionInitialize(BaseModel):
    amount: int = Field(..., description="Amount in GHS (smallest currency unit)")
    reference: Optional[str] = None
    callback_url: Optional[str] = None
    plan: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None


class SubscriptionDisable(BaseModel):
    code: str = Field(..., description="Subscription code")
    token: str = Field(..., description="Email token")

class ChangeSubscriptionRequest(BaseModel):
    new_plan_code: str
    old_subscription_token: str

class SubscriptionEnable(BaseModel):
    code: str = Field(..., description="Subscription code")
    token: str = Field(..., description="Email token")


# ---MULTITENANCY SCHEMAS  ---
class TenantCreate(BaseModel):
    name: str
    slug: str
    admin_email: EmailStr
    admin_first_name: str
    admin_last_name: str
    plan_name: str = "core"  # Default plan

class TenantResponse(BaseModel):
    id: int
    name: str
    slug: str
    is_active: bool
    created_at: datetime
    user_count: int
    portfolio_count: int

    class Config:
        from_attributes = True

class TenantUpdate(BaseModel):
    name: Optional[str] = None
    is_active: Optional[bool] = None

class SystemStats(BaseModel):
    total_tenants: int
    total_users: int
    total_portfolios: int
    total_loans: int
    total_value_locked: float
