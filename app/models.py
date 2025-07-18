from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    ForeignKey,
    Integer,
    String,
    Text,
    Date,
    Numeric,
    Float,
    JSON,
    Table,
    UniqueConstraint
)
from sqlalchemy.sql import func
from enum import Enum as PyEnum
from app.database import Base
from sqlalchemy.orm import relationship
from datetime import datetime



class RequestStatus(str, PyEnum):
    PENDING = "pending"
    APPROVED = "approved"
    DENIED = "denied"
    FLAGGED = "flagged"


class UserRole(str, PyEnum):
    ADMIN = "admin"
    ANALYST = "analyst"
    REVIEWER = "reviewer"
    USER = "user"


class User(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True, index=True)
    first_name = Column(String, nullable=True)
    last_name = Column(String, nullable=True)
    email = Column(String, unique=True, index=True)
    recovery_email = Column(String, nullable=True)
    hashed_password = Column(String, nullable=True)
    role = Column(String, default=UserRole.USER)
    is_active = Column(Boolean, default=True)
    last_login = Column(DateTime(timezone=True))
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())
    portfolios = relationship("Portfolio", back_populates="user")
    quality_comments = relationship("QualityIssueComment", back_populates="user")
    feedback = relationship("Feedback", back_populates="user")
    help = relationship("Help", back_populates="user")


class AccessRequest(Base):
    __tablename__ = "access_requests"

    id = Column(Integer, primary_key=True, index=True)
    email = Column(String, index=True)
    admin_email = Column(String, nullable=True)
    status = Column(String, default=RequestStatus.PENDING)
    role = Column(String, nullable=True)
    is_email_verified = Column(Boolean, default=False)
    token = Column(String, nullable=True)
    token_expiry = Column(DateTime(timezone=True), nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())


class AssetType(str, PyEnum):
    EQUITY = "equity"
    DEBT = "debt"


class CustomerType(str, PyEnum):
    INDIVIDUALS = "individuals"
    INSTITUTION = "institution"
    MIXED = "mixed"


class FundingSource(str, PyEnum):
    PRIVATE_INVESTORS = "private investors"
    PENSION_FUND = "pension fund"
    MUTUAL_FUND = "mutual fund"
    OTHER_FUNDS = "other funds"


class DataSource(str, PyEnum):
    EXTERNAL_APPLICATION = "connect to external application"
    UPLOAD_DATA = "upload data"


class Portfolio(Base):
    __tablename__ = "portfolios"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    name = Column(String, nullable=True)
    description = Column(String, nullable=True)
    asset_type = Column(String, nullable=True)
    customer_type = Column(String, nullable=True)
    funding_source = Column(String, nullable=True)
    data_source = Column(String, nullable=True)
    repayment_source = Column(Boolean, default=False)
    credit_risk_reserve = Column(String, nullable=True)
    loan_assets = Column(String, nullable=True)
    ecl_impairment_account = Column(String, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())
    user = relationship("User", back_populates="portfolios")
    loans = relationship("Loan", back_populates="portfolio", passive_deletes=True)
    clients = relationship("Client", back_populates="portfolio", passive_deletes=True)
    guarantees = relationship("Guarantee", back_populates="portfolio", passive_deletes=True)
    quality_issues = relationship("QualityIssue", back_populates="portfolio", passive_deletes=True)
    reports = relationship("Report", back_populates="portfolio", passive_deletes=True)
    staging_results = relationship("StagingResult", back_populates="portfolio", passive_deletes=True)
    calculation_results = relationship("CalculationResult", back_populates="portfolio", passive_deletes=True)
    ecl_staging_config = Column(JSON, nullable=True)  # Store the configuration used for staging
    bog_staging_config = Column(JSON, nullable=True)  # Store the configuration used for staging


class ClientType(str, PyEnum):
    INDIVIDUAL = "individual"
    CORPORATE = "corporate"
    SME = "sme"


class MaritalStatus(str, PyEnum):
    SINGLE = "single"
    MARRIED = "married"
    DIVORCED = "divorced"
    WIDOWED = "widowed"
    OTHER = "other"


class Gender(str, PyEnum):
    MALE = "male"
    FEMALE = "female"
    OTHER = "other"
    PREFER_NOT_TO_SAY = "prefer_not_to_say"


class Title(str, PyEnum):
    MR = "mr"
    MRS = "mrs"
    MS = "ms"
    MISS = "miss"
    DR = "dr"
    PROF = "prof"
    OTHER = "other"


class Client(Base):
    __tablename__ = "clients"

    id = Column(Integer, primary_key=True, index=True)
    portfolio_id = Column(Integer, ForeignKey("portfolios.id", ondelete='CASCADE'), nullable=False)
    employee_id = Column(String, nullable=True)
    last_name = Column(String, nullable=True, index=True)
    other_names = Column(String, nullable=True)
    residential_address = Column(String, nullable=True)
    postal_address = Column(String, nullable=True)
    phone_number = Column(String, nullable=True)
    title = Column(String, nullable=True)
    marital_status = Column(String, nullable=True)
    gender = Column(String, nullable=True)
    date_of_birth = Column(Date, nullable=True)
    employer = Column(String, nullable=True)
    previous_employee_no = Column(String, nullable=True)
    social_security_no = Column(String, nullable=True)
    voters_id_no = Column(String, nullable=True)
    employment_date = Column(Date, nullable=True)
    next_of_kin = Column(String, nullable=True)
    next_of_kin_contact = Column(String, nullable=True)
    next_of_kin_address = Column(String, nullable=True)
    search_name = Column(String, nullable=True, index=True)
    client_type = Column(String, default=ClientType.INDIVIDUAL)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())
    portfolio = relationship("Portfolio", back_populates="clients")


class LoanType(str, PyEnum):
    PERSONAL = "personal"
    BUSINESS = "business"
    MORTGAGE = "mortgage"
    AUTO = "auto"
    EDUCATION = "education"
    OTHER = "other"


class DeductionStatus(str, PyEnum):
    ACTIVE = "active"
    PENDING = "pending"
    PAUSED = "paused"
    COMPLETED = "completed"
    DEFAULTED = "defaulted"


class Loan(Base):
    __tablename__ = "loans"

    id = Column(Integer, primary_key=True, index=True)
    portfolio_id = Column(Integer, ForeignKey("portfolios.id",ondelete='CASCADE'))
    loan_no = Column(String, index=True, nullable=True) 
    employee_id = Column(String, nullable=True)
    employee_name = Column(String, nullable=True)
    employer = Column(String, nullable=True)
    loan_issue_date = Column(Date, nullable=True)
    deduction_start_period = Column(Date, nullable=True)
    submission_period = Column(Date, nullable=True)
    maturity_period = Column(Date, nullable=True)
    location_code = Column(String, nullable=True)
    dalex_paddy = Column(String, nullable=True)
    team_leader = Column(String, nullable=True)
    loan_type = Column(String, nullable=True)
    loan_amount = Column(Numeric(precision=18, scale=2), nullable=False)
    loan_term = Column(Integer, nullable=True)
    administrative_fees = Column(Numeric(precision=18, scale=2), default=0)
    total_interest = Column(Numeric(precision=18, scale=2), default=0)
    total_collectible = Column(Numeric(precision=18, scale=2), default=0)
    net_loan_amount = Column(Numeric(precision=18, scale=2), default=0)
    monthly_installment = Column(Numeric(precision=18, scale=2), default=0)
    principal_due = Column(Numeric(precision=18, scale=2), default=0)
    interest_due = Column(Numeric(precision=18, scale=2), default=0)
    total_due = Column(Numeric(precision=18, scale=2), default=0)
    principal_paid = Column(Numeric(precision=18, scale=2), default=0)
    interest_paid = Column(Numeric(precision=18, scale=2), default=0)
    total_paid = Column(Numeric(precision=18, scale=2), default=0)
    principal_paid2 = Column(Numeric(precision=18, scale=2), default=0)
    interest_paid2 = Column(Numeric(precision=18, scale=2), default=0)
    total_paid2 = Column(Numeric(precision=18, scale=2), default=0)
    paid = Column(Boolean, default=False)
    cancelled = Column(Boolean, default=False)
    outstanding_loan_balance = Column(Numeric(precision=18, scale=2), default=0)
    accumulated_arrears = Column(Numeric(precision=18, scale=2), default=0)
    ndia = Column(Numeric(precision=18, scale=2), default=0)
    prevailing_posted_repayment = Column(Numeric(precision=18, scale=2), default=0)
    prevailing_due_payment = Column(Numeric(precision=18, scale=2), default=0)
    current_missed_deduction = Column(Numeric(precision=18, scale=2), default=0)
    admin_charge = Column(Numeric(precision=18, scale=2), default=0)
    recovery_rate = Column(Float, default=0)
    deduction_status = Column(String, default=DeductionStatus.PENDING)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())
    portfolio = relationship("Portfolio", back_populates="loans")
    ifrs9_stage = Column(String, nullable=True)
    bog_stage = Column(String, nullable=True)
    bog_prov_rate = Column(Numeric(precision=18, scale=2), default=0)
    amortised_bal = Column(Numeric(precision=38, scale=2), default=0)
    adjusted_amortised_bal = Column(Numeric(precision=38, scale=2), default=0)
    theoretical_balance = Column(Numeric(precision=38, scale=2), default=0)
    ead = Column(Numeric(precision=38, scale=2), default=0)
    lgd = Column(Numeric(precision=38, scale=2), default=0)
    pd = Column(Numeric(precision=38, scale=2), default=0)
    eir = Column(Numeric(precision=38, scale=2), default=0)
    ecl_12 = Column(Numeric(precision=38, scale=2), default=0)
    ecl_lifetime = Column(Numeric(precision=38, scale=2), default=0)
    final_ecl = Column(Numeric(precision=38, scale=2), default=0)
    bog_provision = Column(Numeric(precision=38, scale=2), default=0)
    calculation_date = Column(DateTime(timezone=False), nullable=True)

class Guarantee(Base):
    __tablename__ = "guarantees"
    id = Column(Integer, primary_key=True, index=True)
    portfolio_id = Column(Integer, ForeignKey("portfolios.id", ondelete='CASCADE'), nullable=True)
    guarantor = Column(String, nullable=False)
    pledged_amount = Column(Float, nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())
    portfolio = relationship("Portfolio", back_populates="guarantees")


class ValuationMethod(str, PyEnum):
    MARKET_VALUE = "market_value"
    BOOK_VALUE = "book_value"
    APPRAISAL = "appraisal"
    PURCHASE_PRICE = "purchase_price"
    OTHER = "other"


class SecurityType(str, PyEnum):
    CASH = "cash"
    NON_CASH = "non_cash"


class Security(Base):
    __tablename__ = "securities"

    id = Column(Integer, primary_key=True, index=True)
    client_id = Column(Integer, ForeignKey("clients.id"), nullable=False)
    collateral_description = Column(Text, nullable=True)
    collateral_value = Column(Numeric(precision=18, scale=2), nullable=False)
    forced_sale_value = Column(Numeric(precision=18, scale=2), nullable=True)
    method_of_valuation = Column(String, default=ValuationMethod.MARKET_VALUE)
    cash_or_non_cash = Column(String, default=SecurityType.NON_CASH)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())
    client = relationship("Client", backref="securities")


class DefaultDefinition(Base):
    __tablename__ = "default_definitions"

    id = Column(Integer, primary_key=True, index=True)
    clients_group = Column(String, nullable=True)
    overdue_days_repr_default = Column(Integer, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())


class ExtendingParty(str, PyEnum):
    BANK = "bank"
    CREDIT_UNION = "credit_union"
    MICROFINANCE = "microfinance"
    PRIVATE_LENDER = "private_lender"
    OTHER = "other"


class OtherLoans(Base):
    __tablename__ = "other_loans"

    id = Column(Integer, primary_key=True, index=True)
    client_id = Column(Integer, ForeignKey("clients.id"), nullable=False)
    loan_amount = Column(Numeric(precision=18, scale=2), nullable=False)
    extending_party = Column(String, default=ExtendingParty.BANK)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())
    client = relationship("Client", backref="other_loans")


class MacroEcos(Base):
    __tablename__ = "macro_ecos"

    id = Column(Integer, primary_key=True, index=True)
    employment_rate = Column(Float, nullable=True)
    inflation_rate = Column(Float, nullable=True)
    gdp = Column(Numeric(precision=18, scale=2), nullable=True)
    reference_date = Column(Date, nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())


class QualityIssue(Base):
    __tablename__ = "quality_issues"

    id = Column(Integer, primary_key=True, index=True)
    portfolio_id = Column(Integer, ForeignKey("portfolios.id", ondelete="CASCADE"))
    issue_type = Column(String(50), nullable=False)
    description = Column(Text, nullable=False)
    affected_records = Column(JSON, nullable=False)
    severity = Column(String(20), nullable=False)
    status = Column(String(20), default="open")
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, onupdate=func.now())
    portfolio = relationship("Portfolio", back_populates="quality_issues")
    comments = relationship(
        "QualityIssueComment",
        back_populates="quality_issue",
        cascade="all, delete-orphan",
    )


class QualityIssueComment(Base):
    __tablename__ = "quality_issue_comments"

    id = Column(Integer, primary_key=True, index=True)
    quality_issue_id = Column(
        Integer, ForeignKey("quality_issues.id", ondelete="CASCADE")
    )
    user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"))
    comment = Column(Text, nullable=False)
    created_at = Column(DateTime, default=func.now())
    quality_issue = relationship("QualityIssue", back_populates="comments")
    user = relationship("User", back_populates="quality_comments")


class Report(Base):
    __tablename__ = "reports"

    id = Column(Integer, primary_key=True, index=True)
    portfolio_id = Column(
        Integer, ForeignKey("portfolios.id", ondelete="CASCADE"), nullable=False
    )
    report_type = Column(String, nullable=False)
    report_date = Column(Date, nullable=False)
    report_name = Column(String, nullable=False)
    report_data = Column(JSON, nullable=False)
    file_path = Column(String, nullable=True)
    status = Column(String, default="pending")  # pending, success, failed
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    created_by = Column(Integer, ForeignKey("users.id"), nullable=False)
    portfolio = relationship("Portfolio", back_populates="reports")
    user = relationship("User")


# Feedback


class FeedbackStatus(str, PyEnum):
    SUBMITTED = "submitted"
    OPEN = "open"
    CLOSED = "closed"
    RETURNED = "returned"
    IN_DEVELOPMENT = "in development"
    COMPLETED = "completed"


# Many-to-many association table for users who liked feedback
feedback_likes = Table(
    "feedback_likes",
    Base.metadata,
    Column(
        "user_id", Integer, ForeignKey("users.id", ondelete="CASCADE"), primary_key=True
    ),
    Column(
        "feedback_id",
        Integer,
        ForeignKey("feedback.id", ondelete="CASCADE"),
        primary_key=True,
    ),
)


class Feedback(Base):
    __tablename__ = "feedback"

    id = Column(Integer, primary_key=True, index=True)
    description = Column(Text, nullable=False)
    user_id = Column(
        Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False
    )
    status = Column(String, default=FeedbackStatus.SUBMITTED)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    # Relationships
    user = relationship("User", back_populates="feedback")
    liked_by = relationship("User", secondary=feedback_likes, backref="liked_feedback")


# Help
class HelpStatus(str, PyEnum):
    SUBMITTED = "submitted"
    OPEN = "open"
    CLOSED = "closed"
    RETURNED = "returned"
    IN_DEVELOPMENT = "in development"
    COMPLETED = "completed"


    
class Help(Base):
    __tablename__ = "help"

    id = Column(Integer, primary_key=True, index=True)
    description = Column(Text, nullable=False)
    user_id = Column(
        Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False
    )
    status = Column(String, default=HelpStatus.SUBMITTED)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())
    # Relationships
    user = relationship("User", back_populates="help")




    
class StagingResult(Base):
    """
    Stores the results of loan staging operations, either for local impairment or ECL.
    """
    __tablename__ = "staging_results"

    id = Column(Integer, primary_key=True, index=True)
    portfolio_id = Column(Integer, ForeignKey("portfolios.id", ondelete="CASCADE"))
    staging_type = Column(String, nullable=False)  # "local_impairment" or "ecl"
    config = Column(JSON, nullable=False)  # Store the configuration used for staging
    result_summary = Column(JSON, nullable=False)  # Summary statistics of staging
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    
    # Relationships
    portfolio = relationship("Portfolio", back_populates="staging_results")


class CalculationResult(Base):
    """
    Stores the results of calculation operations, either for local impairment or ECL.
    """
    __tablename__ = "calculation_results"

    id = Column(Integer, primary_key=True, index=True)
    portfolio_id = Column(Integer, ForeignKey("portfolios.id", ondelete="CASCADE"))
    calculation_type = Column(String, nullable=False)  # "local_impairment" or "ecl"
    config = Column(JSON, nullable=False)  # Store the configuration used
    result_summary = Column(JSON, nullable=False)  # Summary stats of calculation
    total_provision = Column(Numeric(precision=18, scale=2), nullable=False)
    provision_percentage = Column(Numeric(precision=10, scale=4), nullable=False)
    reporting_date = Column(Date, nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    
    # Relationships
    portfolio = relationship("Portfolio", back_populates="calculation_results")


