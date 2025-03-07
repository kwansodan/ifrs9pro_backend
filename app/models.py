from sqlalchemy import Boolean, Column, DateTime, ForeignKey, Integer, String, Text, Date, Numeric, Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql import func
from enum import Enum as PyEnum
from app.database import Base
from sqlalchemy.orm import relationship
 
class RequestStatus(str, PyEnum):
    PENDING = "pending"
    APPROVED = "approved"
    DENIED = "denied"
    FLAGGED = "flagged"

class UserRole(str, PyEnum):
    ADMIN = "admin"
    USER = "user"

class User(Base):
    __tablename__ = "users"
    
    id = Column(Integer, primary_key=True, index=True)
    email = Column(String, unique=True, index=True)
    hashed_password = Column(String, nullable=True)
    role = Column(String, default=UserRole.USER)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())
    portfolios = relationship("Portfolio", back_populates="user")

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
    credit_source = Column(String, nullable=True)
    loan_assets = Column(String, nullable=True)
    ecl_impairment_account = Column(String, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())
    user = relationship("User", back_populates="portfolios")


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
    employee_id = Column(Integer, ForeignKey("users.id"), nullable=True)
    lastname = Column(String, nullable=False, index=True)
    othernames = Column(String, nullable=False)
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
    employee = relationship("User", foreign_keys=[employee_id])
    loans = relationship("Loan", back_populates="client")


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
    loan_no = Column(String, unique=True, index=True, nullable=False)
    employee_id = Column(Integer, ForeignKey("users.id"), nullable=True)
    client_id = Column(Integer, ForeignKey("clients.id"), nullable=False)
    employee_name = Column(String, nullable=True)
    employer = Column(String, nullable=True)
    loan_issue_date = Column(Date, nullable=False)
    deduction_start_period = Column(Date, nullable=True)
    submission_period = Column(Date, nullable=True)
    maturity_period = Column(Date, nullable=True)
    location_code = Column(String, nullable=True)
    dalex_paddy = Column(String, nullable=True)
    team_leader = Column(String, nullable=True)
    loan_type = Column(String, nullable=False)
    loan_amount = Column(Numeric(precision=18, scale=2), nullable=False)
    loan_term = Column(Integer, nullable=False)  # In months
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
    employee = relationship("User", foreign_keys=[employee_id])
    client = relationship("Client", back_populates="loans")
