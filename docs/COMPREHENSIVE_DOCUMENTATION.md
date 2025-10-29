# IFRS9 Pro Backend - Comprehensive Documentation

## Table of Contents
1. [Overview](#overview)
2. [Architecture](#architecture)
3. [Database Schema](#database-schema)
4. [API Endpoints](#api-endpoints)
5. [Core Functionality](#core-functionality)
6. [Business Logic](#business-logic)
7. [File Structure](#file-structure)
8. [Configuration](#configuration)
9. [Deployment](#deployment)

## Overview

**IFRS9 Pro Backend** is a comprehensive financial risk management system designed to calculate Expected Credit Loss (ECL) and local impairment provisions for loan portfolios according to IFRS 9 and Bank of Ghana (BOG) standards. The system handles large-scale loan portfolios (70K+ loans) with real-time processing capabilities.

### Key Features
- **Portfolio Management**: Create and manage multiple loan portfolios
- **Data Ingestion**: Bulk import loan and client data from Excel files
- **Quality Assurance**: Automated data quality checks and issue tracking
- **ECL Calculations**: IFRS 9 compliant Expected Credit Loss calculations
- **Local Impairment**: Bank of Ghana impairment calculations
- **Report Generation**: Comprehensive Excel reports for regulatory compliance
- **User Management**: Role-based access control with admin capabilities
- **Real-time Processing**: WebSocket-based progress tracking for long-running operations

## Architecture

### Technology Stack
- **Framework**: FastAPI (Python 3.8+)
- **Database**: PostgreSQL with SQLAlchemy ORM
- **Authentication**: JWT tokens with role-based access
- **File Processing**: Polars for high-performance data processing
- **Background Tasks**: Async task management with progress tracking
- **Cloud Storage**: Azure Blob Storage for file management
- **Email**: Azure Communication Services and Brevo integration

### System Architecture
```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Frontend      │    │   FastAPI       │    │   PostgreSQL    │
│   (React/Vue)   │◄──►│   Backend       │◄──►│   Database      │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                              │
                              ▼
                       ┌─────────────────┐
                       │   Azure Blob    │
                       │   Storage       │
                       └─────────────────┘
```

## Database Schema

### Core Entities

#### Users and Authentication
- **User**: System users with roles (admin, analyst, reviewer, user)
- **AccessRequest**: User registration and approval workflow
- **UserRole**: Role-based permissions (admin, analyst, reviewer, user)

#### Portfolio Management
- **Portfolio**: Main portfolio entity containing loan collections
- **Client**: Customer/client information
- **Loan**: Individual loan records with financial details
- **Security**: Collateral and security information
- **Guarantee**: Loan guarantee details

#### Quality Management
- **QualityIssue**: Data quality issues and their resolution
- **QualityIssueComment**: Comments and discussions on quality issues

#### Calculations and Results
- **StagingResult**: Results of loan staging operations
- **CalculationResult**: Results of ECL and impairment calculations
- **Report**: Generated reports and their metadata

#### Support and Feedback
- **Feedback**: User feedback and feature requests
- **Help**: Help requests and support tickets

### Key Relationships
```
Portfolio (1) ──► (N) Loan
Portfolio (1) ──► (N) Client
Portfolio (1) ──► (N) QualityIssue
Portfolio (1) ──► (N) Report
User (1) ──► (N) Portfolio
Loan (N) ──► (1) Client (via employee_id)
```

## API Endpoints

### Authentication (`/auth`)
- `POST /request-access` - Request system access
- `GET /verify-email/{token}` - Email verification
- `POST /submit-admin-request/` - Submit access request to admin
- `POST /set-password/{token}` - Set user password
- `POST /login` - User login
- `POST /token` - OAuth2 token generation

### Portfolio Management (`/portfolios`)
- `POST /` - Create new portfolio
- `GET /` - List user portfolios with filtering
- `GET /{portfolio_id}` - Get portfolio details with comprehensive summary
- `PUT /{portfolio_id}` - Update portfolio configuration
- `DELETE /{portfolio_id}` - Delete portfolio
- `POST /{portfolio_id}/ingest` - Bulk data ingestion
- `POST /{portfolio_id}/stage-loans-ecl` - Stage loans for ECL
- `POST /{portfolio_id}/stage-loans-local` - Stage loans for local impairment
- `GET /{portfolio_id}/calculate-ecl` - Calculate ECL provisions
- `GET /{portfolio_id}/calculate-local-impairment` - Calculate local impairment

### Quality Management (`/portfolios/{portfolio_id}/quality-issues`)
- `GET /` - List quality issues with filtering
- `GET /{issue_id}` - Get specific quality issue
- `PUT /{issue_id}` - Update quality issue
- `POST /{issue_id}/comments` - Add comment to issue
- `GET /{issue_id}/comments` - Get issue comments
- `POST /{issue_id}/approve` - Approve quality issue
- `POST /approve-all-quality-issues` - Bulk approve all issues
- `GET /download` - Download quality issues as Excel
- `POST /recheck-quality` - Re-run quality checks

### Reports (`/reports`)
- `POST /{portfolio_id}/generate` - Generate reports
- `GET /{portfolio_id}/history` - Get report history
- `GET /{portfolio_id}/report/{report_id}` - Get specific report
- `DELETE /{portfolio_id}/report/{report_id}` - Delete report
- `GET /{portfolio_id}/report/{report_id}/download` - Download report

### Dashboard (`/dashboard`)
- `GET /dashboard` - Get comprehensive dashboard data

### Admin Management (`/admin`)
- `GET /requests` - List access requests
- `PUT /requests/{request_id}` - Update access request
- `DELETE /requests/{request_id}` - Delete access request
- `GET /users` - List all users
- `GET /users/export` - Export users as CSV
- `POST /users` - Create new user
- `PUT /users/{user_id}` - Update user
- `DELETE /users/{user_id}` - Delete user
- `GET /feedback` - List all feedback
- `PUT /feedback/{feedback_id}/status` - Update feedback status
- `GET /help` - List all help requests
- `PUT /help/{help_id}/status` - Update help request status

### WebSocket (`/ws`)
- `WS /tasks/{task_id}` - Real-time task progress updates

## Core Functionality

### 1. Portfolio Management
Portfolios are the central organizational unit containing loan collections. Each portfolio has:
- **Basic Information**: Name, description, asset type, customer type
- **Configuration**: Funding source, data source, repayment source
- **Staging Configuration**: ECL and BOG staging parameters
- **Data Status**: Ingestion status, calculation status, quality approval

### 2. Data Ingestion
The system supports bulk data import from Excel files:
- **Loan Details**: Primary loan information (required)
- **Client Data**: Customer information (required)
- **Guarantee Data**: Loan guarantees (optional)
- **Collateral Data**: Loan collateral (optional)

**Processing Features**:
- Automatic column mapping with case-insensitive matching
- Data type conversion and validation
- Chunked processing for large datasets (70K+ records)
- Progress tracking via WebSocket
- Duplicate detection and handling

### 3. Quality Assurance
Automated data quality checks identify:
- **Duplicate Records**: Customer IDs, loan IDs, addresses, DOBs
- **Data Mismatches**: Loans without matching clients
- **Missing Data**: Required fields, dates, amounts
- **Data Integrity**: Cross-reference validation

**Quality Issue Management**:
- Severity classification (high, medium, low)
- Status tracking (open, approved)
- Comment system for issue resolution
- Bulk approval capabilities

### 4. ECL Calculations (IFRS 9)
Expected Credit Loss calculations follow IFRS 9 standards:

**Staging Process**:
- **Stage 1**: Performing loans (12-month ECL)
- **Stage 2**: Underperforming loans (lifetime ECL)
- **Stage 3**: Credit-impaired loans (lifetime ECL)

**Calculation Components**:
- **Probability of Default (PD)**: ML-based prediction using client demographics
- **Loss Given Default (LGD)**: Based on collateral and recovery rates
- **Exposure at Default (EAD)**: Outstanding balance plus accrued interest

**Formula**: `ECL = EAD × PD × LGD`

### 5. Local Impairment (BOG)
Bank of Ghana impairment calculations:

**Staging Categories**:
- **Current**: 0-30 days past due (1% provision)
- **OLEM**: 31-90 days past due (5% provision)
- **Substandard**: 91-180 days past due (25% provision)
- **Doubtful**: 181-365 days past due (50% provision)
- **Loss**: 366+ days past due (100% provision)

**Calculation Method**:
- Days past due calculation from NDIA or accumulated arrears
- Category assignment based on configurable day ranges
- Provision calculation using category-specific rates

### 6. Report Generation
Comprehensive Excel reports for regulatory compliance:
- **ECL Detailed Report**: Loan-level ECL calculations
- **ECL Summary Report**: Stage-wise ECL summary
- **BOG Impairment Detailed Report**: Loan-level impairment details
- **BOG Impairment Summary Report**: Category-wise impairment summary
- **Journal Entries Report**: Accounting journal entries

## Business Logic

### ECL Calculation Engine
```python
def calculate_ecl(loan, reporting_date):
    # 1. Calculate Exposure at Default (EAD)
    ead = calculate_exposure_at_default_percentage(loan, reporting_date)
    
    # 2. Calculate Probability of Default (PD)
    pd = calculate_probability_of_default(employee_id, outstanding_balance, 
                                        start_date, reporting_date, end_date, arrears)
    
    # 3. Calculate Loss Given Default (LGD)
    lgd = calculate_loss_given_default(loan_amount, outstanding_balance, securities)
    
    # 4. Calculate Marginal ECL
    marginal_ecl = ead * pd * lgd
    
    return marginal_ecl
```

### Local Impairment Engine
```python
def calculate_local_impairment(loans, config):
    for loan in loans:
        days_past_due = calculate_days_past_due(loan)
        
        if current_range[0] <= days_past_due <= current_range[1]:
            category = "Current"
            rate = config.current.rate
        elif olem_range[0] <= days_past_due <= olem_range[1]:
            category = "OLEM"
            rate = config.olem.rate
        # ... continue for other categories
        
        provision = outstanding_balance * rate
```

### Data Quality Engine
```python
def create_quality_issues(portfolio_id):
    issues = []
    
    # Check for duplicates
    duplicate_customers = find_duplicate_customer_ids(portfolio_id)
    for group in duplicate_customers:
        issues.append(QualityIssue(
            issue_type="duplicate_customer_id",
            severity="high",
            description=f"Duplicate employee ID: {employee_id}",
            affected_records=group
        ))
    
    # Check for data mismatches
    unmatched_clients = find_clients_without_matching_loans(portfolio_id)
    for client in unmatched_clients:
        issues.append(QualityIssue(
            issue_type="client_without_matching_loan",
            severity="high",
            description=f"Client has no matching loan",
            affected_records=[client]
        ))
    
    return issues
```

## File Structure

```
ifrs9pro_backend/
├── app/
│   ├── __init__.py
│   ├── config.py                 # Configuration management
│   ├── database.py               # Database connection and models
│   ├── models.py                 # SQLAlchemy models
│   ├── schemas.py                # Pydantic schemas
│   ├── auth/
│   │   ├── email.py             # Email utilities
│   │   └── utils.py             # Authentication utilities
│   ├── calculators/
│   │   ├── ecl.py               # ECL calculation logic
│   │   └── local_impairment.py  # Local impairment logic
│   ├── ml_models/
│   │   └── logistic_model.pkl    # ML model for PD calculation
│   ├── routes/
│   │   ├── admin.py             # Admin endpoints
│   │   ├── auth.py              # Authentication endpoints
│   │   ├── dashboard.py         # Dashboard endpoints
│   │   ├── portfolio.py         # Portfolio management
│   │   ├── quality_issues.py   # Quality management
│   │   ├── reports.py           # Report generation
│   │   ├── user.py              # User management
│   │   └── websocket.py         # WebSocket endpoints
│   └── utils/
│       ├── background_calculations.py    # Background ECL/impairment
│       ├── background_ingestion.py      # Background data ingestion
│       ├── background_processors.py     # Data processing utilities
│       ├── background_tasks.py          # Task management
│       ├── db.py                        # Database utilities
│       ├── ecl_calculator.py            # ECL calculation utilities
│       ├── excel_generator.py          # Excel report generation
│       ├── formatters.py               # Data formatting
│       ├── pdf_generator.py            # PDF generation
│       ├── process_email_notifyer.py   # Email notifications
│       ├── processors.py               # Data processors
│       ├── quality_checks.py           # Quality check utilities
│       ├── report_generators.py        # Report generation
│       ├── reports_factory.py          # Report factory
│       ├── staging.py                  # Loan staging logic
│       └── sync_processors.py         # Synchronous processors
├── alembic/                      # Database migrations
├── docs/                          # Documentation
├── reports/                       # Generated reports
├── main.py                       # FastAPI application entry point
├── requirements.txt              # Python dependencies
├── pyproject.toml               # Project configuration
└── env                          # Environment variables
```

## Configuration

### Environment Variables
```bash
# Database
SQLALCHEMY_DATABASE_URL=postgresql://user:pass@host:port/db

# Authentication
SECRET_KEY=your-secret-key
ACCESS_TOKEN_EXPIRE_HOURS=8
ACCESS_TOKEN_EXPIRE_MINUTES=90
INVITATION_EXPIRE_HOURS=24

# Admin Credentials
ADMIN_EMAIL=admin@ifrs9pro.com
ADMIN_PASSWORD=one2three4

# Azure Services
AZURE_STORAGE_CONNECTION_STRING=DefaultEndpointsProtocol=https;...
AZURE_STORAGE_ACCOUNT_NAME=ifrs9pro
AZURE_STORAGE_ACCOUNT_KEY=your-account-key
CONTAINER_NAME=ifrs9prouploads

# Email Services
AZURE_COMMUNICATION_CONNECTION_STRING=endpoint=https://...
AZURE_SENDER_EMAIL=DoNotReply@...
BREVO_API_KEY=xkeysib-...
SENDER_EMAIL=no-reply@service4gh.com

# Application
DEBUG=True
FRONTEND_BASE_URL=https://ifrs9pro.service4gh.com
BASE_URL=https://ifrs9pro-api.service4gh.com
```

### Database Configuration
The system uses PostgreSQL with the following key features:
- **Connection Pooling**: Optimized for high-concurrency operations
- **Migrations**: Alembic-based database versioning
- **Indexing**: Optimized indexes for large-scale queries
- **Constraints**: Data integrity constraints and foreign keys

### Performance Optimizations
- **Chunked Processing**: Large datasets processed in chunks
- **Background Tasks**: Long-running operations in background
- **Database Indexing**: Optimized queries for 70K+ records
- **Caching**: ML model caching and lazy loading
- **Connection Pooling**: Efficient database connection management

## Deployment

### Production Environment
- **Platform**: Azure App Service
- **Database**: Azure Database for PostgreSQL
- **Storage**: Azure Blob Storage
- **Email**: Azure Communication Services
- **Monitoring**: Application Insights

### Development Setup
```bash
# Install dependencies
pip install -r requirements.txt

# Set up database
alembic upgrade head

# Run application
uvicorn main:app --host 0.0.0.0 --port 8000
```

### Key Features for Large-Scale Operations
1. **Optimized Data Processing**: Handles 70K+ loans efficiently
2. **Real-time Progress Tracking**: WebSocket-based progress updates
3. **Quality Assurance**: Comprehensive data validation
4. **Regulatory Compliance**: IFRS 9 and BOG standards
5. **Report Generation**: Excel-based regulatory reports
6. **User Management**: Role-based access control
7. **Audit Trail**: Complete operation logging

This documentation provides a comprehensive overview of the IFRS9 Pro backend system, covering all major components, functionality, and technical details necessary for understanding and maintaining the system.

