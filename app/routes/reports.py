from fastapi import (
    APIRouter,
    Depends,
    HTTPException,
    status,
    Body,
    BackgroundTasks,
)
from fastapi.responses import StreamingResponse
from sqlalchemy.orm import Session
from datetime import date, datetime
from uuid import uuid4
from typing import List, Optional, Dict, Any
import base64
import logging
from io import BytesIO
import asyncio
from urllib.parse import urlparse

from app.database import get_db
from app.dependencies import get_tenant_db
from app.models import Portfolio, User, Report
from app.auth.utils import get_current_active_user
from app.utils.report_generators import (
    generate_collateral_summary,
    generate_guarantee_summary,
    generate_interest_rate_summary,
    generate_repayment_summary,
    generate_assumptions_summary,
    generate_amortised_loan_balances,
    generate_probability_default_report,
    generate_exposure_default_report,
    generate_loss_given_default_report,
    generate_ecl_detailed_report,
    generate_ecl_report_summarised,
    generate_local_impairment_details_report,
    generate_local_impairment_report_summarised,
    generate_journal_report,
    generate_report_excel,  # Changed from generate_report_pdf
)
# Use MinIO-based factory only
from app.utils.minio_reports_factory import (
    run_and_save_report_task,
    generate_presigned_url_for_download,
    download_report
)
from app.config import settings
from app.schemas import (
    ReportTypeEnum,
    ReportBase,
    ReportRequest,
    ReportSaveRequest,
    ReportCreate,
    ReportUpdate,
    ReportResponse,
    ReportHistoryItem,
    ReportHistoryList,
)


router = APIRouter(prefix="/reports", tags=["reports"])

logger = logging.getLogger(__name__)


@router.post("/{portfolio_id}/generate", 
             description="Generate various types of reports for a portfolio", 
             status_code=status.HTTP_200_OK,
             responses={404: {"description": "Portfolio not found"},
                        401: {"description": "Not Authenticated"}},)
async def generate_report(
    portfolio_id: int,
    report_request: ReportRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_tenant_db),
    current_user: User = Depends(get_current_active_user),
):
    logger.info("ENTER generate report")
    filename = f"{report_request.report_type.value}_{uuid4().hex}.xlsx"
    file_path = f"reports/{filename}"

    try:
        if report_request.report_type not in ["ecl_detailed_report", "ecl_report_summarised_by_stages", "BOG_impairment_detailed_report", "BOG_impairment_summary_by_stages", "journals_report"]:
            return {"error": "Invalid report type."}

        # Save metadata
        report = Report(
            created_by=current_user.id,  # Assuming current_user has an 'id' attribute
            report_type=report_request.report_type,
            report_date=report_request.report_date,
            report_name=filename,
            file_path=file_path,
            status="pending",
            portfolio_id=portfolio_id,  # Save the portfolio_id
            report_data={},  # Initialize report_data (you might populate this later)
        )

        db.add(report)
        db.commit()
        db.refresh(report)

        try:
            # Schedule background task (uses MinIO-backed run_and_save_report_task)
            background_tasks.add_task(run_and_save_report_task, report.id, report_request.report_type, file_path, portfolio_id)
        except Exception as e:
            # Update report status to failed
            report.status = "failed"
            db.commit()
            raise e

            raise e

        logger.info("EXIT generate report")
        return {"message": "Report generation started", "report_id": report.id}

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error generating report: {str(e)}",
        )


@router.get("/{portfolio_id}/history", 
            description="Get report history for a portfolio with optional filters", 
            response_model=ReportHistoryList,
            responses={404: {"description": "Portfolio not found"},
                       401: {"description": "Not Authenticated"}},)
async def get_report_history(
    portfolio_id: int,
    report_type: Optional[ReportTypeEnum] = None,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    skip: int = 0,
    limit: int = 20,
    db: Session = Depends(get_tenant_db),
    current_user: User = Depends(get_current_active_user),
):
    """
    Get the report history for a specific portfolio.
    Optional filtering by report type and date range.
    """
    # Verify portfolio exists and belongs to current user
    portfolio = (
        db.query(Portfolio)
        .filter(Portfolio.id == portfolio_id)
        .first()
    )

    if not portfolio:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Portfolio not found"
        )

    # Build query for reports
    query = db.query(Report).filter(Report.portfolio_id == portfolio_id)

    # Apply filters if provided
    if report_type:
        query = query.filter(Report.report_type == report_type)

    if start_date:
        query = query.filter(Report.report_date >= start_date)

    if end_date:
        query = query.filter(Report.report_date <= end_date)

    # Get total count for pagination
    total = query.count()

    # Apply pagination and order
    reports = query.order_by(Report.created_at.desc()).offset(skip).limit(limit).all()

    return {"items": reports, "total": total}


@router.get("/{portfolio_id}/report/{report_id}", 
            description="Get a specific report by ID", 
            response_model=ReportResponse,
            responses={404: {"description": "Portfolio or Report not found"},
                       401: {"description": "Not Authenticated"}},)
async def get_report(
    portfolio_id: int,
    report_id: int,
    db: Session = Depends(get_tenant_db),
    current_user: User = Depends(get_current_active_user),
):
    """
    Get a specific report by ID.
    """
    # Verify portfolio exists and belongs to current user
    portfolio = (
        db.query(Portfolio)
        .filter(Portfolio.id == portfolio_id)
        .first()
    )

    if not portfolio:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Portfolio not found"
        )

    # Get the report
    report = (
        db.query(Report)
        .filter(Report.id == report_id, Report.portfolio_id == portfolio_id)
        .first()
    )

    if not report:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Report not found"
        )

    return report


@router.delete(
    "/{portfolio_id}/report/{report_id}", 
    description="Delete a specific report by ID", 
    status_code=status.HTTP_204_NO_CONTENT,
    responses={404: {"description": "Portfolio or Report not found"},
               401: {"description": "Not Authenticated"}},
)
async def delete_report(
    portfolio_id: int,
    report_id: int,
    db: Session = Depends(get_tenant_db),
    current_user: User = Depends(get_current_active_user),
):
    """
    Delete a specific report by ID.
    """
    # Verify portfolio exists and belongs to current user
    portfolio = (
        db.query(Portfolio)
        .filter(Portfolio.id == portfolio_id)
        .first()
    )

    if not portfolio:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Portfolio not found"
        )

    # Get the report
    report = (
        db.query(Report)
        .filter(Report.id == report_id, Report.portfolio_id == portfolio_id)
        .first()
    )

    if not report:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Report not found"
        )

    # Delete the report
    try:
        db.delete(report)
        db.commit()
    except Exception as e:
        db.rollback()
        db.commit()
        raise HTTPException(status_code=500, detail=f"Delete failed: {str(e)}")


@router.get("/{portfolio_id}/report/{report_id}/download", 
            description="Download a specific report as Excel", 
            status_code=status.HTTP_200_OK,
            responses={404: {"description": "Portfolio or Report not found"},
                       401: {"description": "Not Authenticated"}},)
async def download_report_excel(
    portfolio_id: int,
    report_id: int,
    db: Session = Depends(get_tenant_db),
    current_user: User = Depends(get_current_active_user),
):
    """
    Fetch report metadata and download the actual file from MinIO.
    Only uses `report_name` from the report object.
    """
    # 1️⃣ Fetch report metadata
    report = (
        db.query(Report)
        .filter(Report.id == report_id, Report.portfolio_id == portfolio_id)
        .first()
    )

    if not report:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Report not found")

    # 2️⃣ Extract report_name
    report_name = report.report_name
    bucket_name = "ifrs9pro-reports"
    object_name = f"reports/{report_name}"

    # 3️⃣ Download and stream the file
    try:
        file_data = download_report(bucket_name, object_name)
        if asyncio.iscoroutine(file_data):
            file_data = await file_data

        return StreamingResponse(
            file_data,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={
                "Content-Disposition": f"attachment; filename={report_name}",
                "Access-Control-Allow-Origin": "https://ifrs9pro.service4gh.com",
                "Access-Control-Allow-Credentials": "true"
            }
        )

    except FileNotFoundError:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Report '{report_name}' not found in bucket '{bucket_name}'"
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error downloading report: {str(e)}"
        )


@router.get("/status/{report_id}", 
            description="Check status of a report generation",
            responses={404: {"description": "Report not found"},
                       401: {"description": "Not Authenticated"}},)
def get_report_status(report_id: int, db: Session = Depends(get_tenant_db)):
    status_val = db.query(Report.status).filter(Report.id == report_id).scalar()

    if not status_val:
        raise HTTPException(status_code=404, detail="Report generated in earlier versions of IFRS9PRO no report status found")
    return status_val