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

from app.database import get_db
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


@router.post("/{portfolio_id}/generate", status_code=status.HTTP_200_OK)
async def generate_report(
    portfolio_id: int,
    report_request: ReportRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_active_user),
):
    filename = f"{report_request.report_type.value}_{uuid4().hex}.xlsx"
    file_path = f"reports/{filename}"

    try:
        if report_request.report_type not in ["ecl_detailed_report", "ecl_report_summarised_by_stages", "BOG_impairment_detailed_report", "BOG_impairmnt_summary_by_stages", "journals_report"]:
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

        # Schedule background task (uses MinIO-backed run_and_save_report_task)
        background_tasks.add_task(run_and_save_report_task, report.id, report_request.report_type, file_path, portfolio_id)

        return {"message": "Report generation started", "report_id": report.id}

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error generating report: {str(e)}",
        )


@router.get("/{portfolio_id}/history", response_model=ReportHistoryList)
async def get_report_history(
    portfolio_id: int,
    report_type: Optional[ReportTypeEnum] = None,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    skip: int = 0,
    limit: int = 20,
    db: Session = Depends(get_db),
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


@router.get("/{portfolio_id}/report/{report_id}", response_model=ReportResponse)
async def get_report(
    portfolio_id: int,
    report_id: int,
    db: Session = Depends(get_db),
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
    "/{portfolio_id}/report/{report_id}", status_code=status.HTTP_204_NO_CONTENT
)
async def delete_report(
    portfolio_id: int,
    report_id: int,
    db: Session = Depends(get_db),
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


@router.get(
    "/{portfolio_id}/report/{report_id}/download", status_code=status.HTTP_200_OK
)
async def download_report_excel(
    portfolio_id: int,
    report_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_active_user),
):
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

    try:
        # generate_presigned_url_for_download may be sync or async in your minio factory.
        presigned = generate_presigned_url_for_download(report.file_path)
        if asyncio.iscoroutine(presigned):
            presigned = await presigned

        return {"download_url": presigned}

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error generating download URL: {str(e)}",
        )


@router.get("/status/{report_id}")
def get_report_status(report_id: int, db: Session = Depends(get_db)):
    status_val = db.query(Report.status).filter(Report.id == report_id).scalar()

    if not status_val:
        raise HTTPException(status_code=404, detail="Report generated in earlier versions of IFRS9PRO no report status found")
    return status_val
