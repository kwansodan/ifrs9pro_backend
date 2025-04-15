from fastapi import (
    APIRouter,
    Depends,
    HTTPException,
    status,
    Body,
)
from fastapi.responses import StreamingResponse
from sqlalchemy.orm import Session
from datetime import date, datetime
from typing import List, Optional, Dict, Any
import base64
from io import BytesIO
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


@router.post("/{portfolio_id}/generate", status_code=status.HTTP_200_OK)
async def generate_report(
    portfolio_id: int,
    report_request: ReportRequest,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_active_user),
):
    """
    Generate a report for a portfolio based on the report type.
    This endpoint does not save the report to the database.
    Returns both JSON report data and Excel file as base64.
    """
    # Verify portfolio exists and belongs to current user
    portfolio = (
        db.query(Portfolio)
        .filter(Portfolio.id == portfolio_id, Portfolio.user_id == current_user.id)
        .first()
    )

    if not portfolio:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Portfolio not found"
        )

    # Generate the report based on the report type
    try:
        report_data = None

        if report_request.report_type == ReportTypeEnum.COLLATERAL_SUMMARY:
            report_data = generate_collateral_summary(
                db=db, portfolio_id=portfolio_id, report_date=report_request.report_date
            )

        elif report_request.report_type == ReportTypeEnum.GUARANTEE_SUMMARY:
            report_data = generate_guarantee_summary(
                db=db, portfolio_id=portfolio_id, report_date=report_request.report_date
            )

        elif report_request.report_type == ReportTypeEnum.INTEREST_RATE_SUMMARY:
            report_data = generate_interest_rate_summary(
                db=db, portfolio_id=portfolio_id, report_date=report_request.report_date
            )

        elif report_request.report_type == ReportTypeEnum.REPAYMENT_SUMMARY:
            report_data = generate_repayment_summary(
                db=db, portfolio_id=portfolio_id, report_date=report_request.report_date
            )

        elif report_request.report_type == ReportTypeEnum.ASSUMPTIONS_SUMMARY:
            report_data = generate_assumptions_summary(
                db=db, portfolio_id=portfolio_id, report_date=report_request.report_date
            )

        elif report_request.report_type == ReportTypeEnum.AMORTISED_LOAN_BALANCES:
            report_data = generate_amortised_loan_balances(
                db=db, portfolio_id=portfolio_id, report_date=report_request.report_date
            )

        elif report_request.report_type == ReportTypeEnum.PROBABILITY_DEFAULT:
            report_data = generate_probability_default_report(
                db=db, portfolio_id=portfolio_id, report_date=report_request.report_date
            )

        elif report_request.report_type == ReportTypeEnum.EXPOSURE_DEFAULT:
            report_data = generate_exposure_default_report(
                db=db, portfolio_id=portfolio_id, report_date=report_request.report_date
            )

        elif report_request.report_type == ReportTypeEnum.LOSS_GIVEN_DEFAULT:
            report_data = generate_loss_given_default_report(
                db=db, portfolio_id=portfolio_id, report_date=report_request.report_date
            )

        elif report_request.report_type == ReportTypeEnum.ECL_DETAILED_REPORT:
            report_data = generate_ecl_detailed_report(
                db=db, portfolio_id=portfolio_id, report_date=report_request.report_date
            )

        elif report_request.report_type == ReportTypeEnum.ECL_REPORT_SUMMARISED:
            report_data = generate_ecl_report_summarised(
                db=db, portfolio_id=portfolio_id, report_date=report_request.report_date
            )

        elif report_request.report_type == ReportTypeEnum.LOCAL_IMPAIRMENT_DETAILS_REPORT:
            report_data = generate_local_impairment_details_report(
                db=db, portfolio_id=portfolio_id, report_date=report_request.report_date
            )

        elif report_request.report_type == ReportTypeEnum.LOCAL_IMPAIRMENT_REPORT_SUMMARISED:
            report_data = generate_local_impairment_report_summarised(
                db=db, portfolio_id=portfolio_id, report_date=report_request.report_date
            )

        elif report_request.report_type == ReportTypeEnum.JOURNALS_REPORT:
            report_data = generate_journal_report(
                db=db, portfolio_ids=[portfolio_id], report_date=report_request.report_date
            )

        else:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Unsupported report type: {report_request.report_type}",
            )

        # Generate the Excel file from the report data
        excel_bytes = generate_report_excel(
            db=db,
            portfolio_id=portfolio_id,
            report_type=report_request.report_type.value,
            report_date=report_request.report_date,
            report_data=report_data,
        )

        # Encode the Excel file as base64
        excel_base64 = base64.b64encode(excel_bytes).decode("utf-8")

        # Create a file name for the Excel file
        file_name = f"{portfolio.name.replace(' ', '_')}_{report_request.report_type.value}_{report_request.report_date}.xlsx"
        
        # Create a human-readable report name based on report type
        human_readable_name = ""
        if report_request.report_type == ReportTypeEnum.COLLATERAL_SUMMARY:
            human_readable_name = f"Collateral Summary Report - {portfolio.name}"
        elif report_request.report_type == ReportTypeEnum.GUARANTEE_SUMMARY:
            human_readable_name = f"Guarantee Summary Report - {portfolio.name}"
        elif report_request.report_type == ReportTypeEnum.INTEREST_RATE_SUMMARY:
            human_readable_name = f"Interest Rate Summary Report - {portfolio.name}"
        elif report_request.report_type == ReportTypeEnum.REPAYMENT_SUMMARY:
            human_readable_name = f"Repayment Summary Report - {portfolio.name}"
        elif report_request.report_type == ReportTypeEnum.ASSUMPTIONS_SUMMARY:
            human_readable_name = f"Assumptions Summary Report - {portfolio.name}"
        elif report_request.report_type == ReportTypeEnum.AMORTISED_LOAN_BALANCES:
            human_readable_name = f"Amortised Loan Balances Report - {portfolio.name}"
        elif report_request.report_type == ReportTypeEnum.PROBABILITY_DEFAULT:
            human_readable_name = f"Probability of Default Report - {portfolio.name}"
        elif report_request.report_type == ReportTypeEnum.EXPOSURE_DEFAULT:
            human_readable_name = f"Exposure at Default Report - {portfolio.name}"
        elif report_request.report_type == ReportTypeEnum.LOSS_GIVEN_DEFAULT:
            human_readable_name = f"Loss Given Default Report - {portfolio.name}"
        elif report_request.report_type == ReportTypeEnum.ECL_DETAILED_REPORT:
            human_readable_name = f"ECL Detailed Report - {portfolio.name}"
        elif report_request.report_type == ReportTypeEnum.ECL_REPORT_SUMMARISED:
            human_readable_name = f"ECL Summarised Report - {portfolio.name}"
        elif report_request.report_type == ReportTypeEnum.LOCAL_IMPAIRMENT_DETAILS_REPORT:
            human_readable_name = f"Local Impairment Detailed Report - {portfolio.name}"
        elif report_request.report_type == ReportTypeEnum.LOCAL_IMPAIRMENT_REPORT_SUMMARISED:
            human_readable_name = f"Local Impairment Summarised Report - {portfolio.name}"
        elif report_request.report_type == ReportTypeEnum.JOURNALS_REPORT:
            human_readable_name = f"IFRS9 and Local Impairment Journal Entries - {report_request.report_date.strftime('%B %d, %Y')}"
        else:
            human_readable_name = f"{report_request.report_type.value.replace('_', ' ').title()} - {portfolio.name}"
        
        # Add the date to the report name if not already included
        if report_request.report_type != ReportTypeEnum.JOURNALS_REPORT and report_request.report_date:
            human_readable_name += f" - {report_request.report_date.strftime('%B %d, %Y')}"

        # Return both the data and Excel in the response
        return {
            "portfolio_id": portfolio_id,
            "report_type": report_request.report_type,
            "report_date": report_request.report_date,
            "report_name": human_readable_name,  # Add the human-readable name
            "data": report_data,
            "file": {
                "filename": file_name,
                "content_type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                "content": excel_base64,
            },
        }

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error generating report: {str(e)}",
        )


@router.post(
    "/{portfolio_id}/save",
    response_model=ReportResponse,
    status_code=status.HTTP_201_CREATED,
)
async def save_report(
    portfolio_id: int,
    report_data: ReportSaveRequest,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_active_user),
):
    """
    Save a generated report to the database.
    """
    # Verify portfolio exists and belongs to current user
    portfolio = (
        db.query(Portfolio)
        .filter(Portfolio.id == portfolio_id, Portfolio.user_id == current_user.id)
        .first()
    )

    if not portfolio:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Portfolio not found"
        )

    try:
        # Remove the 'file' section from report_data if it exists to avoid storing
        # the redundant base64-encoded Excel file in the database
        cleaned_report_data = report_data.report_data
        if isinstance(cleaned_report_data, dict) and "file" in cleaned_report_data:
            # Make a copy without the 'file' key
            cleaned_report_data = {
                k: v for k, v in cleaned_report_data.items() if k != "file"
            }

        # Create a new report record
        new_report = Report(
            portfolio_id=portfolio_id,
            report_type=report_data.report_type,
            report_date=report_data.report_date,
            report_name=report_data.report_name,
            report_data=cleaned_report_data,  # Use the cleaned data without the file
            created_by=current_user.id,
        )

        db.add(new_report)
        db.commit()
        db.refresh(new_report)

        return new_report

    except Exception as e:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error saving report: {str(e)}",
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
        .filter(Portfolio.id == portfolio_id, Portfolio.user_id == current_user.id)
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
        .filter(Portfolio.id == portfolio_id, Portfolio.user_id == current_user.id)
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
        .filter(Portfolio.id == portfolio_id, Portfolio.user_id == current_user.id)
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
    db.delete(report)
    db.commit()

    return None


@router.get(
    "/{portfolio_id}/report/{report_id}/download", status_code=status.HTTP_200_OK
)
async def download_report_excel(
    portfolio_id: int,
    report_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_active_user),
):
    """
    Download a saved report as Excel.
    Returns a streaming response with the Excel file for download.
    """
    # Verify portfolio exists and belongs to current user
    portfolio = (
        db.query(Portfolio)
        .filter(Portfolio.id == portfolio_id, Portfolio.user_id == current_user.id)
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
        # Generate the Excel from the saved report data
        excel_bytes = generate_report_excel(
            db=db,
            portfolio_id=portfolio_id,
            report_type=report.report_type,
            report_date=report.report_date,
            report_data=report.report_data,
        )

        # Create a file name for the Excel
        report_name = f"{portfolio.name.replace(' ', '_')}_{report.report_type}_{report.report_date}.xlsx"

        # Return the Excel as a downloadable file
        return StreamingResponse(
            BytesIO(excel_bytes),
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": f"attachment; filename={report_name}"},
        )

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error generating Excel report: {str(e)}",
        )
