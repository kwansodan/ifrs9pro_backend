from fastapi import (
    APIRouter,
    Depends,
    HTTPException,
    status,
    Body,
    Query,
)
from fastapi.responses import StreamingResponse
from sqlalchemy.orm import Session
from typing import List, Dict, Optional, Any
from datetime import datetime
from io import BytesIO
import pandas as pd
import logging

from app.database import get_db
from app.dependencies import get_tenant_db
from app.models import Portfolio, User, QualityIssue, QualityIssueComment
from app.auth.utils import get_current_active_user
from app.schemas import (
    QualityIssueResponse,
    QualityIssueUpdate,
    QualityIssueCommentCreate,
    QualityIssueCommentModel,
    QualityCheckSummary,
)
from app.utils.quality_checks import create_quality_issues_if_needed

# Create a separate router for quality issues
router = APIRouter(prefix="/portfolios", tags=["quality-issues"])


def transform_affected_records(quality_issues: List[QualityIssue]) -> List[QualityIssue]:
    """
    Transform affected_records from dictionary to list format for backward compatibility.
    This is needed because the schema expects affected_records to be a list of dictionaries,
    but older records in the database might have it as a single dictionary.
    """
    logger = logging.getLogger(__name__)
    
    for issue in quality_issues:
        try:
            # Check if affected_records is a dictionary (old format)
            if isinstance(issue.affected_records, dict):
                # Convert it to a list containing that dictionary
                issue.affected_records = [issue.affected_records]
                logger.debug(f"Transformed affected_records for issue {issue.id} to list format")
        except Exception as e:
            logger.error(f"Error transforming affected_records for issue {issue.id}: {str(e)}")
    
    return quality_issues


@router.get("/{portfolio_id}/quality-issues", 
            description="Get unique quality issues with occurrence counts for a specific portfolio", 
            response_model=List[Dict[str, Any]],
            responses={404: {"description": "Portfolio not found"},
                       401: {"description": "Not authenticated"}},
            )
def get_quality_issues(
    portfolio_id: int,
    status_type: Optional[str] = None,
    issue_type: Optional[str] = None,
    db: Session = Depends(get_tenant_db),
    current_user: User = Depends(get_current_active_user),
):
    """
    Retrieve unique quality issues with occurrence counts for a specific portfolio.
    Groups issues by issue_type and description, returning count of occurrences.
    Optional filtering by status and issue type.
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

    # Build query for quality issues
    query = db.query(QualityIssue).filter(QualityIssue.portfolio_id == portfolio_id)
    
    # Apply filters if provided
    if status_type:
        query = query.filter(QualityIssue.status == status_type)
    if issue_type:
        query = query.filter(QualityIssue.issue_type == issue_type)

    # Get all quality issues
    quality_issues = query.all()

    if not quality_issues:
        return []

    # Group issues by description to get unique issues with counts
    unique_issues = {}
    for issue in quality_issues:
        # Create a unique key based on description only
        key = issue.description
        
        if key not in unique_issues:
            unique_issues[key] = {
                "issue_type": issue.issue_type,
                "description": issue.description,
                "severity": issue.severity,
                "occurrence_count": 1,
                "statuses": {issue.status: 1},
                "first_occurrence": issue.created_at,
                "last_occurrence": issue.updated_at,
                "sample_issue_ids": [issue.id],
            }
        else:
            unique_issues[key]["occurrence_count"] += 1
            # Track status counts
            if issue.status in unique_issues[key]["statuses"]:
                unique_issues[key]["statuses"][issue.status] += 1
            else:
                unique_issues[key]["statuses"][issue.status] = 1
            # Update timestamps
            if issue.created_at < unique_issues[key]["first_occurrence"]:
                unique_issues[key]["first_occurrence"] = issue.created_at
            if issue.updated_at > unique_issues[key]["last_occurrence"]:
                unique_issues[key]["last_occurrence"] = issue.updated_at
            # Add sample issue ID (limit to first 5)
            if len(unique_issues[key]["sample_issue_ids"]) < 5:
                unique_issues[key]["sample_issue_ids"].append(issue.id)

    # Convert to list and sort by occurrence count (descending) and severity
    result = sorted(
        unique_issues.values(),
        key=lambda x: (x["occurrence_count"], x["severity"] == "high", x["severity"] == "medium"),
        reverse=True
    )

    return result


@router.get("/{portfolio_id}/quality-issues/download", 
            description="Download all quality issues for a portfolio as Excel", 
            status_code=status.HTTP_200_OK,
            responses={404: {"description": "Portfolio not found"},
                       401: {"description": "Not authenticated"}},)
async def download_all_quality_issues_excel(
    portfolio_id: int,
    status_type: Optional[str] = None,
    issue_type: Optional[str] = None,
    include_comments: bool = Query(False, description="Include comments in the download"),
    db: Session = Depends(get_tenant_db),
    current_user: User = Depends(get_current_active_user),
):
    """
    Download all quality issues for a portfolio as Excel.
    Optional filtering by status and issue type.
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

    # Build query for quality issues
    query = db.query(QualityIssue).filter(QualityIssue.portfolio_id == portfolio_id)
    
    # Apply filters if provided
    if status_type:
        query = query.filter(QualityIssue.status == status_type)
    if issue_type:
        query = query.filter(QualityIssue.issue_type == issue_type)

    # Order by severity (most severe first) and then by created date (newest first)
    quality_issues = query.order_by(
        QualityIssue.severity.desc(), QualityIssue.created_at.desc()
    ).all()

    if not quality_issues:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="No quality issues found"
        )

    # Transform affected_records from dictionary to list format if needed
    quality_issues = transform_affected_records(quality_issues)

    # Create filename with appropriate filters indicated
    status_suffix = f"_{status_type}" if status_type else ""
    type_suffix = f"_{issue_type}" if issue_type else ""
    filename = f"quality_issues_{portfolio.name.replace(' ', '_')}{status_suffix}{type_suffix}_{datetime.now().strftime('%Y%m%d')}.xlsx"

    # Create DataFrame for all issues
    issues_df = pd.DataFrame([{
        "ID": issue.id,
        "Issue Type": issue.issue_type,
        "Description": issue.description,
        "Severity": issue.severity,
        "Status": issue.status,
        "Created": issue.created_at,
        "Updated": issue.updated_at,
    } for issue in quality_issues])
    
    # Create Excel writer
    buffer = BytesIO()
    with pd.ExcelWriter(buffer) as writer:
        issues_df.to_excel(writer, sheet_name="All Issues", index=False)
        
        # Add a summary sheet
        summary_data = {
            "Type": "Count",
            "Total Issues": len(quality_issues),
            "High Severity": sum(1 for issue in quality_issues if issue.severity == "high"),
            "Medium Severity": sum(1 for issue in quality_issues if issue.severity == "medium"),
            "Low Severity": sum(1 for issue in quality_issues if issue.severity == "low"),
            "Open": sum(1 for issue in quality_issues if issue.status == "open"),
            "Approved": sum(1 for issue in quality_issues if issue.status == "approved"),
            "Other Status": sum(1 for issue in quality_issues if issue.status not in ["open", "approved"]),
        }
        
        summary_df = pd.DataFrame([summary_data])
        summary_df.to_excel(writer, sheet_name="Summary", index=False)
        
        # If comments are included, add a sheet for each issue's comments
        if include_comments:
            for issue in quality_issues:
                comments = (
                    db.query(QualityIssueComment)
                    .filter(QualityIssueComment.quality_issue_id == issue.id)
                    .order_by(QualityIssueComment.created_at)
                    .all()
                )
                
                if comments:
                    comments_df = pd.DataFrame([{
                        "Comment ID": comment.id,
                        "User ID": comment.user_id,
                        "User Email": db.query(User.email).filter(User.id == comment.user_id).scalar(),
                        "Comment": comment.comment,
                        "Created": comment.created_at,
                    } for comment in comments])
                    
                    sheet_name = f"Issue {issue.id} Comments"
                    # Excel sheet names must be <= 31 chars
                    if len(sheet_name) > 31:
                        sheet_name = sheet_name[:31]
                    
                    comments_df.to_excel(writer, sheet_name=sheet_name, index=False)
        
        # Add affected records for each issue if possible
        for issue in quality_issues:
            if issue.affected_records:
                try:
                    if isinstance(issue.affected_records, list) and len(issue.affected_records) > 0:
                        records_df = pd.DataFrame(issue.affected_records)
                        sheet_name = f"Issue {issue.id} Records"
                        # Excel sheet names must be <= 31 chars
                        if len(sheet_name) > 31:
                            sheet_name = sheet_name[:31]
                        records_df.to_excel(writer, sheet_name=sheet_name, index=False)
                except Exception:
                    # Skip if conversion fails for an issue
                    pass
    
    buffer.seek(0)
    return StreamingResponse(
        buffer,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={
            "Content-Disposition": f"attachment; filename={filename}",
            "Access-Control-Allow-Origin": "https://ifrs9pro.service4gh.com",
            "Access-Control-Allow-Credentials": "true"
        },
    )


@router.get("/{portfolio_id}/quality-issues/{issue_id}", 
            description="Retrieve specific quality issues for a particular portfolio", 
            response_model=QualityIssueResponse,
            responses={404: {"description": "Portfolio not found"},
                       401: {"description": "Not authenticated"}},)
def get_quality_issue(
    portfolio_id: int,
    issue_id: int,
    db: Session = Depends(get_tenant_db),
    current_user: User = Depends(get_current_active_user),
):
    """
    Retrieve a specific quality issue by ID.
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

    # Get the quality issue
    issue = (
        db.query(QualityIssue)
        .filter(QualityIssue.id == issue_id, QualityIssue.portfolio_id == portfolio_id)
        .first()
    )

    if not issue:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Quality issue not found"
        )

    # Transform affected_records from dictionary to list format if needed
    issue = transform_affected_records([issue])[0]

    return issue


@router.put("/{portfolio_id}/quality-issues/{issue_id}", 
            description="Update quality issues including approving them", 
            response_model=QualityIssueResponse,
            responses={404: {"description": "Portfolio not found"},
                       401: {"description": "Not authenticated"}},)
def update_quality_issue(
    portfolio_id: int,
    issue_id: int,
    issue_update: QualityIssueUpdate,
    db: Session = Depends(get_tenant_db),
    current_user: User = Depends(get_current_active_user),
):
    """
    Update a quality issue, including approving it (changing status to "approved").
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

    # Get the quality issue
    issue = (
        db.query(QualityIssue)
        .filter(QualityIssue.id == issue_id, QualityIssue.portfolio_id == portfolio_id)
        .first()
    )

    if not issue:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Quality issue not found"
        )

    # Update fields if provided
    update_data = issue_update.dict(exclude_unset=True)
    for key, value in update_data.items():
        setattr(issue, key, value)

    db.commit()
    db.refresh(issue)

    # Transform affected_records from dictionary to list format if needed
    issue = transform_affected_records([issue])[0]

    return issue


@router.post(
    "/{portfolio_id}/quality-issues/{issue_id}/comments", 
    description="Add a comment to a quality issue",
    response_model=QualityIssueCommentModel,
    responses={404: {"description": "Portfolio not found"},
               401: {"description": "Not authenticated"}},
)
def add_comment_to_quality_issue(
    portfolio_id: int,
    issue_id: int,
    comment_data: QualityIssueCommentCreate,
    db: Session = Depends(get_tenant_db),
    current_user: User = Depends(get_current_active_user),
):
    """
    Add a comment to a quality issue.
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

    # Get the quality issue
    issue = (
        db.query(QualityIssue)
        .filter(QualityIssue.id == issue_id, QualityIssue.portfolio_id == portfolio_id)
        .first()
    )

    if not issue:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Quality issue not found"
        )

    # Create new comment
    new_comment = QualityIssueComment(
        quality_issue_id=issue_id, user_id=current_user.id, comment=comment_data.comment
    )

    db.add(new_comment)
    db.commit()
    db.refresh(new_comment)

    return new_comment


@router.get(
    "/{portfolio_id}/quality-issues/{issue_id}/comments", 
    description="Get all comments for a quality issue",
    response_model=List[QualityIssueCommentModel],
    responses={404: {"description": "Portfolio not found"},
               401: {"description": "Not authenticated"}},
)
def get_quality_issue_comments(
    portfolio_id: int,
    issue_id: int,
    db: Session = Depends(get_tenant_db),
    current_user: User = Depends(get_current_active_user),
):
    """
    Get all comments for a quality issue.
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

    # Get the quality issue
    issue = (
        db.query(QualityIssue)
        .filter(QualityIssue.id == issue_id, QualityIssue.portfolio_id == portfolio_id)
        .first()
    )

    if not issue:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Quality issue not found"
        )

    # Get all comments for this issue, ordered by creation date
    comments = (
        db.query(QualityIssueComment)
        .filter(QualityIssueComment.quality_issue_id == issue_id)
        .order_by(QualityIssueComment.created_at)
        .all()
    )

    return comments


@router.put(
    "/{portfolio_id}/quality-issues/{issue_id}/comments/{comment_id}", 
    description="Edit a comment on a quality issue",
    response_model=QualityIssueCommentModel,
    responses={404: {"description": "Portfolio not found"},
               401: {"description": "Not authenticated"}},
)
def edit_quality_issue_comment(
    portfolio_id: int,
    issue_id: int,
    comment_id: int,
    comment_data: QualityIssueCommentCreate,
    db: Session = Depends(get_tenant_db),
    current_user: User = Depends(get_current_active_user),
):
    """
    Edit a comment on a quality issue.
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
    
    # Get the quality issue
    issue = (
        db.query(QualityIssue)
        .filter(QualityIssue.id == issue_id, QualityIssue.portfolio_id == portfolio_id)
        .first()
    )
    if not issue:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Quality issue not found"
        )
    
    # Get the comment and verify ownership
    comment = (
        db.query(QualityIssueComment)
        .filter(
            QualityIssueComment.id == comment_id,
            QualityIssueComment.quality_issue_id == issue_id,
            QualityIssueComment.user_id == current_user.id,
        )
        .first()
    )
    if not comment:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, 
            detail="Comment not found or you don't have permission to edit it"
        )
    
    # Update comment
    comment.comment = comment_data.comment
    
    db.commit()
    db.refresh(comment)
    return comment


@router.post(
    "/{portfolio_id}/quality-issues/{issue_id}/approve", 
    description="Approve a quality issue",
    response_model=QualityIssueResponse,
    responses={404: {"description": "Portfolio not found"},
               401: {"description": "Not authenticated"}},
)
def approve_quality_issue(
    portfolio_id: int,
    issue_id: int,
    comment: Optional[str] = None,
    db: Session = Depends(get_tenant_db),
    current_user: User = Depends(get_current_active_user),
):
    """
    Approve a quality issue, changing its status to "approved".
    Optionally add a comment about the approval.
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

    # Get the quality issue
    issue = (
        db.query(QualityIssue)
        .filter(QualityIssue.id == issue_id, QualityIssue.portfolio_id == portfolio_id)
        .first()
    )

    if not issue:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Quality issue not found"
        )

    # Update status to approved
    issue.status = "approved"

    # Add comment if provided
    if comment:
        new_comment = QualityIssueComment(
            quality_issue_id=issue_id,
            user_id=current_user.id,
            comment=f"Issue approved: {comment}",
        )
        db.add(new_comment)

    db.commit()
    db.refresh(issue)

    # Transform affected_records from dictionary to list format if needed
    issue = transform_affected_records([issue])[0]

    return issue


@router.post("/{portfolio_id}/approve-all-quality-issues", 
             description="Approve all open quality issues for a portfolio at once", 
             response_model=Dict,
             responses={404: {"description": "Portfolio not found"},
                        401: {"description": "Not authenticated"}},)
def approve_all_quality_issues(
    portfolio_id: int,
    comment: Optional[str] = None,
    db: Session = Depends(get_tenant_db),
    current_user: User = Depends(get_current_active_user),
):
    """
    Approve all open quality issues for a portfolio at once.
    Optionally add the same comment to all issues.
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

    # Get all open quality issues
    open_issues = (
        db.query(QualityIssue)
        .filter(
            QualityIssue.portfolio_id == portfolio_id, QualityIssue.status == "open"
        )
        .all()
    )

    if not open_issues:
        return {"message": "No open quality issues to approve", "count": 0}

    # Update all issues to approved
    for issue in open_issues:
        issue.status = "approved"

        # Add comment if provided
        if comment:
            new_comment = QualityIssueComment(
                quality_issue_id=issue.id,
                user_id=current_user.id,
                comment=f"Batch approval: {comment}",
            )
            db.add(new_comment)

    db.commit()

    return {"message": "All quality issues approved", "count": len(open_issues)}


@router.post("/{portfolio_id}/recheck-quality", 
             description="Run quality checks again to find any new issues.", 
             response_model=QualityCheckSummary,
             responses={404: {"description": "Portfolio not found"},
                        401: {"description": "Not authenticated"},},)
def recheck_quality_issues(
    portfolio_id: int,
    db: Session = Depends(get_tenant_db),
    current_user: User = Depends(get_current_active_user),
):
    """
    Run quality checks again to find any new issues.
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

    # Run quality checks and create issues if necessary
    quality_counts = create_quality_issues_if_needed(db, portfolio_id)

    return QualityCheckSummary(
        duplicate_customer_ids=quality_counts["duplicate_customer_ids"],
        duplicate_addresses=quality_counts["duplicate_addresses"],
        duplicate_dob=quality_counts["duplicate_dob"],
        duplicate_loan_ids=quality_counts["duplicate_loan_ids"],
        unmatched_employee_ids=quality_counts["clients_without_matching_loans"],
        loan_customer_mismatches=quality_counts["loans_without_matching_clients"],
        missing_dob=quality_counts["missing_dob"],
        total_issues=quality_counts["total_issues"],
        high_severity_issues=quality_counts["high_severity_issues"],
        open_issues=quality_counts["open_issues"],
    )


@router.get("/{portfolio_id}/quality-issues/{issue_id}/download", 
            description="Download a specific quality issue as Excel", 
            status_code=status.HTTP_200_OK,
            responses={404: {"description": "Portfolio not found"},
                       401: {"description": "Not Aunthenticated"},}
                       )
async def download_quality_issue_excel(
    portfolio_id: int,
    issue_id: int,
    include_comments: bool = Query(True, description="Include comments in the download"),
    db: Session = Depends(get_tenant_db),
    current_user: User = Depends(get_current_active_user),
):
    """
    Download a specific quality issue as Excel.
    Optionally include comments.
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

    # Get the quality issue
    issue = (
        db.query(QualityIssue)
        .filter(QualityIssue.id == issue_id, QualityIssue.portfolio_id == portfolio_id)
        .first()
    )

    if not issue:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Quality issue not found"
        )

    # Transform affected_records from dictionary to list format if needed
    issue = transform_affected_records([issue])[0]

    # Create filename
    filename = f"quality_issue_{issue.id}_{datetime.now().strftime('%Y%m%d')}.xlsx"

    # Create DataFrame for the issue
    issue_df = pd.DataFrame([{
        "ID": issue.id,
        "Issue Type": issue.issue_type,
        "Description": issue.description,
        "Severity": issue.severity,
        "Status": issue.status,
        "Created": issue.created_at,
        "Updated": issue.updated_at,
    }])
    
    # Create Excel writer
    buffer = BytesIO()
    with pd.ExcelWriter(buffer) as writer:
        issue_df.to_excel(writer, sheet_name="Issue Details", index=False)
        
        # Add affected records to a separate sheet
        if issue.affected_records:
            try:
                if isinstance(issue.affected_records, list) and len(issue.affected_records) > 0:
                    records_df = pd.DataFrame(issue.affected_records)
                    records_df.to_excel(writer, sheet_name="Affected Records", index=False)
            except Exception as e:
                # If conversion fails, add a sheet with error message
                pd.DataFrame({"Error": [f"Could not convert affected records: {str(e)}"]}).to_excel(
                    writer, sheet_name="Affected Records Error", index=False
                )
        
        # Add comments to a separate sheet if included
        if include_comments:
            comments = (
                db.query(QualityIssueComment)
                .filter(QualityIssueComment.quality_issue_id == issue_id)
                .order_by(QualityIssueComment.created_at)
                .all()
            )
            
            if comments:
                comments_df = pd.DataFrame([{
                    "ID": comment.id,
                    "User ID": comment.user_id,
                    "User Email": db.query(User.email).filter(User.id == comment.user_id).scalar(),
                    "Comment": comment.comment,
                    "Created": comment.created_at,
                } for comment in comments])
                
                comments_df.to_excel(writer, sheet_name="Comments", index=False)
    
    buffer.seek(0)
    return StreamingResponse(
        buffer,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={
            "Content-Disposition": f"attachment; filename={filename}",
            "Access-Control-Allow-Origin": "https://ifrs9pro.service4gh.com",
            "Access-Control-Allow-Credentials": "true"
        },
    )
