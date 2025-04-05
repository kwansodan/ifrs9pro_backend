from fastapi import (
    APIRouter,
    Depends,
    HTTPException,
    status,
    Body,
)
from sqlalchemy.orm import Session
from typing import List, Dict, Optional
from datetime import datetime

from app.database import get_db
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


@router.get("/{portfolio_id}/quality-issues", response_model=List[QualityIssueResponse])
def get_quality_issues(
    portfolio_id: int,
    status_type: Optional[str] = None,
    issue_type: Optional[str] = None,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_active_user),
):
    """
    Retrieve quality issues for a specific portfolio.
    Optional filtering by status and issue type.
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
            status_code=status.HTTP_200_OK, detail="No quality issues found"
        )

    return quality_issues


@router.get("/{portfolio_id}/quality-issues/{issue_id}", response_model=QualityIssueResponse)
def get_quality_issue(
    portfolio_id: int,
    issue_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_active_user),
):
    """
    Retrieve a specific quality issue by ID.
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

    return issue


@router.put("/{portfolio_id}/quality-issues/{issue_id}", response_model=QualityIssueResponse)
def update_quality_issue(
    portfolio_id: int,
    issue_id: int,
    issue_update: QualityIssueUpdate,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_active_user),
):
    """
    Update a quality issue, including approving it (changing status to "approved").
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

    return issue


@router.post(
    "/{portfolio_id}/quality-issues/{issue_id}/comments",
    response_model=QualityIssueCommentModel,
)
def add_comment_to_quality_issue(
    portfolio_id: int,
    issue_id: int,
    comment_data: QualityIssueCommentCreate,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_active_user),
):
    """
    Add a comment to a quality issue.
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
    response_model=List[QualityIssueCommentModel],
)
def get_quality_issue_comments(
    portfolio_id: int,
    issue_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_active_user),
):
    """
    Get all comments for a quality issue.
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
    response_model=QualityIssueCommentModel,
)
def edit_quality_issue_comment(
    portfolio_id: int,
    issue_id: int,
    comment_id: int,
    comment_data: QualityIssueCommentCreate,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_active_user),
):
    """
    Edit a comment on a quality issue.
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
    "/{portfolio_id}/quality-issues/{issue_id}/approve", response_model=QualityIssueResponse
)
def approve_quality_issue(
    portfolio_id: int,
    issue_id: int,
    comment: Optional[str] = None,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_active_user),
):
    """
    Approve a quality issue, changing its status to "approved".
    Optionally add a comment about the approval.
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

    return issue


@router.post("/{portfolio_id}/approve-all-quality-issues", response_model=Dict)
def approve_all_quality_issues(
    portfolio_id: int,
    comment: Optional[str] = None,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_active_user),
):
    """
    Approve all open quality issues for a portfolio at once.
    Optionally add the same comment to all issues.
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


@router.post("/{portfolio_id}/recheck-quality", response_model=QualityCheckSummary)
def recheck_quality_issues(
    portfolio_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_active_user),
):
    """
    Run quality checks again to find any new issues.
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

    # Run quality checks and create issues if necessary
    quality_counts = create_quality_issues_if_needed(db, portfolio_id)

    return QualityCheckSummary(
        duplicate_customer_ids=quality_counts["duplicate_customer_ids"],
        duplicate_addresses=quality_counts["duplicate_addresses"],
        duplicate_dob=quality_counts["duplicate_dob"],
        duplicate_loan_ids=quality_counts["duplicate_loan_ids"],
        unmatched_employee_ids=quality_counts["unmatched_employee_ids"],
        loan_customer_mismatches=quality_counts["loan_customer_mismatches"],
        missing_dob=quality_counts["missing_dob"],
        total_issues=quality_counts["total_issues"],
        high_severity_issues=quality_counts["high_severity_issues"],
        open_issues=quality_counts["open_issues"],
    )
