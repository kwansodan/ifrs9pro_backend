from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from typing import List, Optional

from app.database import get_db
from app.models import User
from app.models import Feedback, FeedbackStatus
from app.auth.utils import get_current_active_user
from app.schemas import FeedbackCreate, FeedbackResponse, FeedbackStatusEnum

# Create router
router = APIRouter(prefix="/user", tags=["user actions"])

# Regular user feedback endpoints
@router.post("/feedback", response_model=FeedbackResponse)
async def create_feedback(
    feedback_data: FeedbackCreate,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_active_user),
):
    """
    Create new feedback
    """
    # Create new feedback object
    new_feedback = Feedback(
        title=feedback_data.title,
        description=feedback_data.description,
        user_id=current_user.id,
        status=FeedbackStatus.SUBMITTED,
    )
    
    # Add to database
    db.add(new_feedback)
    db.commit()
    db.refresh(new_feedback)
    
    # Prepare response
    liked_by_current_user = current_user in new_feedback.liked_by
    like_count = len(new_feedback.liked_by)
    
    feedback_response = FeedbackResponse.from_orm(new_feedback)
    feedback_response.like_count = like_count
    feedback_response.is_liked_by_user = liked_by_current_user
    
    return feedback_response

@router.get("/feedback", response_model=List[FeedbackResponse])
async def get_all_feedback(
    status: Optional[FeedbackStatusEnum] = None,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_active_user),
):
    """
    Get all feedback visible to regular users
    Optionally filter by status
    """
    # Start with base query
    query = db.query(Feedback)
    
    # Apply status filter if provided
    if status:
        query = query.filter(Feedback.status == status)
    
    # Get all feedback
    feedback_list = query.all()
    
    # Prepare response with like count
    response_data = []
    for feedback in feedback_list:
        liked_by_current_user = current_user in feedback.liked_by
        like_count = len(feedback.liked_by)
        feedback_response = FeedbackResponse.from_orm(feedback)
        feedback_response.like_count = like_count
        feedback_response.is_liked_by_user = liked_by_current_user
        response_data.append(feedback_response)
    
    return response_data

@router.get("/feedback/mine", response_model=List[FeedbackResponse])
async def get_my_feedback(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_active_user),
):
    """
    Get all feedback submitted by the currently logged in user
    """
    feedback_list = db.query(Feedback).filter(Feedback.user_id == current_user.id).all()
    
    # Prepare response with like count
    response_data = []
    for feedback in feedback_list:
        liked_by_current_user = current_user in feedback.liked_by
        like_count = len(feedback.liked_by)
        feedback_response = FeedbackResponse.from_orm(feedback)
        feedback_response.like_count = like_count
        feedback_response.is_liked_by_user = liked_by_current_user
        response_data.append(feedback_response)
    
    return response_data


@router.post("/feedback/{feedback_id}/like", response_model=FeedbackResponse)
async def like_feedback(
    feedback_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_active_user),
):
    """
    Like or unlike a feedback item
    """
    feedback = db.query(Feedback).filter(Feedback.id == feedback_id).first()
    if not feedback:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Feedback not found"
        )
    
    # Toggle like status
    if current_user in feedback.liked_by:
        feedback.liked_by.remove(current_user)
        liked_by_current_user = False
    else:
        feedback.liked_by.append(current_user)
        liked_by_current_user = True
    
    db.commit()
    db.refresh(feedback)
    
    # Prepare response
    like_count = len(feedback.liked_by)
    feedback_response = FeedbackResponse.from_orm(feedback)
    feedback_response.like_count = like_count
    feedback_response.is_liked_by_user = liked_by_current_user
    
    return feedback_response


