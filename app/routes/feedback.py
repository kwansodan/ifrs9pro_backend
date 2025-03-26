from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from app.database import get_db
from app.models import Feedback, User, FeedbackStatus
from app.schemas import (
    FeedbackCreate,
    FeedbackUpdate,
    FeedbackResponse,
    FeedbackDetailResponse,
    FeedbackStatusUpdate,
)
from typing import List
from app.auth.utils import get_current_active_user, is_admin
from sqlalchemy import func

router = APIRouter(prefix="/feedback", tags=["feedback"])


@router.post("/", response_model=FeedbackResponse, status_code=status.HTTP_201_CREATED)
async def create_feedback(
    feedback: FeedbackCreate,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_active_user),
):
    """
    Create a new feedback entry
    """
    new_feedback = Feedback(
        title=feedback.title,
        description=feedback.description,
        user_id=current_user.id,
        status=FeedbackStatus.SUBMITTED,
    )

    db.add(new_feedback)
    db.commit()
    db.refresh(new_feedback)

    # Count likes
    like_count = len(new_feedback.liked_by)

    # Add like_count and is_liked_by_user to response
    response_data = FeedbackResponse.from_orm(new_feedback)
    response_data.like_count = like_count
    response_data.is_liked_by_user = False

    return response_data


@router.get("/", response_model=List[FeedbackResponse])
async def get_all_feedback(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_active_user),
    skip: int = 0,
    limit: int = 100,
):
    """
    Get all feedback entries with pagination
    """
    feedback_list = db.query(Feedback).offset(skip).limit(limit).all()

    # Prepare response with like count and user like status
    response_data = []
    for feedback in feedback_list:
        liked_by_current_user = current_user in feedback.liked_by
        like_count = len(feedback.liked_by)

        feedback_response = FeedbackResponse.from_orm(feedback)
        feedback_response.like_count = like_count
        feedback_response.is_liked_by_user = liked_by_current_user

        response_data.append(feedback_response)

    return response_data


@router.get("/{feedback_id}", response_model=FeedbackDetailResponse)
async def get_feedback(
    feedback_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_active_user),
):
    """
    Get a specific feedback entry by ID
    """
    feedback = db.query(Feedback).filter(Feedback.id == feedback_id).first()

    if not feedback:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Feedback not found"
        )

    # Check if user has liked this feedback
    liked_by_current_user = current_user in feedback.liked_by
    like_count = len(feedback.liked_by)

    # Prepare response
    feedback_response = FeedbackDetailResponse.from_orm(feedback)
    feedback_response.like_count = like_count
    feedback_response.is_liked_by_user = liked_by_current_user

    return feedback_response


@router.put("/{feedback_id}", response_model=FeedbackResponse)
async def update_feedback(
    feedback_id: int,
    feedback_update: FeedbackUpdate,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_active_user),
):
    """
    Update a feedback entry (only title and description)
    """
    feedback = db.query(Feedback).filter(Feedback.id == feedback_id).first()

    if not feedback:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Feedback not found"
        )

    # Check if user is the creator of the feedback
    if feedback.user_id != current_user.id:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="You can only update your own feedback",
        )

    # Update provided fields
    update_data = feedback_update.dict(exclude_unset=True)
    for key, value in update_data.items():
        setattr(feedback, key, value)

    db.commit()
    db.refresh(feedback)

    # Prepare response
    liked_by_current_user = current_user in feedback.liked_by
    like_count = len(feedback.liked_by)

    feedback_response = FeedbackResponse.from_orm(feedback)
    feedback_response.like_count = like_count
    feedback_response.is_liked_by_user = liked_by_current_user

    return feedback_response


@router.post("/{feedback_id}/like", response_model=FeedbackResponse)
async def like_feedback(
    feedback_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_active_user),
):
    """
    Like a feedback entry
    """
    feedback = db.query(Feedback).filter(Feedback.id == feedback_id).first()

    if not feedback:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Feedback not found"
        )

    # Check if user has already liked this feedback
    if current_user in feedback.liked_by:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="You have already liked this feedback",
        )

    # Add user to liked_by
    feedback.liked_by.append(current_user)
    db.commit()

    # Prepare response
    like_count = len(feedback.liked_by)

    feedback_response = FeedbackResponse.from_orm(feedback)
    feedback_response.like_count = like_count
    feedback_response.is_liked_by_user = True

    return feedback_response


@router.delete("/{feedback_id}/like", response_model=FeedbackResponse)
async def unlike_feedback(
    feedback_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_active_user),
):
    """
    Unlike a feedback entry
    """
    feedback = db.query(Feedback).filter(Feedback.id == feedback_id).first()

    if not feedback:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Feedback not found"
        )

    # Check if user has liked this feedback
    if current_user not in feedback.liked_by:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="You have not liked this feedback",
        )

    # Remove user from liked_by
    feedback.liked_by.remove(current_user)
    db.commit()

    # Prepare response
    like_count = len(feedback.liked_by)

    feedback_response = FeedbackResponse.from_orm(feedback)
    feedback_response.like_count = like_count
    feedback_response.is_liked_by_user = False

    return feedback_response


@router.delete("/{feedback_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_feedback(
    feedback_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_active_user),
):
    """
    Delete a feedback entry
    """
    feedback = db.query(Feedback).filter(Feedback.id == feedback_id).first()

    if not feedback:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Feedback not found"
        )

    # Check if user is the creator or an admin
    if feedback.user_id != current_user.id and current_user.role != "admin":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="You can only delete your own feedback",
        )

    db.delete(feedback)
    db.commit()

    return None
