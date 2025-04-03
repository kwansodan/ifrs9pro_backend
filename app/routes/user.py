from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from typing import List, Optional

from app.database import get_db
from app.models import User
from app.models import Feedback, FeedbackStatus
from app.models import User, Help, HelpStatus
from app.auth.utils import get_current_active_user
from app.schemas import FeedbackCreate, FeedbackResponse, FeedbackStatusEnum, FeedbackUpdate
from app.schemas import HelpCreate, HelpResponse, HelpStatusEnum, HelpUpdate


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
        description=feedback_data.description,
        user_id=current_user.id,
        status=FeedbackStatus.SUBMITTED,
    )
    
    # Add to database
    db.add(new_feedback)
    db.commit()
    db.refresh(new_feedback)
    
    # Create response dictionary manually
    response_data = {
        "id": new_feedback.id,
        "description": new_feedback.description,
        "status": new_feedback.status,
        "user_id": new_feedback.user_id,
        "created_at": new_feedback.created_at,
        "updated_at": new_feedback.updated_at,
        "user": {
            "id": current_user.id,
            "email": current_user.email,
            "first_name": current_user.first_name,
            "last_name": current_user.last_name
        } if hasattr(current_user, "email") else None,
        "like_count": 0,  # New feedback has no likes
        "is_liked_by_user": False,
        "is_creator": True  # User creating feedback is the creator
    }
    
    # Return response directly from dictionary
    return FeedbackResponse(**response_data)

@router.put("/feedback/{feedback_id}", response_model=FeedbackResponse)
async def update_feedback(
    feedback_id: int,
    feedback_data: FeedbackUpdate,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_active_user),
):
    """
    Update a specific feedback entry
    Only the creator can update their feedback
    """
    # Get the feedback
    feedback = db.query(Feedback).filter(Feedback.id == feedback_id).first()
    if not feedback:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Feedback not found"
        )
    
    # Check if user is the creator
    if feedback.user_id != current_user.id:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN, 
            detail="You can only update your own feedback"
        )
    
    # Update the fields
    if feedback_data.description is not None:
        feedback.description = feedback_data.description
    
    # Save changes
    db.commit()
    db.refresh(feedback)
    
    # Create response dictionary manually
    response_data = {
        "id": feedback.id,
        "description": feedback.description,
        "status": feedback.status,
        "user_id": feedback.user_id,
        "created_at": feedback.created_at,
        "updated_at": feedback.updated_at,
        "user": {
            "id": feedback.user.id,
            "email": feedback.user.email,
            "first_name": feedback.user.first_name,
            "last_name": feedback.user.last_name
        } if hasattr(feedback, "user") and feedback.user else None,
        "like_count": len(feedback.liked_by),
        "is_liked_by_user": current_user in feedback.liked_by,
        "is_creator": True  # Must be creator to update
    }
    
    # Return response directly from dictionary
    return FeedbackResponse(**response_data)

@router.delete("/feedback/{feedback_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_feedback(
    feedback_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_active_user),
):
    """
    Delete a specific feedback entry
    Only the creator can delete their feedback
    """
    # Get the feedback
    feedback = db.query(Feedback).filter(Feedback.id == feedback_id).first()
    if not feedback:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Feedback not found"
        )
    
    # Check if user is the creator
    if feedback.user_id != current_user.id:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN, 
            detail="You can only delete your own feedback"
        )
    
    # Delete the feedback
    db.delete(feedback)
    db.commit()
    
    # Return no content for successful deletion
    return None

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
    
    # Prepare response with manual mapping
    response_data = []
    for feedback in feedback_list:
        # Create dictionary manually with all required fields
        feedback_dict = {
            "id": feedback.id,
            "description": feedback.description,
            "status": feedback.status,
            "user_id": feedback.user_id,
            "created_at": feedback.created_at,
            "updated_at": feedback.updated_at,
            "user": {
                "id": feedback.user.id,
                "email": feedback.user.email,
                "first_name": feedback.user.first_name,
                "last_name": feedback.user.last_name
            } if hasattr(feedback, "user") and feedback.user else None,
            "like_count": len(feedback.liked_by),
            "is_liked_by_user": current_user in feedback.liked_by,
            "is_creator": feedback.user_id == current_user.id
        }
        
        # Create response object from dictionary
        response_data.append(FeedbackResponse(**feedback_dict))
    
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
    
    # Prepare response with manual mapping
    response_data = []
    for feedback in feedback_list:
        # Create dictionary manually with all required fields
        feedback_dict = {
            "id": feedback.id,
            "description": feedback.description,
            "status": feedback.status,
            "user_id": feedback.user_id,
            "created_at": feedback.created_at,
            "updated_at": feedback.updated_at,
            "user": {
                "id": feedback.user.id,
                "email": feedback.user.email,
                "first_name": feedback.user.first_name,
                "last_name": feedback.user.last_name
            } if hasattr(feedback, "user") and feedback.user else None,
            "like_count": len(feedback.liked_by),
            "is_liked_by_user": current_user in feedback.liked_by,
            "is_creator": True  # All feedback in /mine endpoint is created by current user
        }
        
        # Create response object from dictionary
        response_data.append(FeedbackResponse(**feedback_dict))
    
    return response_data

@router.get("/feedback/{feedback_id}", response_model=FeedbackResponse)
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
    
    # Create response dictionary manually
    response_data = {
        "id": feedback.id,
        "description": feedback.description,
        "status": feedback.status,
        "user_id": feedback.user_id,
        "created_at": feedback.created_at,
        "updated_at": feedback.updated_at,
        "user": {
            "id": feedback.user.id,
            "email": feedback.user.email,
            "first_name": feedback.user.first_name,
            "last_name": feedback.user.last_name
        } if hasattr(feedback, "user") and feedback.user else None,
        "like_count": len(feedback.liked_by),
        "is_liked_by_user": current_user in feedback.liked_by,
        "is_creator": feedback.user_id == current_user.id
    }
    
    # Return response directly from dictionary
    return FeedbackResponse(**response_data)

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
    
    # Create response dictionary manually
    response_data = {
        "id": feedback.id,
        "description": feedback.description,
        "status": feedback.status,
        "user_id": feedback.user_id,
        "created_at": feedback.created_at,
        "updated_at": feedback.updated_at,
        "user": {
            "id": feedback.user.id,
            "email": feedback.user.email,
            "first_name": feedback.user.first_name,
            "last_name": feedback.user.last_name
        } if hasattr(feedback, "user") and feedback.user else None,
        "like_count": len(feedback.liked_by),
        "is_liked_by_user": liked_by_current_user,
        "is_creator": feedback.user_id == current_user.id
    }
    
    # Return response directly from dictionary
    return FeedbackResponse(**response_data)



@router.post("/help", response_model=HelpResponse)
async def create_help(
    help_data: HelpCreate,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_active_user),
):
    """
    Create a new help request
    """
    # Create new help object
    new_help = Help(
        description=help_data.description,
        user_id=current_user.id,
        status=HelpStatus.SUBMITTED,
    )
    
    # Add to database
    db.add(new_help)
    db.commit()
    db.refresh(new_help)
    
    # Create response dictionary
    response_data = {
        "id": new_help.id,
        "description": new_help.description,
        "status": new_help.status,
        "user_id": new_help.user_id,
        "created_at": new_help.created_at,
        "updated_at": new_help.updated_at,
        "user": {
            "id": current_user.id,
            "email": current_user.email,
            "first_name": current_user.first_name,
            "last_name": current_user.last_name
        } if hasattr(current_user, "email") else None,
        "is_creator": True
    }
    
    # Return response
    return HelpResponse(**response_data)

@router.put("/help/{help_id}", response_model=HelpResponse)
async def update_help(
    help_id: int,
    help_data: HelpUpdate,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_active_user),
):
    """
    Update a help request
    Only the creator can update their help request
    """
    # Get the help
    help_item = db.query(Help).filter(Help.id == help_id).first()
    if not help_item:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Help request not found"
        )
    
    # Check if user is the creator
    if help_item.user_id != current_user.id:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN, 
            detail="You can only update your own help requests"
        )
    
    # Update the fields
    if help_data.description is not None:
        help_item.description = help_data.description
    
    # Save changes
    db.commit()
    db.refresh(help_item)
    
    # Create response dictionary
    response_data = {
        "id": help_item.id,
        "description": help_item.description,
        "status": help_item.status,
        "user_id": help_item.user_id,
        "created_at": help_item.created_at,
        "updated_at": help_item.updated_at,
        "user": {
            "id": help_item.user.id,
            "email": help_item.user.email,
            "first_name": help_item.user.first_name,
            "last_name": help_item.user.last_name
        } if hasattr(help_item, "user") and help_item.user else None,
        "is_creator": True
    }
    
    # Return response
    return HelpResponse(**response_data)

@router.delete("/help/{help_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_help(
    help_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_active_user),
):
    """
    Delete a help request
    Only the creator can delete their help request
    """
    # Get the help
    help_item = db.query(Help).filter(Help.id == help_id).first()
    if not help_item:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Help request not found"
        )
    
    # Check if user is the creator
    if help_item.user_id != current_user.id:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN, 
            detail="You can only delete your own help requests"
        )
    
    # Delete the help
    db.delete(help_item)
    db.commit()
    
    # Return no content for successful deletion
    return None

@router.get("/help", response_model=List[HelpResponse])
async def get_my_help(
    status: Optional[HelpStatusEnum] = None,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_active_user),
):
    """
    Get all help requests for the current user
    Optionally filter by status
    """
    # Start with base query for current user's help requests
    query = db.query(Help).filter(Help.user_id == current_user.id)
    
    # Apply status filter if provided
    if status:
        query = query.filter(Help.status == status)
    
    # Get all help items
    help_items = query.all()
    
    # Prepare response with manual mapping
    response_data = []
    for help_item in help_items:
        # Create dictionary manually with all required fields
        help_dict = {
            "id": help_item.id,
            "description": help_item.description,
            "status": help_item.status,
            "user_id": help_item.user_id,
            "created_at": help_item.created_at,
            "updated_at": help_item.updated_at,
            "user": {
                "id": help_item.user.id,
                "email": help_item.user.email,
                "first_name": help_item.user.first_name,
                "last_name": help_item.user.last_name
            } if hasattr(help_item, "user") and help_item.user else None,
            "is_creator": True  # All help items belong to current user
        }
        
        # Create response object from dictionary
        response_data.append(HelpResponse(**help_dict))
    
    return response_data

@router.get("/help/{help_id}", response_model=HelpResponse)
async def get_help(
    help_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_active_user),
):
    """
    Get a specific help request by ID
    Users can only view their own help requests
    """
    # Get the help with current user check
    help_item = db.query(Help).filter(
        Help.id == help_id, 
        Help.user_id == current_user.id
    ).first()
    
    if not help_item:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, 
            detail="Help request not found or not authorized to view"
        )
    
    # Create response dictionary
    response_data = {
        "id": help_item.id,
        "description": help_item.description,
        "status": help_item.status,
        "user_id": help_item.user_id,
        "created_at": help_item.created_at,
        "updated_at": help_item.updated_at,
        "user": {
            "id": help_item.user.id,
            "email": help_item.user.email,
            "first_name": help_item.user.first_name,
            "last_name": help_item.user.last_name
        } if hasattr(help_item, "user") and help_item.user else None,
        "is_creator": True
    }
    
    # Return response
    return HelpResponse(**response_data)
