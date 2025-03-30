import csv
import io
from datetime import datetime
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from fastapi.responses import StreamingResponse
from app.database import get_db
from app.models import AccessRequest, User, Feedback
from app.schemas import (
    AccessRequestSubmit,
    AccessRequestResponse,
    AccessRequestUpdate,
    UserResponse,
    UserCreate,
    UserUpdate,
)
from typing import List
from app.auth.email import (
    send_verification_email,
    send_admin_notification,
    send_invitation_email,
    send_password_setup_email,
)
from app.auth.utils import (
    create_email_verification_token,
    create_invitation_token,
    get_password_hash,
    verify_password,
    create_access_token,
    get_current_active_user,
    is_admin,
    decode_token,
)
from app.schemas import (
    FeedbackStatusUpdate,
    FeedbackResponse,
    FeedbackDetailResponse,
)


router = APIRouter(prefix="/admin", tags=["admin"])


# Handle access requests
@router.get("/requests", response_model=List[AccessRequestResponse])
async def get_access_requests(
    db: Session = Depends(get_db), current_user: User = Depends(is_admin)
):
    access_requests = (
        db.query(AccessRequest).filter(AccessRequest.is_email_verified == True).all()
    )

    return access_requests


@router.put("/requests/{request_id}")
async def update_access_request(
    request_id: int,
    request_update: AccessRequestUpdate,
    db: Session = Depends(get_db),
    current_user: User = Depends(is_admin),
):
    access_request = (
        db.query(AccessRequest).filter(AccessRequest.id == request_id).first()
    )

    if not access_request:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Request not found"
        )

    access_request.status = request_update.status

    if request_update.status == RequestStatus.APPROVED and request_update.role:
        access_request.role = request_update.role

        # Generate invitation token
        token = create_invitation_token(access_request.email)
        access_request.token = token
        access_request.token_expiry = datetime.utcnow() + timedelta(
            hours=settings.INVITATION_EXPIRE_HOURS
        )

        # Send invitation email
        await send_invitation_email(access_request.email, token)

    db.commit()

    return {"message": "Request updated successfully"}


@router.delete("/requests/{request_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_access_request(
    request_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_active_user),
):
    """
    Delete a specific access request by ID.
    """

    access_request = (
        db.query(AccessRequest).filter(AccessRequest.id == request_id).first()
    )

    if access_request:
        db.delete(access_request)
        db.commit()
    else:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Access request not found",
        )

    return None


# Handle user management
@router.get("/users", response_model=List[UserResponse])
async def get_users(
    db: Session = Depends(get_db), current_user: User = Depends(is_admin)
):
    users = db.query(User).all()

    return users

@router.get("/users/export", response_class=StreamingResponse)
async def export_users_csv(
    db: Session = Depends(get_db), 
    current_user: User = Depends(is_admin)
):
    """
    Export all users as a CSV file.
    Only accessible to admin users.
    """
    # Query all users
    users = db.query(User).all()
    
    # Create a StringIO object to write CSV data
    output = io.StringIO()
    writer = csv.writer(output)
    
    # Write header row
    writer.writerow([
        "ID", 
        "First Name",
        "Last Name",
        "Email", 
        "Recovery Email",
        "Role", 
        "Is Active", 
        "Last Login",
        "Created At",
        "Updated At"
    ])
    
    # Write user data
    for user in users:
        writer.writerow([
            user.id,
            user.first_name or "",
            user.last_name or "",
            user.email,
            user.recovery_email or "",
            user.role,
            user.is_active,
            user.last_login.strftime("%Y-%m-%d %H:%M:%S") if user.last_login else "",
            user.created_at.strftime("%Y-%m-%d %H:%M:%S") if user.created_at else "",
            user.updated_at.strftime("%Y-%m-%d %H:%M:%S") if user.updated_at else ""
        ])
    
    # Prepare the output
    output.seek(0)
    
    # Generate filename with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"users_export_{timestamp}.csv"
    
    # Return the CSV as a streaming response
    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"}
    )

@router.get("/users/{user_id}", response_model=UserResponse)
async def get_user(
    user_id: int, db: Session = Depends(get_db), current_user: User = Depends(is_admin)
):
    user = db.query(User).filter(User.id == user_id).first()

    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    return user


@router.post("/users", response_model=UserResponse)
async def create_user(
    user_create: UserCreate,
    db: Session = Depends(get_db),
    current_user: User = Depends(is_admin),
):
    # Check if user with same email already exists
    existing_user = db.query(User).filter(User.email == user_create.email).first()
    if existing_user:
        raise HTTPException(status_code=400, detail="Email already registered")

    # Create new user
    user_data = user_create.dict(exclude={"portfolio_id"})

    # Convert enum values to strings if necessary
    if user_data.get("role"):
        user_data["role"] = user_data["role"].value

    new_user = User(**user_data)
    db.add(new_user)
    db.commit()
    db.refresh(new_user)

    # Link portfolio if provided
    if user_create.portfolio_id:
        portfolio = (
            db.query(Portfolio).filter(Portfolio.id == user_create.portfolio_id).first()
        )
        if not portfolio:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail="Portfolio not found"
            )
        portfolio.user_id = new_user.id
        db.commit()

    # create token containing users email
    token = create_invitation_token(new_user.email)

    # Send email for password setup
    send_password_setup_email(new_user.email, token)

    return new_user


@router.delete("/users/{user_id}", status_code=204)
async def delete_user(
    user_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(is_admin),
):
    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="User not found"
        )

    # Delete the user
    db.delete(user)
    db.commit()

    return None  #


@router.put("/users/{user_id}", response_model=UserResponse)
async def update_user(
    user_id: int,
    user_update: UserUpdate,
    db: Session = Depends(get_db),
    current_user: User = Depends(is_admin),
):
    user = db.query(User).filter(User.id == user_id).first()

    if not user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="User not found"
        )

    # Update provided fields
    update_data = user_update.dict(exclude_unset=True)

    # Convert enum values to strings
    if "role" in update_data and update_data["role"]:
        update_data["role"] = update_data["role"].value

    for key, value in update_data.items():
        setattr(user, key, value)

    db.commit()
    db.refresh(user)
    return user





# Feedback routes
@router.get("/feedback", response_model=List[FeedbackResponse])
async def admin_get_all_feedback(
    db: Session = Depends(get_db),
    current_user: User = Depends(is_admin),
):
    """
    Admin endpoint to get all feedback entries
    """
    feedback_list = db.query(Feedback).all()

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


@router.put("/feedback/{feedback_id}/status", response_model=FeedbackResponse)
async def update_feedback_status(
    feedback_id: int,
    status_update: FeedbackStatusUpdate,
    db: Session = Depends(get_db),
    current_user: User = Depends(is_admin),
):
    """
    Admin endpoint to update feedback status
    """
    feedback = db.query(Feedback).filter(Feedback.id == feedback_id).first()

    if not feedback:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Feedback not found"
        )

    # Update status
    feedback.status = status_update.status.value
    db.commit()
    db.refresh(feedback)

    # Prepare response
    liked_by_current_user = current_user in feedback.liked_by
    like_count = len(feedback.liked_by)

    feedback_response = FeedbackResponse.from_orm(feedback)
    feedback_response.like_count = like_count
    feedback_response.is_liked_by_user = liked_by_current_user

    return feedback_response
