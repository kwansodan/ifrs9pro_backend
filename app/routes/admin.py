from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from app.database import get_db
from app.models import AccessRequest, User
from app.schemas import (
    AccessRequestSubmit,
    AccessRequestResponse,
    AccessRequestUpdate,
    UserResponse,
    UserUpdate,
)
from typing import List
from app.auth.email import (
    send_verification_email,
    send_admin_notification,
    send_invitation_email,
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
        raise HTTPException(status_code=404, detail="Request not found")

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


@router.put("/users/{user_id}", response_model=UserResponse)
async def update_user(
    user_id: int,
    user_update: UserUpdate,
    db: Session = Depends(get_db),
    current_user: User = Depends(is_admin),
):
    user = db.query(User).filter(User.id == user_id).first()

    if not user:
        raise HTTPException(status_code=404, detail="User not found")

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
