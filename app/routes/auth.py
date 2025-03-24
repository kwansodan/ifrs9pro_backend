from fastapi import APIRouter, Depends, HTTPException, status, Request, Form, Body
from fastapi.responses import HTMLResponse, RedirectResponse
from sqlalchemy.orm import Session
from datetime import datetime, timedelta
from app.database import get_db
from app.models import AccessRequest, User, RequestStatus, UserRole
from app.schemas import (
    EmailVerificationRequest,
    AccessRequestSubmit,
    AccessRequestResponse,
    AccessRequestUpdate,
    PasswordSetup,
    Token,
    LoginRequest,
    LoginResponse
    
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
from app.auth.email import (
    send_verification_email,
    send_admin_notification,
    send_invitation_email,
)
from typing import List
from app.config import settings
import os


router = APIRouter(tags=["auth"])

VALID_ADMIN_EMAILS = os.getenv("VALID_ADMIN_EMAILS", "admin@example.com").split(",")

@router.post("/request-access")
async def request_access(
    request_data: EmailVerificationRequest, db: Session = Depends(get_db)
):
    # Check if user already exists
    existing_user = db.query(User).filter(User.email == request_data.email).first()
    if existing_user:
        raise HTTPException(status_code=409, detail="Email already registered")
    
    # Check for existing pending request
    existing_request = (
        db.query(AccessRequest)
        .filter(
            AccessRequest.email == request_data.email,
            AccessRequest.status == RequestStatus.PENDING,
        )
        .first()
    )
    
    if existing_request:
        # Check if the existing request has a verified email
        if existing_request.is_email_verified:
            raise HTTPException(
                status_code=409, detail="Access request already submitted"
            )
        else:
            # Check if token is expired - ensure both are timezone-naive for comparison
            current_time = datetime.utcnow()
            token_expiry = existing_request.token_expiry
            
            # Convert to naive datetime if token_expiry is timezone-aware
            if token_expiry.tzinfo is not None:
                token_expiry = token_expiry.replace(tzinfo=None)
                
            is_expired = token_expiry < current_time
            
            # Generate a new token if expired
            if is_expired:
                token = create_email_verification_token(request_data.email)
                existing_request.token = token
                existing_request.token_expiry = datetime.utcnow() + timedelta(hours=24)
                db.commit()
            else:
                token = existing_request.token
                
            # Resend verification email
            await send_verification_email(request_data.email, token)
            return {"message": "Verification email sent"}
    
    # Create new access request
    token = create_email_verification_token(request_data.email)
    new_request = AccessRequest(
        email=request_data.email,
        token=token,
        token_expiry=datetime.utcnow() + timedelta(hours=24),
    )
    db.add(new_request)
    db.commit()
    
    # Send verification email
    await send_verification_email(request_data.email, token)
    return {"message": "Verification email sent"}

@router.get("/verify-email/{token}")
async def verify_email(token: str, db: Session = Depends(get_db)):
    try:
        token_data, token_type = decode_token(token)

        if token_type != "email_verification":
            raise HTTPException(status_code=400, detail="Invalid token type")

        access_request = (
            db.query(AccessRequest)
            .filter(
                AccessRequest.email == token_data.email,
                AccessRequest.status == RequestStatus.PENDING,
            )
            .first()
        )

        if not access_request:
            raise HTTPException(status_code=404, detail="Request not found")

        access_request.is_email_verified = True
        db.commit()

        return {
            "message": "Email successfully verified. Thank you for confirming your email address."
        }

    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/submit-admin-request/")
async def submit_admin_request(
    request_data: AccessRequestSubmit, db: Session = Depends(get_db)
):
    # Find the verified email request
    access_request = (
        db.query(AccessRequest)
        .filter(
            AccessRequest.email == request_data.email,
            AccessRequest.is_email_verified == True,
            AccessRequest.status == RequestStatus.PENDING,
        )
        .first()
    )
    if not access_request:
        raise HTTPException(status_code=404, detail="Verified email request not found")

    # Update the admin email if provided
    if request_data.admin_email:
        access_request.admin_email = request_data.admin_email

        # Check if admin email belongs to a valid admin user
        admin_user = (
            db.query(User)
            .filter(User.email == request_data.admin_email, User.is_admin == True)
            .first()
        )

        # If admin user is valid, notify them
        if admin_user:
            db.commit()
            await send_admin_notification(request_data.admin_email, request_data.email)

    db.commit()
    return AccessRequestResponse(
        id=access_request.id,
        email=access_request.email,
        admin_email=access_request.admin_email,
        status=access_request.status,
        created_at=access_request.created_at,
        is_email_verified=access_request.is_email_verified,
    )


@router.get("/admin/requests", response_model=List[AccessRequestResponse])
async def get_access_requests(
    db: Session = Depends(get_db), current_user: User = Depends(is_admin)
):
    access_requests = (
        db.query(AccessRequest).filter(AccessRequest.is_email_verified == True).all()
    )

    return access_requests


@router.put("/admin/requests/{request_id}")
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


@router.post("/set-password/{token}")
async def set_password(
    token: str, password_data: PasswordSetup, db: Session = Depends(get_db)
):
    password = password_data.password
    confirm_password = password_data.confirm_password

    if not password or not confirm_password:
        raise HTTPException(
            status_code=400, detail="Password and confirm password are required"
        )

    if password != confirm_password:
        raise HTTPException(status_code=400, detail="Passwords do not match")

    if len(password) < 8:
        raise HTTPException(
            status_code=400, detail="Password must be at least 8 characters"
        )

    try:
        token_data, token_type = decode_token(token)

        if token_type != "invitation":
            raise HTTPException(status_code=400, detail="Invalid token type")

        access_request = (
            db.query(AccessRequest)
            .filter(
                AccessRequest.email == token_data.email,
                AccessRequest.status == RequestStatus.APPROVED,
            )
            .first()
        )

        if not access_request:
            raise HTTPException(status_code=404, detail="Approved request not found")

        # Check if user already exists
        existing_user = (
            db.query(User).filter(User.email == access_request.email).first()
        )
        if existing_user:
            # Update existing user's password instead of creating new user
            existing_user.hashed_password = get_password_hash(password)
            # You might want to update other fields as needed
        else:
            # Create the user
            new_user = User(
                email=access_request.email,
                hashed_password=get_password_hash(password),
                role=access_request.role,
            )
            db.add(new_user)

        # Mark the request as complete
        access_request.status = RequestStatus.APPROVED

        db.commit()

        # Generate access token
        access_token = create_access_token(
            data={"sub": access_request.email},
            expires_delta=timedelta(hours=settings.INVITATION_EXPIRE_HOURS),
        )

        return {
            "message": "Password set successfully",
            "access_token": access_token,
            "token_type": "bearer",
        }

    except Exception as e:
        # More specific error handling
        if "UNIQUE constraint failed: users.email" in str(e):
            raise HTTPException(
                status_code=400,
                detail="An account with this email already exists. If you've already set up your password, please log in.",
            )
        raise HTTPException(status_code=400, detail=f"Error setting password: {str(e)}")


@router.post("/login", response_model=LoginResponse)
async def login(request: LoginRequest, db: Session = Depends(get_db)):
    user = db.query(User).filter(User.email == request.email).first()

    if not user or not verify_password(request.password, user.hashed_password):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect email or password",
            headers={"WWW-Authenticate": "Bearer"},
        )

    # Set last login
    user.last_login = datetime.utcnow()
    db.commit()
    db.refresh(user)

    access_token_expires = timedelta(minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES)

    # Include user info in the token
    token_data = {
        "sub": user.email,
        "id": user.id,
        "role": user.role,
        "is_active": user.is_active,
    }

    access_token = create_access_token(
        data=token_data, expires_delta=access_token_expires
    )

    # Decode the token for sending back user info
    decoded_token = decode_token(access_token)
    access_request = db.query(AccessRequest).filter(AccessRequest.email == user.email).first()
    
    access_request_status = None
    if access_request is not None:
        access_request_status = access_request.status

    return {
        "access_token": access_token,
        "token_type": "bearer",
        "expires_in": access_token_expires.total_seconds(),
        "user": {
            "id": user.id,
            "first_name": user.first_name,
            "last_name": user.last_name,
            "email": user.email,
            "recovery_email": user.recovery_email,
            "role": user.role,
            "is_active": user.is_active,
            "access_request_status": access_request_status,
        }
        
    }
