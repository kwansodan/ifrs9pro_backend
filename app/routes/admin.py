import csv
import io
from datetime import datetime, timedelta, timezone
from fastapi import APIRouter, Depends, HTTPException, status, Response
from sqlalchemy.orm import Session
from fastapi.responses import JSONResponse, StreamingResponse
from app.database import get_db
from app.models import (
    AccessRequest,
    User,
    Feedback,
    Help,
    RequestStatus,
    Portfolio,
    UserSubscription,
    SubscriptionUsage,
    SubscriptionPlan,
)
from app.schemas import (
    AccessRequestSubmit,
    AccessRequestResponse,
    AccessRequestUpdate,
    UserResponse,
    UserCreate,
    UserUpdate,
    HelpStatusEnum,
    HelpResponse,
    HelpUpdate,
    HelpStatusUpdate,
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
from app.utils.billing import require_active_subscription
from app.schemas import (
    FeedbackStatusUpdate,
    FeedbackResponse,

)
from app.config import settings

router = APIRouter(prefix="/admin", tags=["admin"])


# Handle access requests
@router.get("/requests", 
            response_model=List[AccessRequestResponse],
            responses={401: {"description": "Not authenticated"}},
            operation_id="list_all_access_requests",  
            description="Get all access requests")
async def get_access_requests(
    db: Session = Depends(get_db), current_user: User = Depends(is_admin)
):
    access_requests = (
        db.query(AccessRequest).filter(AccessRequest.is_email_verified == True).all()
    )

    return access_requests



# Handle user management
@router.get("/users",  
            description="Get all users in db", 
            response_model=List[UserResponse],
            responses={401: {"description": "Not authenticated"}})
async def get_users(
    db: Session = Depends(get_db), current_user: User = Depends(is_admin)
):
    users = db.query(User).all()

    return users


@router.get("/users/export",  
            description="Get all users and export them as csv", 
            response_class=StreamingResponse,
            responses={401: {"description": "Not authenticated"}})
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


@router.post("/users",
            description="create a user in db",
            response_model=UserResponse,
            responses={400: {"description": "Email already registered"},
                       401: {"description": "Not Authenticated"}},)
async def create_user(
    user_create: UserCreate,
    db: Session = Depends(get_db),
    current_user: User = Depends(is_admin),
    subscription: UserSubscription = Depends(require_active_subscription),
):
    # Check if user with same email already exists
    existing_user = db.query(User).filter(User.email == user_create.email).first()
    if existing_user:
        raise HTTPException(status_code=400, detail="Email already registered")

    # Enforce team member limit for this subscription
    usage = (
        db.query(SubscriptionUsage)
        .filter(SubscriptionUsage.subscription_id == subscription.id)
        .with_for_update()
        .first()
    )
    plan = db.query(SubscriptionPlan).filter(SubscriptionPlan.id == subscription.plan_id).first()

    if not usage or not plan:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Subscription usage or plan configuration missing.",
        )

    if usage.current_team_count >= plan.max_team_size:
        raise HTTPException(
            status_code=status.HTTP_402_PAYMENT_REQUIRED,
            detail="Team member limit reached for your subscription plan.",
        )

    # Create new user
    user_data = user_create.dict(exclude={"portfolio_id"})

    # Convert enum values to strings if necessary
    if user_data.get("role"):
        user_data["role"] = user_data["role"].value

    new_user = User(**user_data)
    db.add(new_user)
    db.commit()
    db.refresh(new_user)

    # Associate new user with same current subscription (team membership) and
    # increment team usage atomically.
    if current_user.current_subscription_id:
        new_user.current_subscription_id = current_user.current_subscription_id
        new_user.subscription_status = "active"
        usage.current_team_count += 1
        db.add(new_user)
        db.add(usage)
        db.commit()

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
    await send_password_setup_email(new_user.email, token)

    return new_user


# Feedback routes
@router.get("/feedback",
            description="Get all feedback entries",
            response_model=List[FeedbackResponse],
            responses={401: {"description": "Not authenticated"}})
async def admin_get_all_feedback(
    db: Session = Depends(get_db),
    current_user: User = Depends(is_admin),
):
    """
    Admin endpoint to get all feedback entries
    """
    feedback_list = db.query(Feedback).all()
    
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


# Help routes for admin
@router.get("/help",  
            description="Get all help entries", 
            response_model=List[HelpResponse],
            responses={401: {"description": "Not authenticated"}})
async def admin_get_all_help(
    db: Session = Depends(get_db),
    current_user: User = Depends(is_admin),
):
    """
    Admin endpoint to get all help entries
    """
    help_list = db.query(Help).all()
    
    # Prepare response with manual mapping
    response_data = []
    for help_item in help_list:
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
            "is_creator": help_item.user_id == current_user.id
        }
        
        # Create response object from dictionary
        response_data.append(HelpResponse(**help_dict))
    
    return response_data

@router.get("/help/{help_id}",  
            description="Get help entry by ID", 
            response_model=HelpResponse,
            responses={404: {"description": "Help entry not found"},
                       401: {"description": "Not authenticated"}},
            )
async def admin_get_help(
    help_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(is_admin),
):
    """
    Admin endpoint to get a specific help entry
    """
    help_item = db.query(Help).filter(Help.id == help_id).first()
    if not help_item:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Help request not found"
        )
    
    # Create response dictionary manually
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
        "is_creator": help_item.user_id == current_user.id
    }
    
    # Return response directly from dictionary
    return HelpResponse(**response_data)


@router.put("/help/{help_id}/status",  
            description="Get help status by ID", 
            response_model=HelpResponse,
            responses={404: {"description": "Help not found"},
                       401: {"description": "Not Authenticated"}},)
async def update_help_status(
    help_id: int,
    status_update: HelpStatusUpdate,
    db: Session = Depends(get_db),
    current_user: User = Depends(is_admin),
):
    """
    Admin endpoint to update help status
    """
    help_item = db.query(Help).filter(Help.id == help_id).first()
    if not help_item:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Help request not found"
        )
    
    # Update status
    help_item.status = status_update.status.value
    db.commit()
    db.refresh(help_item)
    
    # Create response dictionary manually
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
        "is_creator": help_item.user_id == current_user.id
    }
    
    # Return response directly from dictionary
    return HelpResponse(**response_data)


@router.delete("/help/{help_id}",  
            description="Delete a specific help entry by ID", 
            status_code=status.HTTP_204_NO_CONTENT,
            responses={404: {"description": "Help not found"},
                       401: {"description": "Not authenticated"},},)
async def admin_delete_help(
    help_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(is_admin),
):
    """
    Admin endpoint to delete any help entry
    """
    # Get the help
    help_item = db.query(Help).filter(Help.id == help_id).first()
    if not help_item:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Help request not found"
        )
    
    # Delete the help
    db.delete(help_item)
    db.commit()
    
    # Explicitly return an empty 204 response
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.delete("/requests/{request_id}", 
            description="Delete access requests",
            status_code=status.HTTP_204_NO_CONTENT,
            responses={404: {"description": "Request not found"},
                       401: {"description": "Not Authenticated"}},
            )
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

@router.put("/requests/{request_id}", 
            operation_id="update_specific_access_request",  
            description="Update access requests",
            responses={404: {"description": "Request not found"},
                       401: {"description": "Not authenticated"}},
            )
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
        access_request.token_expiry = datetime.now(timezone.utc) + timedelta(
            hours=settings.INVITATION_EXPIRE_HOURS
        )

        # Send invitation email
        await send_invitation_email(access_request.email, token)

    db.commit()

    return {"message": "Request updated successfully"}


@router.get("/users/{user_id}", 
            description="Get a specific user by ID",
            responses={404: {"description": "User not found"},
                       401: {"description": "Not authenticated"},},
            response_model=UserResponse)
async def get_user(
    user_id: int, db: Session = Depends(get_db), current_user: User = Depends(is_admin)
):
    user = db.query(User).filter(User.id == user_id).first()

    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    return user

@router.get("/feedback/{feedback_id}",
            description="Get a specific feedback by ID",
            response_model=FeedbackResponse,
            responses={404: {"description": "Feedback not found"},
                       401: {"description": "Not authenticated"}},
            )
async def admin_get_feedback(
    feedback_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(is_admin),
):
    """
    Admin endpoint to get a specific feedback entry
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

@router.put("/feedback/{feedback_id}/status",  
            description="Update a feedback status",
            response_model=FeedbackResponse,
            responses={404: {"description": "Feedback not found"},
                       401: {"description": "NOt Authenticated"}},)
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

@router.delete("/feedback/{feedback_id}",  
            description="Delete a feedback by ID",
            status_code=status.HTTP_204_NO_CONTENT,
            responses={404: {"description": "Feedback not found"},
                       401: {"description": "Not authenticated"}},)
async def admin_delete_feedback(
    feedback_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(is_admin),
):
    """
    Admin endpoint to delete any feedback entry
    """
    # Get the feedback
    feedback = db.query(Feedback).filter(Feedback.id == feedback_id).first()
    if not feedback:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Feedback not found"
        )
    
    # Delete the feedback
    db.delete(feedback)
    db.commit()
    
    # Return no content for successful deletion
    return None


@router.put("/users/{user_id}", 
            response_model=UserResponse,
            responses={404: {"description": "User not found"},
                       401: {"description": "Not Aunthenticated"}},)
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


@router.delete("/users/{user_id}",
            description="Delete a user by ID",
            status_code=204,
            responses={404: {"description": "User not found"},
                       401: {"description": "Not authenticated"}}
                       )
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

