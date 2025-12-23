from fastapi import APIRouter, Depends, HTTPException, status, BackgroundTasks
from sqlalchemy.orm import Session, joinedload
from sqlalchemy import func
from typing import List, Optional
from datetime import datetime

# Import your database and auth dependencies
from app.database import get_db
from app.auth.utils import get_current_active_user, require_super_admin
from app.models import (
    User, 
    UserRole, 
    Tenant, 
    Portfolio, 
    Loan, 
    SubscriptionPlan
)
from app.schemas import (
    TenantCreate, 
    TenantResponse, 
    TenantUpdate, 
    SystemStats
)

# Import Pydantic schemas (Define these in schemas.py or keep inline if preferred)
from pydantic import BaseModel, EmailStr

router = APIRouter(
    prefix="/superadmin",
    tags=["superadmin"],
    responses={404: {"description": "Not found"}},
)

# --- ENDPOINTS ---

@router.post("/tenants", 
                response_model=TenantResponse, 
                status_code=status.HTTP_201_CREATED,
                responses={401: {"description": "Not authenticated"},
                            409: {"description": "Tenant slug already exists"}},)
def onboard_new_tenant(
    tenant_in: TenantCreate, 
    db: Session = Depends(get_db), 
    admin_user: User = Depends(require_super_admin)
):
    """
    Onboard a new Tenant:
    1. Creates the Tenant record.
    2. Creates the initial Admin User for that Tenant.
    3. (Optional) Assigns a default subscription plan.
    """
    # 1. Check for duplicates
    existing_tenant = db.query(Tenant).filter(Tenant.slug == tenant_in.slug).first()
    if existing_tenant:
        raise HTTPException(status_code=400, detail="Tenant slug already exists.")

    existing_user = db.query(User).filter(User.email == tenant_in.admin_email).first()
    if existing_user:
        raise HTTPException(status_code=400, detail="User email already exists in the system.")

    # 2. Create Tenant
    new_tenant = Tenant(
        name=tenant_in.name,
        slug=tenant_in.slug,
        is_active=True
    )
    db.add(new_tenant)
    db.flush()  # Flush to get new_tenant.id

    # 3. Create Tenant Admin
    # Note: Password handling should follow your auth flow (e.g., send invite email)
    # Here we set a placeholder or trigger an email flow.
    new_admin = User(
        email=tenant_in.admin_email,
        first_name=tenant_in.admin_first_name,
        last_name=tenant_in.admin_last_name,
        role=UserRole.ADMIN,  # Tenant Admin
        tenant_id=new_tenant.id,
        is_active=True,
        hashed_password="CHANGE_ME_ON_FIRST_LOGIN" # In production, trigger a reset flow
    )
    db.add(new_admin)
    
    # 4. (Optional) Assign Plan - Linking to your existing SubscriptionPlan logic
    # This assumes you have refactored subscriptions to link to Tenant, not User
    default_plan = db.query(SubscriptionPlan).filter(SubscriptionPlan.name == tenant_in.plan_name).first()
    if default_plan:
        # Create subscription logic here...
        pass

    db.commit()
    db.refresh(new_tenant)

    # Return structure with counts (0 for new tenant)
    return TenantResponse(
        id=new_tenant.id,
        name=new_tenant.name,
        slug=new_tenant.slug,
        is_active=new_tenant.is_active,
        created_at=new_tenant.created_at,
        user_count=1,
        portfolio_count=0
    )

@router.get("/tenants", response_model=List[TenantResponse], responses={401: {"description": "Not authenticated"},})
def list_all_tenants(
    skip: int = 0, 
    limit: int = 100, 
    db: Session = Depends(get_db), 
    admin_user: User = Depends(require_super_admin)
):
    """
    List all tenants with summary statistics (user count, portfolio count).
    """
    # Optimized query with subqueries or counts
    tenants = db.query(Tenant).offset(skip).limit(limit).all()
    
    results = []
    for t in tenants:
        u_count = db.query(User).filter(User.tenant_id == t.id).count()
        p_count = db.query(Portfolio).filter(Portfolio.tenant_id == t.id).count()
        
        results.append(TenantResponse(
            id=t.id,
            name=t.name,
            slug=t.slug,
            is_active=t.is_active,
            created_at=t.created_at,
            user_count=u_count,
            portfolio_count=p_count
        ))
    
    return results

@router.get("/tenants/{tenant_id}", response_model=TenantResponse, responses={401: {"description": "Not authenticated"},})
def get_tenant_details(
    tenant_id: int, 
    db: Session = Depends(get_db), 
    admin_user: User = Depends(require_super_admin)
):
    tenant = db.query(Tenant).filter(Tenant.id == tenant_id).first()
    if not tenant:
        raise HTTPException(status_code=404, detail="Tenant not found")
        
    u_count = db.query(User).filter(User.tenant_id == tenant.id).count()
    p_count = db.query(Portfolio).filter(Portfolio.tenant_id == tenant.id).count()
    
    return TenantResponse(
        id=tenant.id,
        name=tenant.name,
        slug=tenant.slug,
        is_active=tenant.is_active,
        created_at=tenant.created_at,
        user_count=u_count,
        portfolio_count=p_count
    )

@router.patch("/tenants/{tenant_id}/status", responses={401: {"description": "Not authenticated"},})
def update_tenant_status(
    tenant_id: int, 
    status_update: TenantUpdate,
    db: Session = Depends(get_db), 
    admin_user: User = Depends(require_super_admin)
):
    """
    Suspend or Activate a tenant.
    This effectively locks/unlocks all users in that tenant if your deps.py checks tenant.is_active.
    """
    tenant = db.query(Tenant).filter(Tenant.id == tenant_id).first()
    if not tenant:
        raise HTTPException(status_code=404, detail="Tenant not found")

    if status_update.is_active is not None:
        tenant.is_active = status_update.is_active
    
    if status_update.name is not None:
        tenant.name = status_update.name

    db.commit()
    db.refresh(tenant)
    return {"message": "Tenant updated successfully", "is_active": tenant.is_active}


@router.get("/stats", 
            responses={401: {"description": "Not authenticated"},},
            response_model=SystemStats)
def get_global_system_stats(
    db: Session = Depends(get_db), 
    admin_user: User = Depends(require_super_admin)
):
    """
    High-level dashboard for the Super Admin.
    Aggregates data across ALL tenants.
    """
    total_tenants = db.query(Tenant).count()
    total_users = db.query(User).count()
    total_portfolios = db.query(Portfolio).count()
    total_loans = db.query(Loan).count()
    
    # Calculate Total Value Locked (Sum of all loan amounts across system)
    # Using coalesce to handle None results if table is empty
    total_value = db.query(func.sum(Loan.loan_amount)).scalar() or 0.0

    return SystemStats(
        total_tenants=total_tenants,
        total_users=total_users,
        total_portfolios=total_portfolios,
        total_loans=total_loans,
        total_value_locked=float(total_value)
    )