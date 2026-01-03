from fastapi import Depends
from sqlalchemy.orm import Session
from app.database import get_db, current_tenant_id
from app.auth.utils import get_current_active_user

async def get_tenant_db(
    current_user = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    Tenant-scoped database session dependency.
    
    Automatically filters all queries by the current user's tenant_id.
    
    Use this for:
    - All user-facing routes (portfolios, loans, reports, etc.)
    - Billing routes that query tenant-specific data
    - Any route that should only access current tenant's data
    """
    if not current_user:
        # Should be caught by Depends(get_current_active_user) if it enforces auth
        raise ValueError("get_tenant_db requires authenticated user")
    
    # Handle both ORM object and potential Pydantic model if user flow changes
    tenant_id = getattr(current_user, 'tenant_id', None)
    
    if tenant_id is None:
        raise ValueError(f"User {current_user.email} has no tenant_id")
    
    # Set tenant context for this request
    token = current_tenant_id.set(tenant_id)
    
    try:
        yield db
    finally:
        # Clear tenant context after request
        current_tenant_id.reset(token)
