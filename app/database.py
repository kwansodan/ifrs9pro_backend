from sqlalchemy import create_engine, event
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
from app.config import settings
from sqlalchemy.orm import declared_attr
from sqlalchemy import (
    Column,
    ForeignKey,
    Integer,
)
from contextvars import ContextVar
from contextlib import contextmanager
from typing import Optional, Generator
import logging

logger = logging.getLogger(__name__)

# Context variable for tracking current tenant ID (thread-safe)
current_tenant_id: ContextVar[Optional[int]] = ContextVar('current_tenant_id', default=None)


engine = create_engine(settings.SQLALCHEMY_DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = declarative_base()


class TenantMixin:
    """
    Mixin for tenant-scoped models.
    
    Inherit from this mixin for any model that should be isolated by tenant:
    - User data (portfolios, loans, clients, etc.)
    - Reports and analytics
    - Quality issues and feedback
    
    Do NOT use for:
    - Tenant model itself
    - Global configuration (SubscriptionPlan, DefaultDefinition, etc.)
    - Models that inherit tenant from parent via FK
    
    Example:
        class Portfolio(TenantMixin, Base):
            __tablename__ = "portfolios"
            id = Column(Integer, primary_key=True)
            name = Column(String)
            # tenant_id is automatically added by TenantMixin
    """
    @declared_attr
    def tenant_id(cls):
        # OnDelete CASCADE ensures if a tenant is deleted, their data is gone.
        return Column(Integer, ForeignKey("tenants.id", ondelete="CASCADE"), nullable=False, index=True)


def get_current_tenant_id() -> Optional[int]:
    """
    Get the current tenant ID from context.
    Returns None if no tenant context is set.
    """
    return current_tenant_id.get()


@contextmanager
def set_tenant_context(tenant_id: int) -> Generator[None, None, None]:
    """
    Context manager to manually set tenant context.
    
    Usage:
        with set_tenant_context(tenant_id):
            # All queries here will be filtered by tenant_id
            db.query(Portfolio).all()  # Automatically filtered
    
    This is useful for:
    - Webhook handlers (tenant determined from payload)
    - Background jobs (tenant passed as parameter)
    - Admin operations on specific tenant
    """
    token = current_tenant_id.set(tenant_id)
    try:
        yield
    finally:
        current_tenant_id.reset(token)


def get_db():
    """
    Non-tenant-scoped database session.
    
    Use this for:
    - Authentication/login (before tenant is known)
    - Superadmin routes (cross-tenant access)
    - Webhook handlers (before tenant is determined)
    - Queries on global models (Tenant, SubscriptionPlan, etc.)
    """
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()




# Import dependencies needed for the real signature



# SQLAlchemy Event Listener for Automatic Tenant Filtering
@event.listens_for(Session, "do_orm_execute")
def receive_do_orm_execute(orm_execute_state):
    """
    Automatically inject tenant_id filter into all SELECT queries.
    
    This event listener intercepts all ORM queries and adds a WHERE clause
    filtering by tenant_id if:
    1. A tenant context is set (current_tenant_id is not None)
    2. The model being queried has a tenant_id attribute (uses TenantMixin)
    
    Models without tenant_id are not affected (Tenant, SubscriptionPlan, etc.)
    
    This provides automatic tenant isolation without manual filtering.
    """
    tenant_id = current_tenant_id.get()
    
    # Only apply filter if tenant context is set
    if tenant_id is None:
        return
    
    # Only apply to SELECT statements (not INSERT, UPDATE, DELETE)
    if not orm_execute_state.is_select:
        return
    
    # Get the model being queried
    if len(orm_execute_state.statement.column_descriptions) == 0:
        return
    
    # Check if the primary entity has tenant_id attribute
    for description in orm_execute_state.statement.column_descriptions:
        entity = description.get('entity')
        if entity is None:
            continue
        
        # Check if this model has tenant_id column (uses TenantMixin)
        if hasattr(entity, 'tenant_id'):
            # Add tenant_id filter to the query
            orm_execute_state.statement = orm_execute_state.statement.filter(
                entity.tenant_id == tenant_id
            )
            logger.debug(f"Applied tenant filter: tenant_id={tenant_id} on {entity.__name__}")
            break


# SQLAlchemy Event Listener for Automatic Tenant ID Injection on Insert
@event.listens_for(Session, "before_flush")
def receive_before_flush(session, flush_context, instances):
    """
    Automatically inject tenant_id into new objects during flush.
    
    This ensures that when a user creates an object (Portfolio, Loan, etc.),
    it is automatically assigned to their current tenant without manual setting.
    """
    tenant_id = current_tenant_id.get()
    
    # We only auto-inject if a tenant context is active
    if tenant_id is None:
        return
    
    # Iterate over all new objects to be inserted
    for obj in session.new:
        # Check if the object is tenant-scoped (TenantMixin)
        if isinstance(obj, TenantMixin):
            # If tenant_id is missing, inject it
            if not getattr(obj, "tenant_id", None):
                obj.tenant_id = tenant_id
                logger.debug(f"Auto-injected tenant_id={tenant_id} into {obj.__class__.__name__}")


# Initialize the database
def init_db():
    # Import models here to avoid circular imports
    from app.models import User, AccessRequest

    # Base.metadata.create_all(bind=engine)
    pass