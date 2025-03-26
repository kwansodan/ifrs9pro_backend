import os
from fastapi import Depends, FastAPI, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.httpsredirect import HTTPSRedirectMiddleware
from sqlalchemy.orm import Session
from app.database import get_db, init_db
from app.routes import auth, portfolio, admin, reports, dashboard
from app.models import User, UserRole
from app.auth.utils import get_password_hash
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from datetime import datetime, timedelta
from app.auth.utils import (
    get_password_hash,
    verify_password,
    create_access_token,
)
from app.config import settings

# Initialize database tables explicitly before the app starts
init_db()

app = FastAPI()

# Redirect to https
# app.add_middleware(HTTPSRedirectMiddleware)

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Register routers
app.include_router(auth.router)
app.include_router(admin.router)
app.include_router(portfolio.router)
app.include_router(reports.router)
app.include_router(dashboard.router)


@app.get("/")
async def root():
    return {"message": "Welcome to IFRS9Pro API"}


oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")

@app.post("/token")
async def get_token(
    form_data: OAuth2PasswordRequestForm = Depends(),
    db: Session = Depends(get_db)
):
    # Reuse same logic as your login endpoint
    user = db.query(User).filter(User.email == form_data.username).first()
    if not user or not verify_password(form_data.password, user.hashed_password):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    # Update last login
    user.last_login = datetime.utcnow()
    db.commit()
    
    # Create token
    access_token_expires = timedelta(minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES)
    token_data = {
        "sub": user.email,
        "id": user.id,
        "role": user.role,
        "is_active": user.is_active,
    }
    access_token = create_access_token(data=token_data, expires_delta=access_token_expires)
    
    # Return in format expected by OAuth2
    return {
        "access_token": access_token,
        "token_type": "bearer"
    }

# Create admin user function
def create_admin_user():
    # Get a new session
    db = SessionLocal = next(get_db())
    try:
        admin_email = os.getenv("ADMIN_EMAIL")
        admin_password = os.getenv("ADMIN_PASSWORD")

        existing_admin = db.query(User).filter(User.email == admin_email).first()
        if not existing_admin:
            admin_user = User(
                email=admin_email,
                hashed_password=get_password_hash(admin_password),
                role=UserRole.ADMIN,
            )
            db.add(admin_user)
            db.commit()
            print(f"Admin user created: {admin_email}")
    except Exception as e:
        db.rollback()
        print(f"Error creating admin user: {e}")
    finally:
        db.close()


@app.on_event("startup")
async def startup_event():
    create_admin_user()


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        app, 
        host="0.0.0.0", 
        port=8000,
        forwarded_allow_ips="*",      
        proxy_headers=True            
    )
