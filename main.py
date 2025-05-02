import os
import logging
from fastapi import Depends, FastAPI, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.httpsredirect import HTTPSRedirectMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from sqlalchemy.orm import Session
from app.database import get_db, init_db
# Import all routers including websocket
from app.routes import auth, portfolio, admin, reports, dashboard, user as user_router, quality_issues, websocket
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
import pickle
import numpy as np
import asyncio


# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("app")

# Initialize the FastAPI app first
app = FastAPI()

# Add a health check endpoint immediately
@app.get("/health")
async def health_check():
    """Simple health check endpoint for Azure health probes"""
    return {"status": "healthy"}

# Add GZip compression middleware
app.add_middleware(GZipMiddleware, minimum_size=1000)

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://ifrs9pro.service4gh.com", "http://localhost:5173"],
    allow_credentials=True,
    allow_methods=["POST", "PUT", "DELETE", "GET"],
    allow_headers=["*"],
)

# Register routers
app.include_router(auth.router)
app.include_router(admin.router)
app.include_router(portfolio.router)
app.include_router(reports.router)
app.include_router(dashboard.router)
app.include_router(user_router.router)
app.include_router(quality_issues.router)
app.include_router(websocket.router)

@app.get("/")
async def root():
    return {"message": "Welcome to IFRS9Pro API"}

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")

@app.post("/token")
async def get_token(
    form_data: OAuth2PasswordRequestForm = Depends(), db: Session = Depends(get_db)
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
    access_token = create_access_token(
        data=token_data, expires_delta=access_token_expires
    )

    # Return in format expected by OAuth2
    return {"access_token": access_token, "token_type": "bearer"}

# Global model variable for lazy loading
model = None

def get_model():
    """Lazy-load the ML model only when needed"""
    global model
    if model is None:
        try:
            logger.info("Loading ML model...")
            with open("app/ml_models/logistic_model.pkl", "rb") as file:
                model = pickle.load(file)
            logger.info("ML model loaded successfully")
        except Exception as e:
            logger.error(f"Error loading model: {e}")
            # Return a simple fallback model that won't break the application
            return None
    return model

# Commented out prediction endpoint - uncomment when ready
# @app.get("/predict")
# def predict_default_probability(year_of_birth: int):
#     """
#     Calculate and return the probability of default based on year of birth.
#     """
#     try:
#         model = get_model()  # Lazy load the model
#         if not model:
#             raise HTTPException(status_code=503, detail="Model not available")
#         
#         # Prepare input for the model
#         X_new = np.array([[year_of_birth]])
#         
#         # Get prediction and probability from model
#         prediction = model.predict(X_new)[0]
#         probability = model.predict_proba(X_new)[0][1]  # Probability of default
#         
#         return {
#             "year_of_birth": year_of_birth,
#             "default_prediction": int(prediction),
#             "probability_of_default": round(probability, 4)
#         }
#     
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=str(e))
@app.on_event("startup")
async def init_db_async():
    """Initialize database tables asynchronously"""
    try:
        logger.info("Initializing database...")
        init_db()
        logger.info("Database initialized successfully")
    except Exception as e:
        logger.error(f"Error initializing database: {e}")
        
@app.on_event("startup")
async def create_admin_user_async():
    """Create admin user asynchronously with better error handling"""
    try:
        logger.info("Creating admin user if needed...")
        # Get a new session
        db = next(get_db())
        try:
            admin_email = os.getenv("ADMIN_EMAIL")
            admin_password = os.getenv("ADMIN_PASSWORD")

            if not admin_email or not admin_password:
                logger.warning("Admin credentials not provided in environment variables")
                return

            # Query with FOR UPDATE to lock the row and prevent race conditions
            existing_admin = db.query(User).filter(User.email == admin_email).first()
            
            if not existing_admin:
                try:
                    admin_user = User(
                        email=admin_email,
                        hashed_password=get_password_hash(admin_password),
                        role=UserRole.ADMIN,
                        is_active=True,  # Ensure the admin is active
                    )
                    db.add(admin_user)
                    db.commit()
                    logger.info(f"Admin user created: {admin_email}")
                except Exception as e:
                    db.rollback()
                    # Check if it's a unique violation
                    if "UniqueViolation" in str(e) or "duplicate key" in str(e):
                        logger.info(f"Admin user was created by another process, ignoring: {admin_email}")
                    else:
                        # Re-raise if it's not a unique violation
                        raise
            else:
                logger.info("Admin user already exists")
                
                # Optionally update admin password if needed
                # Uncomment if you want to update the admin password on startup
                # if not verify_password(admin_password, existing_admin.hashed_password):
                #     existing_admin.hashed_password = get_password_hash(admin_password)
                #     db.commit()
                #     logger.info("Admin password updated")
                
        except Exception as e:
            db.rollback()
            logger.error(f"Error creating admin user: {e}")
        finally:
            db.close()
    except Exception as e:
        logger.error(f"Error in create_admin_user_async: {e}")
        
@app.on_event("startup")
async def startup_event():
    """
    Application startup event handler
    - First responds to health checks
    """
    logger.info("Application startup event triggered")



from fastapi.responses import JSONResponse
from fastapi.requests import Request

@app.options("/{rest_of_path:path}")
async def preflight_handler(request: Request):
    return JSONResponse(
        status_code=200,
        headers={
            "Access-Control-Allow-Origin": "https://ifrs9pro.service4gh.com",
            "Access-Control-Allow-Methods": "POST, GET, OPTIONS, PUT, DELETE",
            "Access-Control-Allow-Headers": "*",
            "Access-Control-Allow-Credentials": "true",
        }
    )

if __name__ == "__main__":
    import uvicorn
    import os 
    port = int(os.environ.get("PORT", 8000))

    uvicorn.run(
        app, 
        host="0.0.0.0", 
        port=port, 
        forwarded_allow_ips="*",
        proxy_headers=True
    )
