# app/main.py
import os
from fastapi import FastAPI, Depends
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from app.database import get_db, init_db
from app.routes import auth, portfolio
from app.models import User, UserRole
from app.auth.utils import get_password_hash

# Initialize database tables explicitly before the app starts
init_db()

app = FastAPI()

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
app.include_router(portfolio.router)

@app.get("/")
async def root():
    return {"message": "Welcome to IFRS9Pro API"}

@app.get("/dashboard")
async def dashboard():
    return {"message": "Welcome to your dashboard"}

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
                role=UserRole.ADMIN
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
    # Create admin user on startup
    create_admin_user()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
