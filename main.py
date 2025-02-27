import os
from fastapi import FastAPI, Depends
from fastapi.middleware.cors import CORSMiddleware
from app.database import engine, Base
from app.routes import auth
from app.models import User, UserRole
from sqlalchemy.orm import Session
from app.database import get_db
from app.auth.utils import get_password_hash

app = FastAPI()

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Create tables
Base.metadata.create_all(bind=engine)

# Register routers
app.include_router(auth.router)

@app.get("/")
async def root():
    return {"message": "Welcome to IFRS9Pro API"}

@app.get("/dashboard")
async def dashboard():
    return {"message": "Welcome to your dashboard"}

@app.on_event("startup")
async def startup_event():
    # Create admin user if it doesn't exist
    db = next(get_db())
    admin_email = os.getenv("ADMIN_EMAIL", "admin@example.com")
    admin_password = os.getenv("ADMIN_PASSWORD", "admin123")
    
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

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
