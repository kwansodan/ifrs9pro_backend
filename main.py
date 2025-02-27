from fastapi import FastAPI

app = FastAPI()


@app.get("/")
async def root():
    return {"message": "Hello World"}

# Flow
# @app.post("/request-access")
# @app.get("/verify-email/{token}")
# @app.post("/submit-admin-request/") use email and admin email here
# @app.get("/admin/requests") get requests
# @app.put("/admin/request") update request
# @app.put("/admin/role") update role
# Send invitation email after role update
# @app.put("/user") update password
# @app.get("/login")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
