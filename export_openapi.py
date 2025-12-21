from fastapi.openapi.utils import get_openapi
from main import app   # adjust if your FastAPI entrypoint is not main.py
import yaml

openapi = get_openapi(
    title=app.title,
    version=app.version,
    routes=app.routes
)

with open("openapi.yaml", "w") as f:
    yaml.dump(openapi, f, sort_keys=False)
