from fastapi.openapi.utils import get_openapi
from main import app
import yaml

with open("openapi.yaml", "w") as f:
    yaml.dump(
        get_openapi(
            title=app.title,
            version=app.version,
            routes=app.routes
        ),
        f
    )
