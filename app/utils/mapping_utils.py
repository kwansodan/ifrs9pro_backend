from sqlalchemy.inspection import inspect as sqlalchemy_inspect
from sqlalchemy.exc import NoInspectionAvailable
from pydantic import BaseModel


def get_model_columns(model):
    # If user passed a Pydantic model, get fields from schema
    if isinstance(model, type) and issubclass(model, BaseModel):
        return list(model.__fields__.keys())

    # Otherwise treat as SQLAlchemy model
    try:
        mapper = sqlalchemy_inspect(model)
        return mapper.columns.keys()
    except NoInspectionAvailable:
        raise ValueError(f"Model {model} is not SQLAlchemy or Pydantic")
