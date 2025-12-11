# Simple factory helpers — adapt to your ORM models
import random
import string
from datetime import datetime

try:
    from app.models import User  # <- update to your models module
except Exception:
    User = None


def random_email():
    return f"test+{''.join(random.choices(string.ascii_lowercase, k=6))}@example.com"


def create_user(db_session, **kwargs):
    """Create and persist a user for tests. Update fields as per your model."""
    defaults = {
        'email': kwargs.pop('email', random_email()),
        'password_hash': kwargs.pop('password_hash', 'fakehashed'),
        'first_name': kwargs.pop('first_name', 'Test'),
        'last_name': kwargs.pop('last_name', 'User'),
        'role': kwargs.pop('role', 'user'),
        'is_active': kwargs.pop('is_active', True),
    }

    if User is None:
        # Fallback: return a dict. Tests using create_user should be adapted if your model differs.
        user = defaults
        user['id'] = 1
        return user

    user = User(**{**defaults, **kwargs})
    db_session.add(user)
    db_session.commit()
    db_session.refresh(user)
    return user