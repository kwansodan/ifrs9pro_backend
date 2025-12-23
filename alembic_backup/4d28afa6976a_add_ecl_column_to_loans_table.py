"""add ecl column to loans table

Revision ID: 4d28afa6976a
Revises: d59ab5d0a61a
Create Date: 2025-11-06 14:43:13.105409

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '4d28afa6976a'
down_revision: Union[str, None] = 'd59ab5d0a61a'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass
