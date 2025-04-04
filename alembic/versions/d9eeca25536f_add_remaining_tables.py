"""Add remaining tables

Revision ID: d9eeca25536f
Revises: 0462c8870a88
Create Date: 2025-03-16 19:52:50.312453

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "d9eeca25536f"
down_revision: Union[str, None] = "0462c8870a88"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column(
        "securities", sa.Column("cash_or_non_cash", sa.String(), nullable=True)
    )
    op.drop_column("securities", "security_type")
    # ### end Alembic commands ###


def downgrade() -> None:
    """Downgrade schema."""
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column(
        "securities",
        sa.Column("security_type", sa.VARCHAR(), autoincrement=False, nullable=True),
    )
    op.drop_column("securities", "cash_or_non_cash")
    # ### end Alembic commands ###
