"""link client table and other loans table

Revision ID: 4ee48a123012
Revises: d9eeca25536f
Create Date: 2025-03-16 19:55:35.673530

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "4ee48a123012"
down_revision: Union[str, None] = "d9eeca25536f"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column("other_loans", sa.Column("client_id", sa.Integer(), nullable=False))
    op.create_foreign_key(None, "other_loans", "clients", ["client_id"], ["id"])
    # ### end Alembic commands ###


def downgrade() -> None:
    """Downgrade schema."""
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_constraint(None, "other_loans", type_="foreignkey")
    op.drop_column("other_loans", "client_id")
    # ### end Alembic commands ###
