"""Initial migration

Revision ID: 6e949910a0c6
Revises: 
Create Date: 2025-03-07 04:41:49.274267

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "6e949910a0c6"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column("portfolios", sa.Column("credit_source", sa.String(), nullable=True))
    op.add_column("portfolios", sa.Column("loan_assets", sa.String(), nullable=True))
    op.add_column(
        "portfolios", sa.Column("ecl_impairment_account", sa.String(), nullable=True)
    )
    # ### end Alembic commands ###


def downgrade() -> None:
    """Downgrade schema."""
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column("portfolios", "ecl_impairment_account")
    op.drop_column("portfolios", "loan_assets")
    op.drop_column("portfolios", "credit_source")
    # ### end Alembic commands ###
