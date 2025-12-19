"""add subscription fields to users

Revision ID: d263405e3d21
Revises: 4d28afa6976a
Create Date: 2025-12-16 23:23:31.519380

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'd263405e3d21'
down_revision: Union[str, None] = '4d28afa6976a'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade():
    op.add_column('users', sa.Column('paystack_customer_code', sa.String(), nullable=True))
    op.add_column('users', sa.Column('current_subscription_id', sa.Integer(), nullable=True))
    op.add_column('users', sa.Column('subscription_status', sa.String(), nullable=True))

def downgrade():
    op.drop_column('users', 'subscription_status')
    op.drop_column('users', 'current_subscription_id')
    op.drop_column('users', 'paystack_customer_code')