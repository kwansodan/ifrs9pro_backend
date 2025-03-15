"""Decouple loan and client tables

Revision ID: 773b1bf97ef0
Revises: 3b9b10bb6513
Create Date: 2025-03-15 16:13:35.855550

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '773b1bf97ef0'
down_revision: Union[str, None] = '3b9b10bb6513'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    # Drop foreign key constraints first
    op.drop_constraint('clients_employee_id_fkey', 'clients', type_='foreignkey')
    op.drop_constraint('loans_client_id_fkey', 'loans', type_='foreignkey')
    op.drop_constraint('loans_employee_id_fkey', 'loans', type_='foreignkey')
    
    # Then alter column types
    op.alter_column('clients', 'employee_id',
               existing_type=sa.INTEGER(),
               type_=sa.String(),
               nullable=False)
    op.alter_column('loans', 'employee_id',
               existing_type=sa.INTEGER(),
               type_=sa.String(),
               nullable=False)
    
    # Finally drop the client_id column
    op.drop_column('loans', 'client_id')

def downgrade() -> None:
    """Downgrade schema."""
    # Add column first
    op.add_column('loans', sa.Column('client_id', sa.INTEGER(), autoincrement=False, nullable=False))
    
    # Change column types back
    op.alter_column('loans', 'employee_id',
               existing_type=sa.String(),
               type_=sa.INTEGER(),
               nullable=True)
    op.alter_column('clients', 'employee_id',
               existing_type=sa.String(),
               type_=sa.INTEGER(),
               nullable=True)
    
    # Recreate foreign key constraints
    op.create_foreign_key('loans_client_id_fkey', 'loans', 'clients', ['client_id'], ['id'])
    op.create_foreign_key('loans_employee_id_fkey', 'loans', 'users', ['employee_id'], ['id'])
    op.create_foreign_key('clients_employee_id_fkey', 'clients', 'users', ['employee_id'], ['id'])
