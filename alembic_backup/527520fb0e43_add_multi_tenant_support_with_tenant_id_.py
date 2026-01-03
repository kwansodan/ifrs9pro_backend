from typing import Sequence, Union
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = '527520fb0e43'
down_revision: Union[str, None] = '476264579e83'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # 1. Drop legacy subscriptions table (dev-safe)
    op.execute("DROP TABLE IF EXISTS user_subscriptions CASCADE;")

    tables_with_tenant = [
        'access_requests',
        'clients',
        'feedback',
        'guarantees',
        'help',
        'loans',
        'portfolios',
        'quality_issues',
        'reports',
        'securities',
        'users',
    ]

    # 2. Add tenant_id correctly (nullable → backfill → NOT NULL)
    for table in tables_with_tenant:
        # add as nullable
        op.add_column(table, sa.Column('tenant_id', sa.Integer(), nullable=True))

        # backfill existing rows (dev default tenant = 1)
        op.execute(f"UPDATE {table} SET tenant_id = 1")

        # enforce NOT NULL
        op.alter_column(table, 'tenant_id', nullable=False)

        # index + FK
        op.create_index(f'ix_{table}_tenant_id', table, ['tenant_id'], unique=False)
        op.create_foreign_key(
            None,
            table,
            'tenants',
            ['tenant_id'],
            ['id'],
            ondelete='CASCADE',
        )

    # 3. Rewire subscription foreign keys
    op.drop_constraint('loans_subscription_id_fkey', 'loans', type_='foreignkey')
    op.create_foreign_key(
        None,
        'loans',
        'tenant_subscriptions',
        ['subscription_id'],
        ['id'],
    )

    op.drop_constraint('portfolios_subscription_id_fkey', 'portfolios', type_='foreignkey')
    op.create_foreign_key(
        None,
        'portfolios',
        'tenant_subscriptions',
        ['subscription_id'],
        ['id'],
    )

    op.drop_constraint(
        'subscription_usage_subscription_id_fkey',
        'subscription_usage',
        type_='foreignkey',
    )
    op.create_foreign_key(
        None,
        'subscription_usage',
        'tenant_subscriptions',
        ['subscription_id'],
        ['id'],
    )

    # 4. Remove obsolete user subscription columns
    op.drop_constraint('users_current_subscription_id_fkey', 'users', type_='foreignkey')
    op.drop_column('users', 'current_subscription_id')
    op.drop_column('users', 'subscription_status')
    op.drop_column('users', 'paystack_customer_code')



def downgrade() -> None:
    """Downgrade schema: remove tenant columns and recreate user_subscriptions table."""

    # Recreate user_subscriptions table
    op.execute("""
    CREATE TABLE user_subscriptions (
        id SERIAL PRIMARY KEY,
        user_id INTEGER NOT NULL REFERENCES users(id),
        plan_id INTEGER NOT NULL REFERENCES subscription_plans(id),
        paystack_subscription_code VARCHAR NOT NULL,
        paystack_customer_code VARCHAR,
        authorization_code VARCHAR,
        status VARCHAR NOT NULL,
        current_period_start TIMESTAMPTZ,
        current_period_end TIMESTAMPTZ,
        next_billing_date TIMESTAMPTZ,
        started_at TIMESTAMPTZ,
        cancelled_at TIMESTAMPTZ,
        created_at TIMESTAMPTZ DEFAULT now(),
        updated_at TIMESTAMPTZ
    );
    """)

    # Drop tenant_id columns and related indexes/foreign keys
    tables_with_tenant = [
        'access_requests', 'clients', 'feedback', 'guarantees', 'help',
        'loans', 'portfolios', 'quality_issues', 'reports', 'securities', 'users'
    ]

    for table in tables_with_tenant:
        op.drop_constraint(None, table, type_='foreignkey')
        op.drop_index(f'ix_{table}_tenant_id', table_name=table)
        op.drop_column(table, 'tenant_id')
