"""
Deprecated migration filename kept as a no-op compatibility shim.
"""

from mongodb_migrations.base import BaseMigration


class Migration(BaseMigration):
    def upgrade(self):
        return

    def downgrade(self):
        raise RuntimeError("Downgrade is not supported for this migration.")
