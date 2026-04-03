'''
Deprecated migration filename kept as a no-op compatibility shim.
'''

from mongodb_migrations.base import BaseMigration


class Migration(BaseMigration):
    def upgrade(self) -> None:
        return

    def downgrade(self) -> None:
        raise RuntimeError('Downgrade is not supported for this migration.')
