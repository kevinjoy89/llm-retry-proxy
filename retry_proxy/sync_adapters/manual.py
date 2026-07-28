from .base import PoolSyncAdapter


class ManualAdapter(PoolSyncAdapter):
    """Adapter for manually managed keys that require no upstream connection."""

    name = "manual"
    label = "手动管理"
    credential_fields = []
    capabilities = ["manual_keys"]

    async def connect(self, client, source, credentials):
        return {}

    async def fetch(self, client, source, session):
        return session, list(source.get("entries") or [])

    async def disconnect(self, client, source, session):
        pass

    def connected(self, session):
        return True

    def public_session(self, session):
        return {}

    def persistent_session(self, session):
        return {}
