"""LLM failover: primary → secondary → policy default."""

from typing import Protocol


class Reasoner(Protocol):
    async def propose(self, context: dict) -> dict | None: ...


class FailoverReasoner:
    """Try primary then secondary; None means caller uses policy default."""

    def __init__(self, primary: Reasoner, secondary: Reasoner | None = None):
        self.primary = primary
        self.secondary = secondary
        self.last_provider: str | None = None

    async def propose(self, context: dict) -> dict | None:
        try:
            result = await self.primary.propose(context)
            if result is not None:
                self.last_provider = "primary"
                return result
        except Exception:
            pass
        if self.secondary is not None:
            try:
                result = await self.secondary.propose(context)
                if result is not None:
                    self.last_provider = "secondary"
                    return result
            except Exception:
                pass
        self.last_provider = "default"
        return None
