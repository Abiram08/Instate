"""Chaos harness — kill at every stage (§15).

Not just mid-action: mid-webhook-verify, mid-gate-lock, mid-rebuild.
Each helper raises ChaosKill mid-function; the caller must handle it
and the test asserts the system recovers.
"""

from contextlib import asynccontextmanager


class ChaosKill(Exception):
    pass


class ChaosHarness:
    def __init__(self, kill_points: set[str] | None = None):
        self.kill_points = kill_points or set()
        self.hits: list[str] = []

    def maybe_kill(self, point: str):
        if point in self.kill_points:
            self.hits.append(point)
            raise ChaosKill(f"chaos at {point}")

    @asynccontextmanager
    async def stage(self, name: str):
        self.maybe_kill(f"before:{name}")
        try:
            yield
        finally:
            self.maybe_kill(f"after:{name}")
