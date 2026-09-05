"""Chaos harness: raise ChaosKill at configured points; callers must handle it."""

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
