"""Instage agent — the thin pipeline: diagnose → gate → reason → execute."""

from instate.agent.decide import ProcessingResult, drain_pending, process_failure

__all__ = ["ProcessingResult", "drain_pending", "process_failure"]
