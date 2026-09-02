"""Instate adapters — thin wrappers over the outside world (§14).

Both adapters are protocol-first: the core depends on the Protocol, never
on the concrete SDK. Tests inject fakes; production injects the real
clients. The concrete SDKs are imported lazily so the core runs (and the
full test suite passes) without them installed.
"""
