"""Source contracts: the YAML that says what each replicated table is, and how to prove the lake matches it."""
import yaml

from nexus.paths import ROOT

CONTRACTS = ROOT / "contracts"


def load_sources() -> list[dict]:
    return yaml.safe_load((CONTRACTS / "sources.yml").read_text())


def source(name: str) -> dict:
    return next(s for s in load_sources() if s["table"] == name)


def load_quality() -> dict:
    return yaml.safe_load((CONTRACTS / "quality.yml").read_text())
