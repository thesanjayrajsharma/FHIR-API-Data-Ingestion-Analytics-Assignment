from __future__ import annotations
import os
from dataclasses import dataclass
from typing import Dict, List
import yaml

DEFAULT_CONFIG = os.path.join(os.path.dirname(__file__), "..", "..", "config", "pipeline_config.yml")


@dataclass(frozen=True)
class Config:
    raw: dict

    # ---- FHIR ----
    @property
    def fhir(self) -> dict:
        return self.raw["fhir"]

    @property
    def resources(self) -> List[str]:
        return [r["name"] for r in self.raw["resources"]]

    # ---- storage ----
    @property
    def catalog(self) -> str:
        return self.raw["storage"]["catalog"]

    @property
    def schemas(self) -> Dict[str, str]:
        return self.raw["storage"]["schemas"]

    def schema(self, layer: str) -> str:
        return f"{self.catalog}.{self.schemas[layer]}"

    def table(self, layer: str, name: str) -> str:
        """Fully qualified table name, e.g. table('silver','patient')."""
        return f"{self.schema(layer)}.{name.lower()}"

    @property
    def raw_root(self) -> str:
        return f"/Volumes/{self.catalog}/{self.schemas['raw']}/{self.raw['storage']['raw_volume']}"


def load_config(path: str = DEFAULT_CONFIG, overrides: dict | None = None) -> Config:
    with open(path, "r", encoding="utf-8") as fh:
        raw = yaml.safe_load(fh)
    for dotted, value in (overrides or {}).items():
        node = raw
        *parents, leaf = dotted.split(".")
        for p in parents:
            node = node[p]
        node[leaf] = value
    return Config(raw)
