"""Pacote raiz com aliases legados para compatibilidade.

O código principal foi reorganizado em subpacotes por domínio:
- ``src.cli``
- ``src.web``
- ``src.scraping``
- ``src.processing``
- ``src.structured``
- ``src.benchmarks``

Para preservar imports históricos como ``import src.api`` e
``from src import search``, este ``__init__`` pré-registra aliases de
submódulos em ``sys.modules``.
"""

from __future__ import annotations

import importlib
import importlib.abc
import importlib.util
import sys


_LEGACY_MODULES = {
    "acervos": "src.scraping.acervos",
    "api": "src.web.api",
    "benchmark_correcao": "src.benchmarks.correcao",
    "benchmark_ocr": "src.benchmarks.ocr",
    "benchmark_search": "src.benchmarks.search",
    "captcha": "src.scraping.captcha",
    "chunker": "src.processing.chunker",
    "correcao_provider": "src.processing.correcao_provider",
    "driver": "src.scraping.driver",
    "entities": "src.structured.entities",
    "evidence_store": "src.structured.repository",
    "hires_pipeline": "src.scraping.hires_pipeline",
    "indexer": "src.processing.indexer",
    "llm_correcao": "src.processing.llm_correcao",
    "llm_correcao_claude": "src.processing.llm_correcao_claude",
    "metadata_enrichment": "src.processing.metadata_enrichment",
    "ocr": "src.processing.ocr",
    "parallel": "src.scraping.parallel",
    "relations": "src.structured.relations",
    "scraper": "src.scraping.scraper",
    "search": "src.processing.search",
    "structured_models": "src.structured.models",
    "structured_service": "src.structured.service",
}
class _LegacyAliasLoader(importlib.abc.Loader):
    def __init__(self, fullname: str, target: str) -> None:
        self.fullname = fullname
        self.target = target

    def create_module(self, spec):  # type: ignore[override]
        return None

    def exec_module(self, module) -> None:  # type: ignore[override]
        target_module = importlib.import_module(self.target)
        sys.modules[self.fullname] = target_module


class _LegacyAliasFinder(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname: str, path, target=None):  # type: ignore[override]
        prefix = f"{__name__}."
        if not fullname.startswith(prefix):
            return None

        short_name = fullname[len(prefix) :]
        target_module = _LEGACY_MODULES.get(short_name)
        if target_module is None:
            return None

        loader = _LegacyAliasLoader(fullname, target_module)
        return importlib.util.spec_from_loader(fullname, loader)


if not any(isinstance(finder, _LegacyAliasFinder) for finder in sys.meta_path):
    sys.meta_path.insert(0, _LegacyAliasFinder())


def __getattr__(name: str):
    target = _LEGACY_MODULES.get(name)
    if target is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

    module = importlib.import_module(target)
    sys.modules[f"{__name__}.{name}"] = module
    return module


__all__ = ["config", *_LEGACY_MODULES]
