from __future__ import annotations

import importlib.util
from pathlib import Path
from typing import Any


def _load_83565_parser():
    current_dir = Path(__file__).parent
    source = current_dir / "11_83565.py"
    spec = importlib.util.spec_from_file_location("splash_parser_11_83565_alias", source)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Failed to load parser module: {source}")

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    parse_func = getattr(module, "parse", None)
    if parse_func is None or not callable(parse_func):
        raise RuntimeError(f"Parser file {source} does not define callable parse(text)")
    return parse_func


_parse_83565 = _load_83565_parser()


def parse(text: str) -> dict[str, Any]:
    # Version 11.83082 exports the same result layout in this workflow.
    return _parse_83565(text)
