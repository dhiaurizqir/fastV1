import yaml
from pathlib import Path
from typing import Dict, Any

BASE_DIR = Path(__file__).resolve().parent
PATHS_FILE = BASE_DIR / "paths.yml"
PARAMETERS_DIR = BASE_DIR / "parameters"


def _load_yaml(path: Path) -> Dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")
    with open(path, "r") as f:
        return yaml.safe_load(f)


def load_paths() -> Dict[str, Any]:
    return _load_yaml(PATHS_FILE)


def load_parameters() -> Dict[str, Dict[str, Any]]:
    params = {}
    for file in PARAMETERS_DIR.glob("*.yml"):
        params[file.stem] = _load_yaml(file)
    return params

def get_path(
    paths: dict,
    layer: str,
    namespace: str,
    dataset: str,
) -> str:
    try:
        return paths[layer][namespace][dataset]["path"]
    except KeyError as e:
        raise KeyError(
            f"Path not found for "
            f"layer={layer}, namespace={namespace}, dataset={dataset}"
        ) from e