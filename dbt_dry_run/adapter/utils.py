from pathlib import Path
import os


def _default_profiles_dir() -> Path:
    if os.environ.get("DBT_PROFILES_DIR"):
        return Path(os.environ["DBT_PROFILES_DIR"])
    return (
        Path.cwd() if (Path.cwd() / "profiles.yml").exists() else Path.home() / ".dbt"
    )


def default_profiles_dir() -> str:
    return _default_profiles_dir().as_posix()
