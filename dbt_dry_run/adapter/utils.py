from pathlib import Path


def _default_profiles_dir() -> Path:
    return (
        Path.cwd() if (Path.cwd() / "profiles.yml").exists() else Path.home() / ".dbt"
    )


def default_profiles_dir() -> str:
    return _default_profiles_dir().as_posix()
