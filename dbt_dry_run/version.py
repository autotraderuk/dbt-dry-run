__all__ = ("VERSION",)

from importlib import metadata

try:
    VERSION = metadata.version("dbt-dry-run")
except metadata.PackageNotFoundError:
    VERSION = "0.0.0"
