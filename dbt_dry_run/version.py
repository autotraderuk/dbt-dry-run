__all__ = ("VERSION",)

import pkg_resources

try:
    VERSION = pkg_resources.get_distribution("dbt-dry-run").version
except pkg_resources.DistributionNotFound:
    VERSION = "0.0.0"
