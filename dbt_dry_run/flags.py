from dataclasses import dataclass
from typing import Optional

SKIP_NOT_COMPILED: bool = False
FULL_REFRESH: bool = False
EXTRA_CHECK_COLUMNS_METADATA_KEY: Optional[str] = None


@dataclass
class Flags:
    skip_not_compiled: bool = False
    full_refresh: bool = False
    extra_check_columns_metadata_key: Optional[str] = None


_DEFAULT_FLAGS = Flags()


def set_flags(flags: Flags) -> None:
    global SKIP_NOT_COMPILED
    global FULL_REFRESH
    global EXTRA_CHECK_COLUMNS_METADATA_KEY
    SKIP_NOT_COMPILED = flags.skip_not_compiled
    FULL_REFRESH = flags.full_refresh
    EXTRA_CHECK_COLUMNS_METADATA_KEY = flags.extra_check_columns_metadata_key


def reset_flags() -> None:
    set_flags(_DEFAULT_FLAGS)
