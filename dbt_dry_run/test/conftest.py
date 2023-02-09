from typing import Generator

import pytest

from dbt_dry_run import flags


@pytest.fixture
def default_flags() -> Generator[flags.Flags, None, None]:
    flags.reset_flags()
    yield flags._DEFAULT_FLAGS
    flags.reset_flags()
