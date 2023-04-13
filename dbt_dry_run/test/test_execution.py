from typing import Optional
from unittest.mock import MagicMock

import pytest

from dbt_dry_run import flags
from dbt_dry_run.execution import should_check_columns
from dbt_dry_run.flags import Flags
from dbt_dry_run.models.manifest import NodeMeta

SOME_KEY = "SOME_KEY"


@pytest.mark.parametrize(
    "meta_value, expected_value", [(False, False), (True, True), (None, False)]
)
def test_should_check_columns_return_default_key_value(
    default_flags: Flags, meta_value: bool, expected_value: bool
) -> None:
    flags.set_flags(flags.Flags(extra_check_columns_metadata_key=None))
    node = MagicMock()
    node.get_combined_metadata.return_value = meta_value
    assert should_check_columns(node) is expected_value


@pytest.mark.parametrize(
    "meta_value, expected_value", [(False, False), (True, True), (None, False)]
)
def test_should_check_columns_return_extra_key_value_if_default_not_specified(
    default_flags: Flags, meta_value: bool, expected_value: bool
) -> None:
    flags.set_flags(flags.Flags(extra_check_columns_metadata_key=SOME_KEY))
    node = MagicMock()

    def combined_metadata_side_effect(arg: str) -> Optional[bool]:
        if arg == NodeMeta.DEFAULT_CHECK_COLUMNS_KEY:
            return None
        if arg == SOME_KEY:
            return meta_value
        raise RuntimeError(f"Unrecognised key called to mock {arg}")

    node.get_combined_metadata.side_effect = combined_metadata_side_effect
    assert should_check_columns(node) is expected_value


@pytest.mark.parametrize("meta_value, expected_value", [(False, False), (True, True)])
def test_should_check_columns_defaults_to_default_meta_key(
    default_flags: Flags, meta_value: bool, expected_value: bool
) -> None:
    flags.set_flags(flags.Flags(extra_check_columns_metadata_key=SOME_KEY))
    node = MagicMock()

    def combined_metadata_side_effect(arg: str) -> Optional[bool]:
        if arg == NodeMeta.DEFAULT_CHECK_COLUMNS_KEY:
            return meta_value
        if arg == SOME_KEY:
            return not meta_value
        raise RuntimeError(f"Unrecognised key called to mock {arg}")

    node.get_combined_metadata.side_effect = combined_metadata_side_effect
    assert should_check_columns(node) is expected_value


@pytest.mark.parametrize(
    "meta_value, expected_value", [("foo", True), ("", False), (0, False), (1, True)]
)
def test_should_check_columns_casts_non_bools(
    default_flags: Flags, meta_value: bool, expected_value: bool
) -> None:
    flags.set_flags(flags.Flags(extra_check_columns_metadata_key=None))
    node = MagicMock()
    node.get_combined_metadata.return_value = meta_value
    assert should_check_columns(node) is expected_value
