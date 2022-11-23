from typing import List

from dbt_dry_run.linting.column_linting import (
    get_extra_documented_columns,
    get_undocumented_columns,
)
from dbt_dry_run.models import Table
from dbt_dry_run.models.manifest import ManifestColumn
from dbt_dry_run.test.utils import field_with_name


def test_get_extra_documented_columns_passes_if_no_extra_columns() -> None:
    table = Table(fields=[field_with_name("a")])
    manifest_columns = {"a": ManifestColumn(name="a", description="a column")}

    expected: List[str] = []
    actual = get_extra_documented_columns(manifest_columns, table)

    assert actual == expected


def test_get_extra_documented_columns_fails_if_extra_columns() -> None:
    table = Table(fields=[field_with_name("a")])
    manifest_columns = {
        "a": ManifestColumn(name="a", description="a column"),
        "b": ManifestColumn(name="a", description="an extra column"),
    }

    expected = ["Extra column in metadata: 'b'"]
    actual = get_extra_documented_columns(manifest_columns, table)

    assert actual == expected


def test_get_undocumented_columns_passes_if_all_columns_present() -> None:
    table = Table(fields=[field_with_name("a")])
    manifest_columns = {"a": ManifestColumn(name="a", description="a column")}

    expected: List[str] = []
    actual = get_undocumented_columns(manifest_columns, table)

    assert actual == expected


def test_get_undocumented_columns_fails_if_undocumented_columns() -> None:
    table = Table(fields=[field_with_name("a"), field_with_name("b")])
    manifest_columns = {
        "a": ManifestColumn(name="a", description="a column"),
        "c": ManifestColumn(name="c", description="an extra column"),
    }

    expected = ["Column not documented in metadata: 'b'"]
    actual = get_undocumented_columns(manifest_columns, table)

    assert actual == expected
