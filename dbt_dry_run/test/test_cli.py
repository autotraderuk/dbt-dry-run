from typing import Any

import pytest
from typer.testing import CliRunner

from dbt_dry_run import cli
from dbt_dry_run.exception import ManifestValidationError


@pytest.mark.parametrize(
    "vars_input, expected_vars",
    [
        ("{foo: 'bar'}", {"foo": "bar"}),
        ('{"foo": "bar"}', {"foo": "bar"}),
    ],
)
def test_cli_vars_accept_yaml_and_json_quoting(
    monkeypatch: pytest.MonkeyPatch, vars_input: str, expected_vars: dict[str, Any]
) -> None:
    captured_vars: dict[str, Any] = {}

    class FakeDbtArgs:
        def __init__(self, **kwargs: Any):
            captured_vars["value"] = kwargs["vars"]

    monkeypatch.setattr(cli, "DbtArgs", FakeDbtArgs)
    monkeypatch.setattr(cli, "ProjectService", lambda _args: object())

    def raise_manifest_error(_project: object) -> object:
        raise ManifestValidationError("stop execution after vars parsing")

    monkeypatch.setattr(cli, "dry_run_manifest", raise_manifest_error)

    runner = CliRunner()
    result = runner.invoke(cli.app, ["--vars", vars_input])

    assert result.exit_code == 1
    assert captured_vars["value"] == expected_vars
