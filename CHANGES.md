## Changelog

# dbt-dry-run v0.7.7

## Improvements

- Official compatibility with dbt v1.7

# dbt-dry-run v0.7.6

## Improvements

- Add `total_bytes_processed` to report artefact

# dbt-dry-run v0.7.5

## Bugfixes

- Fix issue with incremental models where `sql_header` is set

# dbt-dry-run v0.7.4

## Bugfixes

- Fix false failure when incremental models use `require_partition_filter=True`

# dbt-dry-run v0.7.3

## Bugfixes

- Incremental models now correctly predict the column order if the table already exists in the target environment
- External tables no longer always require defining the schema twice in the YAML if the table source allows it
- Incremental models no longer cause a syntax error when they use `with recursive` CTEs

# dbt-dry-run v0.7.2

## Bugfixes

- Seed files now get their schema using type inference from the adapter so always line up with what dbt will produces
- Seed file `column_type` configuration is respected

# dbt-dry-run v0.7.1

## Bugfixes

- Fix dry runner falsely reporting success if incremental has incompatible type change for existing column

# dbt-dry-run v0.7.0

## Improvements

- Adds `--full-refresh` support. Dry running with full refresh will make use of predicted schema. This option aligns with the dbt cli 
- Adds `--target-path` support. This option aligns with the dbt cli

# dbt-dry-run v0.6.8

- Compatibility with dbt v1.6

# dbt-dry-run v0.6.7

- Compatibility with dbt v1.5

- Adds `--threads` option as an override

# dbt-dry-run v0.6.6

## Bugfixes & Improvements

- Added `--extra-check-columns-metadata-key` CLI option. Specifying this will mean that you can use another metadata 
  key instead of just `dry_run.check_columns`. `dry_run.check_columns` will always take priority over the extra key.
  This is useful if you have an existing metadata key such as `governance.is_odp` that you want to enable metadata 
  checking for

- Added `--version` CLI option to print the installed version of `dbt-dry-run`

- Added support for Python 3.11 ([zachary-povey](https://github.com/zachary-povey))

# dbt-dry-run v0.6.5

## Bugfixes & Improvements

- Added command line flag `--skip-not-compiled` which will override the default behaviour of raising a `NotCompiledExceptipon`
  if a node is in the manifest that should be compiled. This should only be used in certain circumstances where you want 
  to skip an entire section of your dbt project from the dry run. Or if you don't want to dry run tests
  
- Added `status` to the report artefact which can be `SUCCESS`, `FAILED`, `SKIPPED`

# dbt-dry-run v0.6.4

## Bugfixes & Improvements

- Add support for dbt 1.4

# dbt-dry-run v0.6.3

## Bugfixes & Improvements

- Add support for INTERVAL and JSON types.

- Improved error handling when parsing the predicted schema of the dry run queries. Error message will now raise an
  `UnknownSchemaException` detailing the field type returned by BigQuery that it does not recognise

# dbt-dry-run v0.6.2

## Bugfixes

- Don't dry run nodes that are disabled

# dbt-dry-run v0.6.1

## Bugfixes

- Move column specification to `dry_run_columns` in `external` to avoid conflict with `dbt-external-tables` and other
  dbt integrations

# dbt-dry-run v0.6.0

## Improvements & Bugfixes

- Support dry running tests. Generic and custom tests will be checked. This can catch errors such as column name typos in the
  generic test metadata and SQL syntax errors in custom tests

- Added support for `dbt-external-tables`. Any `source` marked with `external` will be 'dry runned' by reading the
  schema from the yaml metdata for the source. The dry run does not support schema prediction for external tables
  
# dbt-dry-run v0.5.1

## Bugfixes

- Fixed issue where using a SQL header and `_dbt_max_partition` in an incremental would cause a false positive dry run
  failure due to `Variable declarations are allowed only at the start of a block or script`

# dbt-dry-run v0.5.0

## Improvements & Bugfixes

- Add support for column metadata linting/validation. Mark a model in its metadata with `dry_run.check_columns: true`
  to enable checks that ensure that column names in the predicted dbt project schema match the columns in the metadata
  see the `README.md` for more info, failure will be reporting as a `LINTING` error:

  ```text
  Dry running X models
  Node model.test_column_linting.badly_documented_model failed linting with rule violations:
          UNDOCUMENTED_COLUMNS : Column not documented in metadata: 'c'
          EXTRA_DOCUMENTED_COLUMNS : Extra column in metadata: 'd'

  Total 1 failures:
  1       :       model.test_column_linting.badly_documented_model        :       LINTING :       ERROR
  DRY RUN FAILURE!
  ```

# dbt-dry-run v0.4.2

## Improvements & Bugfixes

- Add support for dbt v1.3.0

- Throw error if dbt project uses python models as these cannot be used with the dry run as we cannot currently predict
  their schemas

# dbt-dry-run v0.4.1

## Improvements & Bugfixes

- Allow `unique_key` to be a list of strings for incremental models which was added in `dbt v1.1`

# dbt-dry-run v0.4.0

## Improvements & Bugfixes

- Under the hood re-write of how we create the BigQuery connection. We now directly interact with dbt and create the
  project config and BigQuery adapter using dbt instead of reimplementing the logic that dbt uses by reading your
  `profiles.yml`

- Due to the re-write there is backwards incompatible changes with the CLI where you should now run the dry runner in
  the same way use run `dbt compile` as it will search for the `dbt_project.yml` in the same directory as you run the
  dry runner (By default). This can be overridden in the same way as in dbt using the `--project-dir` option

- The CLI now also uses [Typer][get-typer] so the CLI help is now improved. Typing `dbt-dry-run --help` outputs:

  ```
  ❯ dbt-dry-run --help
    Usage: dbt-dry-run [OPTIONS] [PROFILE]

    Options:
      --profiles-dir TEXT             [dbt] Where to search for `profiles.yml`
                                      [default: /Users/<user>/.dbt]
      --project-dir TEXT              [dbt] Where to search for `dbt_project.yml`
                                      [default: /Users/<user>/Code/dbt-
                                      dry-run]
      --vars TEXT                     [dbt] CLI Variables to pass to dbt
      --target TEXT                   [dbt] Target profile
      --verbose / --no-verbose        Output verbose error messages  [default: no-
                                      verbose]
      --report-path TEXT              Json path to dump report to
      --install-completion [bash|zsh|fish|powershell|pwsh]
                                      Install completion for the specified shell.
      --show-completion [bash|zsh|fish|powershell|pwsh]
                                      Show completion for the specified shell, to
                                      copy it or customize the installation.
      --help                          Show this message and exit.

  ```

  Where any option description prefixed with `[dbt]` should work in the same way as it does in the dbt CLI

- Fixed issue where `partition_by` `data_type` was case-sensitive so a value of `DATE` would not be accepted by the
  dry runner but would be accepted by dbt when parsing the manifest

# dbt-dry-run v0.3.1

## Improvements & Bugfixes

- Fixed incremental models that used `_dbt_max_partition` variable failing as the dry runner was not
  declaring this variable

- Fixed select literals for numeric types not explicitly casting to BigNumeric or Numeric

## dbt-dry-run v0.3.0

### Improvements & Bugfixes

- Add snapshot support! The dry runner can now correctly predict the schema of your snapshots and if they will
  run or not. It can catch configuration errors as well such as incorrect `unique_key`, `check_cols` and
  `updated_at` columns

- Added support for `as_number` filter in `profiles.yml`

- Support both `schema` and `dataset` in `profiles.yml` as in BigQuery they are used interchangeably

## dbt-dry-run v0.2.0

### Improvements & Bugfixes

- Improved error messages when passing incorrect parameters to the command line such as invalid `manifest.json`
or profile directory

- `profiles.yml` that use `env_var` templating will correctly render (See [dbt docs][dbt-env-var])

- `keyfile` is now optional when using `oath` authentication method in `profiles.yml`

- Added `--output-path` argument. This will produce a JSON report of project dry run with predicted schema/error
  message of each model and seed

- Concurrency will now respect `threads` in `profiles.yml` rather than being hardcoded to `8`

### Under the hood

- Integration test suite against BigQuery instance

## dbt-dry-run v0.1.7

### Improvements & Bugfixes

- Support `impersonate_service_account` in `profiles.yml`

[dbt-env-var]: https://docs.getdbt.com/reference/dbt-jinja-functions/env_var
[get-typer]: https://typer.tiangolo.com/
