## Changelog

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