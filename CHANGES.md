## Changelog

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