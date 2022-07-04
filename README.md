# dbt-dry-run

[dbt][dbt-home] is a tool that helps manage data transformations using templated SQL queries. These SQL queries are 
executed against a target data warehouse. It doesn't check the validity of SQL queries before it executes your project.
This dry runner uses BigQuery's [dry run][bq-dry-run] capability to allow you to check that SQL queries are valid before
trying to execute them.

See the [blog post][blog-post] for more information on how the dry runner works.

![Terminal Recording of failing dry run](./terminal-demo.gif)

## Quickstart

### Installation

The dry runner can be installed via pip:

`
pip install dbt-dry-run
`

### Running

The dry runner has a single command called `dbt-dry-run` in order for it to run you must 
first compile a dbt manifest using `dbt compile` as you normally would.

Then on the same machine (So that the dry runner has access to your dbt project source and the 
`manifest.yml`) you can run the dry-runner with:

```
dbt-dry-run <PROFILE>
```

Where `PROFILE` should match the profile specified in your `dbt_project.yml`:

```
# This setting configures which "profile" dbt uses for this project.
# This will get overridden by the root project
profile: 'default'
```

Like dbt it will search for `profiles.yml` in `~/.dbt/` and use the default target specified.
It will also look for the `manifest.yml` in the current working directory. 
Just like in the dbt CLI you can override these defaults:

```
dbt-dry-run default --profiles-dir /my_org_dbt/profiles/ --target local --manifest-path target/manifest.json
```

## Reporting Results & Failures

If the result is successful it will output the number of 
models that were tested like so:

```
Dry running 3 models

DRY RUN SUCCESS!
```

The process will also return exit code 0

If there are failures it will print a summary table of the nodes that failed:

```
Dry running 3 models
Node model.test_models_with_invalid_sql.second_layer failed with exception:
400 POST https://bigquery.googleapis.com/...: Column d in USING clause not found on left side of join at [6:88]

(job ID: 5e336f32-273d-480a-b8bb-cdf4fca66a98)

Total 1 failures:
1       :       model.test_models_with_invalid_sql.second_layer :       BadRequest      :       ERROR
DRY RUN FAILURE!`
```

The process will also return exit code 1

### Report Artefact

If you specify `---report-path` a JSON file will be outputted regardless of dry run success/failure with detailed 
information of each node's predicted schema or error message if it has failed:

```json
{
  "success": false,
  "node_count": 3,
  "failure_count": 1,
  "failed_node_ids": [
    "model.test_models_with_invalid_sql.second_layer"
  ],
  "nodes": [
    {
      "unique_id": "seed.test_models_with_invalid_sql.my_seed",
      "success": true,
      "error_message": null,
      "table": {
        "fields": [
...
        ]
      }
    },
    {
      "unique_id": "model.test_models_with_invalid_sql.first_layer",
      "success": true,
      "error_message": null,
      "table": {
        "fields": [
...
        ]
      }
    },
    {
      "unique_id": "model.test_models_with_invalid_sql.second_layer",
      "success": false,
      "error_message": "BadRequest",
      "table": null
    }
  ]
}
```

## Contributing/Running locally

To setup a dev environment you need [poetry][get-poetry], first run `poetry install` to install all dependencies. Then
the `Makefile` contains all the commands needed to run the test suite and linting.

- verify: Formats code with `black`, type checks with `mypy` and then runs the unit tests with coverage.
- run-local: Runs one of the integration tests locally. _Requires BigQuery Instance_ (See Integration Tests)
- integration: Runs the integration tests against BigQuery (See Integration Tests)

### Running Integration Tests

In order to run integration tests locally you will need access to a BigQuery project/instance in which your gcloud application 
default credentials has the role `Big Query Data Owner`. The BigQuery instance should have an empty dataset called `dry_run`.

Setting the environment variable `DBT_PROJECT=<YOUR GCP PROJECT HERE>` will tell the integration tests which GCP 
project to run the test suite against. The test suite does not currently materialize any data into the project.

The integration tests will run on any push to `main` to ensure the package's core functionality is still in place.

__Auto Trader employees can request authorisation to access the `at-dry-run-integration-dev` project for this purpose__

## Capabilities and Limitations

### Things this can catch

The dry run can catch anything the BigQuery planner can identify before the query has run. Which 
includes:

1. Typos in SQL keywords:  `selec` instead of `select`
2. Typos in columns names: `orders.produts` instead of `orders.products`
3. Problems with incompatible data types: Trying to execute "4" + 4
4. Incompatible schema changes to models: Removing a column from a view that is referenced
by a downstream model explicitly
5. Incompatible schema changes to sources: Third party modifies schema of source tables without 
your knowledge
6. Permission errors: The dry runner should run under the same service account your production 
job runs under. This allows you to catch problems with table/project permissions as dry run queries
need table read permissions just like the real query
7. Incorrect configuration of snapshots: For example a typo in the `unique_key` config. Or `check_cols` which do not 
exist in the snapshot
   
### Things this can't catch

There are certain cases where a syntactically valid query can fail due to the data in 
the tables:

1. Queries that run but do not return intended/correct result. This is checked using [tests][dbt-tests]
2. `NULL` values in `ARRAY_AGG` (See [IGNORE_NULLS bullet point][bq-ignore-nulls])
3. Bad query performance that makes it too complex/expensive to run

### Things still to do...

Implementing the dry runner required re-implementing some areas of dbt. Mainly how the 
adapter sets up connections and credentials with the BigQuery client, we have only 
implemented the methods of how we connect to our warehouse so if you don't use OAUTH or 
service account JSON files then this won't be able to read `profiles.yml` correctly.

The implementation of seeds is incomplete as well as we don't use them very much in our 
own dbt projects. The dry runner will just use the datatypes that `agate` infers from the CSV 
files.

[dbt-home]: https://www.getdbt.com/
[bq-dry-run]: https://cloud.google.com/bigquery/docs/dry-run-queries
[dbt-tests]: https://docs.getdbt.com/docs/building-a-dbt-project/tests
[bq-ignore-nulls]: https://cloud.google.com/bigquery/docs/reference/standard-sql/aggregate_functions#array_agg
[blog-post]: https://engineering.autotrader.co.uk/2022/04/06/dry-running-our-data-warehouse-using-bigquery-and-dbt.html
[get-poetry]: https://python-poetry.org/

## License

Copyright 2022 Auto Trader Limited

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
