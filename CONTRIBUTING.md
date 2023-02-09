# Contributing/Running locally

To setup a dev environment you need [poetry][get-poetry], first run `poetry install` to install all dependencies. Then
the `Makefile` contains all the commands needed to run the test suite and linting.

- verify: Formats code with `black`, type checks with `mypy` and then runs the unit tests with coverage.
- integration: Runs the integration tests against BigQuery (See Integration Tests)

There is also a shell script `./run-integration.sh <PROJECT_DIR>` which will run one of the integration tests locally.
Where `<PROJECT_DIR>` is one of the directory names in `/integration/projects/`. (See Integration Tests)

## Running Integration Tests

In order to run integration tests locally you will need access to a BigQuery project/instance in which your gcloud
application default credentials has the role `Big Query Data Owner`. The BigQuery instance should have an empty dataset
called `dry_run`.

Setting the environment variable `DBT_PROJECT=<YOUR GCP PROJECT HERE>` will tell the integration tests which GCP project
to run the test suite against. The test suite does not currently materialize any data into the project.

The integration tests will run on any push to `main` to ensure the package's core functionality is still in place.

__Auto Trader employees can request authorisation to access the `at-dry-run-integration-dev` project for this purpose__

# Preparing for a Release

## Bump Version

We are using semantic versioning for `dbt-dry-run`. If the release contains just bugfixes then just bump the patch 
version. If there are new features or minor breaking changes then bump the minor version. I don't see a case where 
we would bump the major version to 1 in the immediate future until the package is more widely used in production.

To bump the version update the version in `pyproject.toml`

## Update CHANGES.md

Currently, we are using the below format for each release:

```

# dbt-dry-run v0.x.x

## Improvements & Bugfixes

- Here you should describe each bugfix and new feature. If there are breaking changes then this should be
  made explicit here and with details on how to migrate

## Under the Hood

- Here any internal refactoring/performance improvments should be referenced or changes to the CI/CD etc

- If either of these sections is blank then it can be removed

```

## Releasing to PyPi

Currently, the github action setup in `.github/workflows/main.yml` automatically release to PyPi on tag. The procedure 
for releasing should be:

1. Check the last github action on `main` was green

2. Check the `CHANGES.md` has been updated with all the commits/PRs from the last tagged release. Decide what version 
   the new release should be, this project roughly follows SemVer

3. Make and push a version bump commit which increases the version in `pyproject.toml` (Ensure this is consistent with the latest 
   version in `CHANGES.md`)
   
4. Once the version bump commit GH action is green then tag this commit with the same version prefix with `v` so the tag
   name should be `vX.X.X`. Once this tag is pushed another GH action will start which will release the package version 
   to production PyPI
   
5. Verify the package is released