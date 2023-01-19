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