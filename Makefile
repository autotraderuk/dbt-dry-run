.PHONY: install
install:
	uv sync --all-groups

.PHONY: test
test:
	uv run pytest --cov=dbt_dry_run

.PHONY: testcov
testcov: test
	uv run coverage html

.PHONY: integration
integration:
	uv run pytest ./integration

.PHONY: mypy
mypy:
	uv run mypy dbt_dry_run integration

.PHONY: lint
lint:
	uv run ruff check

.PHONY: format
format:
	uv run ruff format dbt_dry_run
	uv run ruff format integration

.PHONY: verify
verify: format mypy lint testcov

.PHONY: build
build: verify
	git diff --exit-code # Exit 1 if there are changes from format
	rm -r ./dist || true
	uv build

.PHONY: release
release:
	git diff HEAD main --quiet || (echo 'Must release in main' && false)
	uv run twine upload ./dist/*.whl
	uv run twine upload ./dist/*.tar.gz

.PHONY: gha-pr
gha-pr:
	act pull_request

.PHONY: gha-push
gha-push:
	act push
