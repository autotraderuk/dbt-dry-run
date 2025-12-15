.PHONY: install
install:
	poetry env use 3.13
	poetry install

.PHONY: test
test:
	poetry run pytest --cov=dbt_dry_run

.PHONY: testcov
testcov: test
	poetry run coverage html

.PHONY: integration
integration:
	poetry run pytest ./integration

.PHONY: mypy
mypy:
	poetry run mypy dbt_dry_run integration

.PHONY: lint
lint:
	poetry run ruff check

.PHONY: format
format:
	poetry run ruff format dbt_dry_run
	poetry run ruff format integration

.PHONY: verify
verify: format mypy lint testcov

.PHONY: build
build: verify
	git diff --exit-code # Exit 1 if there are changes from format
	rm -r ./dist || true
	poetry build

.PHONY: release
release:
	git diff HEAD main --quiet || (echo 'Must release in main' && false)
	poetry run twine upload ./dist/*.whl
	poetry run twine upload ./dist/*.tar.gz
