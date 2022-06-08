.PHONY: setup-dev
setup-dev:
	pip install -e .\[test\]

.PHONY: test
test:
	pytest --cov=dbt_dry_run

.PHONY: testcov
testcov: test
	@echo "building coverage html"
	@coverage html

.PHONY: integration
integration:
	pytest ./integration

.PHONY: mypy
mypy:
	mypy dbt_dry_run

.PHONY: format
format:
	black dbt_dry_run
	isort dbt_dry_run

.PHONY: verify
verify: format mypy test

.PHONY: build
build: verify
	git diff --exit-code # Exit 1 if there are changes from format
	rm -r ./dist || true
	poetry build

.PHONY: release
release:
	twine upload ./dist/*.whl
	twine upload ./dist/*.tar.gz
