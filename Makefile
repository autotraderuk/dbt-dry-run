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

.PHONY: lint
lint:
	ruff check

.PHONY: format
format:
	ruff format dbt_dry_run
	ruff format integration

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
	twine upload ./dist/*.whl
	twine upload ./dist/*.tar.gz
