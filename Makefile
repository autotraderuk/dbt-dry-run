.PHONY: test
test:
	pytest --cov=dbt_dry_run

.PHONY: testcov
testcov: test
	@echo "building coverage html"
	@coverage html

.PHONY: integration
integration: integration-bq integration-snowflake

.PHONY: integration-bq
integration-bq:
	pytest ./integration --db bigquery

.PHONY: integration-snowflake
integration-snowflake:
	pytest ./integration --db snowflake


.PHONY: mypy
mypy:
	mypy dbt_dry_run

.PHONY: format
format:
	black dbt_dry_run
	black integration
	isort dbt_dry_run

.PHONY: verify
verify: format mypy testcov

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
