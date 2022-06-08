.PHONY: test
test:
	pytest --cov=dbt_dry_run

.PHONY: testcov
testcov: test
	@echo "building coverage html"
	@coverage html

.PHONY: run-local
run-local:
	dbt compile --profiles-dir ./integration/profiles --target integration-local --project-dir ./integration/projects/test_models_with_invalid_sql
	python3 -m dbt_dry_run --profiles-dir ./integration/profiles --target integration-local --manifest-path ./integration/projects/test_models_with_invalid_sql/target/manifest.json default --report-path ./integration/projects/test_models_with_invalid_sql/target/dry_run.json

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
verify: format mypy testcov

.PHONY: build
build: verify
	git diff --exit-code # Exit 1 if there are changes from format
	rm -r ./dist || true
	poetry build

.PHONY: release
release:
	twine upload ./dist/*.whl
	twine upload ./dist/*.tar.gz
