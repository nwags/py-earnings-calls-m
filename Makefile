.PHONY: test run-api docs

test:
	pytest

run-api:
	uvicorn py_earnings_calls.api.app:create_app --factory --host 0.0.0.0 --port 8000

docs:
	$(MAKE) -C docs html
