PYTHON ?= python3.12
PIP    := $(PYTHON) -m pip
PYTEST := $(PYTHON) -m pytest

CONTRACTS_PIN := git+https://github.com/shenfanjie5-bit/project-ult-contracts.git@v0.1.3
SDK_PIN       := git+https://github.com/shenfanjie5-bit/project-ult-subsystem-sdk.git@v0.1.2

.PHONY: help install-dev install-contracts-schemas test-fast contract test ci clean

help:
	@echo "Targets:"
	@echo "  install-dev               - install pinned contracts, SDK, and dev deps"
	@echo "  install-contracts-schemas - install with contract schema extra"
	@echo "  test-fast                 - unit and boundary tests"
	@echo "  contract                  - contract-focused tests"
	@echo "  test                      - full pytest collection"
	@echo "  ci                        - install-dev + test"

install-dev:
	$(PIP) install "$(CONTRACTS_PIN)"
	$(PIP) install "$(SDK_PIN)"
	$(PIP) install -e ".[dev]"

install-contracts-schemas:
	$(PIP) install "$(CONTRACTS_PIN)"
	$(PIP) install "$(SDK_PIN)"
	$(PIP) install -e ".[dev,contracts-schemas]"

test-fast:
	$(PYTEST) tests/unit tests/boundary -q

contract:
	$(PYTEST) tests/contract -q

test:
	$(PYTEST)

ci: install-dev test

clean:
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name .pytest_cache -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name "*.egg-info" -exec rm -rf {} + 2>/dev/null || true
