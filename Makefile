.DEFAULT: help

BASE_NAME=$(shell basename $(PWD))

#Colours
NO_COLOUR=\033[0m
OK_COLOUR=\033[32m
INFO_COLOUR=\033[33m
ERROR_COLOUR=\033[31;01m
WARN_COLOUR=\033[33;01m

help::
	@printf "\n$(BASE_NAME) make targets\n"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "$(INFO_COLOUR)%-35s$(NO_COLOUR) %s\n", $$1, $$2}'
	@printf "\n"

name:
	@echo $(shell basename $(PWD))

deps: ## Install development dependencies
	@pip install -r requirements.txt
	@pre-commit install

deps-update: ## Update Python dependencies
	@pip install -r requirements-to-freeze.txt --upgrade
	@pip freeze > requirements.txt

deps-uninstall: ## Uninstall Python dependencies
	@pip uninstall -yr requirements.txt
	@pip freeze > requirements.txt

env: ## Create local Python virtualenv
	@python -m venv env

test: ## Run tests
	@pytest

lint: ## Run lint on all files
	@pre-commit run \
		--all-files \
		--verbose

autopep8: ## Check for PEP8 issues
	@autopep8 . --recursive --in-place --pep8-passes 2000 --verbose

autopep8-stats: ## Output PEP8 stats
	@pep8 --quiet --statistics .

clean: ## Clean the build
	@find . -name '__pycache__' | xargs rm -rf

.PHONY: deps* lint test clean autopep8* help dev*
