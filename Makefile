name:
	@echo $(shell basename $(PWD))

env:
	@virtualenv -p python3 env

deps:
	@pip install -r requirements.txt

test:
	@pytest

lint:
	@pre-commit run \
		--all-files \
		--verbose

autopep8:
	@autopep8 . --recursive --in-place --pep8-passes 2000 --verbose

autopep8-stats:
	@pep8 --quiet --statistics .

clean:
	@find . -name '__pycache__' | xargs rm -rf

.PHONY: deps* lint test clean autopep8*
