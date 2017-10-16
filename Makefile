name:
	@echo $(shell basename $(PWD))

env:
	@virtualenv -p python3 env

deps:
	@pip install -r requirements.txt