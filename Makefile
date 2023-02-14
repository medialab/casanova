# Variables
SOURCE = casanova

# Functions
define clean
	rm -rf *.egg-info .pytest_cache build dist
	find . -name "*.pyc" | xargs rm -f
	find . -name __pycache__ | xargs rm -rf
	rm -f *.spec
endef

# Commands
all: lint test
test: unit
publish: clean lint test upload
	$(call clean)

clean:
	$(call clean)

deps:
	pip3 install -U pip
	pip3 install -r requirements.txt

lint:
	@echo Searching for unused imports...
	importchecker $(SOURCE) | grep -v __init__ || true
	importchecker test | grep -v __init__ || true
	@echo

format:
	@echo Formatting source code using black
	black $(SOURCE) benchmark scripts test
	@echo

unit:
	@echo Running unit tests...
	pytest -svvv
	@echo

upload:
	python setup.py sdist bdist_wheel
	twine upload dist/*

# To be sure it works with pyenv:
#   $ pyenv local casanova 3.7 3.8 3.9 3.10 3.11
tox:
	tox
