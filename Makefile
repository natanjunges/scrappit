.PHONY: all upload clean

all: dist/*

dist/*: venv/
	. venv/bin/activate && python3 -m build

venv/:
	python3 -m venv venv
	. venv/bin/activate && pip install -U pip
	. venv/bin/activate && pip install build twine

upload: venv/ dist/*
	. venv/bin/activate && twine upload dist/*

clean:
	rm -rf venv/
	rm -rf dist/
