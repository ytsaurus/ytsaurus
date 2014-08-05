PYTHON ?= `which python`
PROJECT = `dpkg-parsechangelog | awk -F': ' '$1 == "Source" { print $2; }'`
BUILDIR = $(CURDIR)/debian/$(PROJECT)
WRAPPER_VERSION=$(shell dpkg-parsechangelog -lyandex-yt-python/debian/changelog | grep "Version:" | awk '{print $$2}')

all:
	@echo "make source - Create source package"
	@echo "make deb - Generate a deb package"
	@echo "make clean - Get rid of scratch and byte files"

version:
	echo "VERSION='$(WRAPPER_VERSION)'" > yt/wrapper/version.py

source:
	$(PYTHON) setup.py sdist

test: deb_clean
	$(PYTHON) setup.py test

docs:
	rm -rf docs
	mkdir docs
	sphinx-apidoc -F -o docs yt
	sphinx-build -b html docs docs/_build/ 

deb:
	$(MAKE) test
	$(MAKE) deb_without_test

egg:
	$(PYTHON) setup.py bdist_egg
	cp dist/*.egg .

deb_without_test: deb_clean
	# build the source package in the parent directory
	# then rename it to project_version.orig.tar.gz
	DEB=1 $(PYTHON) setup.py sdist --dist-dir=../
	# build the package
	dpkg-buildpackage -i -I -rfakeroot

deb_clean:
	find . -name '*.pyc' -delete
	sudo rm -rf tests yt/wrapper/tests/sandbox/*
	sudo rm -rf build/ dist/ MANIFEST *.spec *.egg *.egg-info

clean: deb_clean
	rm -rf debian setup.py docs/

.PHONY: docs

