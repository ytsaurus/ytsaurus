PYTHON=`which python`
PROJECT=`dpkg-parsechangelog | awk -F': ' '$1 == "Source" { print $2; }'`
BUILDIR=$(CURDIR)/debian/$(PROJECT)

all:
	@echo "make source - Create source package"
	@echo "make deb - Generate a deb package"
	@echo "make clean - Get rid of scratch and byte files"

source:
	$(PYTHON) setup.py sdist

test: clean
	$(PYTHON) setup.py test


prepare:
	cp yt/wrapper/mapreduce yt/wrapper/mapreduce-yt

deb:
	$(MAKE) test
	$(MAKE) clean
	$(MAKE) deb_without_test

egg: prepare
	python setup.py bdist_egg
	cp dist/*.egg .

deb_without_test: clean
	@MAKE prepare
	# build the source package in the parent directory
	# then rename it to project_version.orig.tar.gz
	$(PYTHON) setup.py sdist --dist-dir=../
	# build the package
	dpkg-buildpackage -i -I -rfakeroot

clean:
	$(PYTHON) setup.py clean
	find . -name '*.pyc' -delete
	sudo rm -rf tests Yt.egg-info yt/wrapper/mapreduce-yt
	sudo rm -rf build/ MANIFEST
	sudo $(MAKE) -f $(CURDIR)/debian/rules clean

