PYTHON=`which python`
PROJECT=`dpkg-parsechangelog | awk -F': ' '$1 == "Source" { print $2; }'`
BUILDIR=$(CURDIR)/debian/$(PROJECT)

all:
	@echo "make source - Create source package"
	@echo "make deb - Generate a deb package"
	@echo "make clean - Get rid of scratch and byte files"

source:
	$(PYTHON) setup.py sdist

deb:
	# build the source package in the parent directory
	# then rename it to project_version.orig.tar.gz
	$(PYTHON) setup.py sdist --dist-dir=../
	# build the package
	dpkg-buildpackage -i -I -rfakeroot

clean:
	$(PYTHON) setup.py clean
	rm -rf build/ MANIFEST
	find . -name '*.pyc' -delete
	rm -rf tests
	$(MAKE) -f $(CURDIR)/debian/rules clean
