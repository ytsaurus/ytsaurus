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

deb: test
	cp yt/wrapper/mapreduce yt/wrapper/mapreduce-yt
	# build the source package in the parent directory
	# then rename it to project_version.orig.tar.gz
	$(PYTHON) setup.py sdist --dist-dir=../
	# build the package
	dpkg-buildpackage -i -I -rfakeroot

clean:
	$(PYTHON) setup.py clean
	find . -name '*.pyc' -delete
	rm -rf build/ MANIFEST
	rm -rf tests Yt.egg-info yt/wrapper/mapreduce-yt
	sudo $(MAKE) -f $(CURDIR)/debian/rules clean

