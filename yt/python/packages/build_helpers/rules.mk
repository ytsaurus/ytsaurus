export DEBPYTHON_DEFAULT=2.7
export DEBPYTHON_SUPPORTED=2.7

export PYBUILD_SYSTEM=distutils

export PYBUILD_VERBOSE=1
export DH_VERBOSE=1

LOCATION := $(shell dirname $(realpath $(lastword $(MAKEFILE_LIST))))
export PATH := $(LOCATION):$(PATH)
export PYTHONPATH := $(LOCATION):$(PYTHONPATH)

override_dh_install:
	dh_install
	$(LOCATION)/fix_shebangs.sh
