.PHONY: docker

docker:
	# TODO: check there is no uncommited changes in yt/python/yt/packages to prevent prepare_python_modules fail.
	docker build ../../../ -f ./Dockerfile -t $(tag)
