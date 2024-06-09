.PHONY: docker

docker:
	# TODO: check there is no uncommited changes in yt/python/yt/packages to prevent prepare_python_modules fail.
	docker build \
	  ../../../../ \
	  -f ../../../odin/Dockerfile \
	  --build-arg="BUILD_TARGETS=" \
	  --target lib-builder \
	  -t odin/lib-builder && \
	docker build -f ./Dockerfile ../../../../ -t $(tag)
