COMMIT_SHA_FETCH := $(shell git rev-parse HEAD)

DOCKER_REGISTRY = ghcr.io
DOCKER_REPOSITORY = ytsaurus/sidecars

IMAGE_REPO = $(DOCKER_REGISTRY)/$(DOCKER_REPOSITORY)

COMMIT_SHA = $(COMMIT_SHA_FETCH)
TODAY = $(shell date -u +%Y-%m-%d)
IMAGE_TAG = $(TODAY)-$(COMMIT_SHA)

prepare-bin:
	mkdir -p bin/

.PHONY: build-hydra
build-hydra: prepare-bin
	rm -f hydra_persistence_uploader/bin/hydra_persistence_uploader
	../../../ya make hydra_persistence_uploader/bin/ -r
	cp hydra_persistence_uploader/bin/hydra_persistence_uploader bin/hydra_persistence_uploader

.PHONY: build-timbertruck
build-timbertruck:
	rm -f ../../admin/timbertruck/cmd/timbertruck_os/timbertruck_os
	../../../ya make ../../admin/timbertruck/cmd/timbertruck_os/ -r
	cp ../../admin/timbertruck/cmd/timbertruck_os/timbertruck_os bin/timbertruck_os

.PHONY: docker-push
docker-push:
	docker push $(IMAGE_REPO):$(IMAGE_TAG)

.PHONY: docker-build
docker-build: build-hydra build-timbertruck
	docker build \
		--tag=$(IMAGE_REPO):latest \
		--tag=$(IMAGE_REPO):$(IMAGE_TAG) .

.PHONY: docker-release
docker-release: docker-build docker-push
