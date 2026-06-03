COMMIT_SHA_FETCH := $(shell git rev-parse HEAD)

DOCKER_REGISTRY = ghcr.io
DOCKER_REPOSITORY = ytsaurus/build-env

IMAGE_REPO = $(DOCKER_REGISTRY)/$(DOCKER_REPOSITORY)

COMMIT_SHA = $(COMMIT_SHA_FETCH)
TODAY := $(shell date -u +%Y-%m-%d)
IMAGE_TAG = $(TODAY)-$(COMMIT_SHA)

.PHONY: docker-push
docker-push:
	docker push $(IMAGE_REPO):$(IMAGE_TAG)

.PHONY: docker-build
docker-build:
	docker build \
		--tag=$(IMAGE_REPO):latest \
		--tag=$(IMAGE_REPO):$(IMAGE_TAG) .

.PHONY: docker-release
docker-release: docker-build docker-push
