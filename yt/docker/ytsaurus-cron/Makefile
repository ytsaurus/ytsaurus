COMMIT_SHA_FETCH := $(shell git rev-parse HEAD)

DOCKER_REGISTRY = docker.io
DOCKER_REPOSITORY = ytsaurus/ytsaurus-cron

IMAGE_REPO = $(DOCKER_REGISTRY)/$(DOCKER_REPOSITORY)

COMMIT_SHA = $(COMMIT_SHA_FETCH)
TODAY = $(shell date -u +%Y-%m-%d)
IMAGE_TAG = $(TODAY)-$(COMMIT_SHA)

.PHONY: docker-build
docker-build:
	docker build \
		--build-context docker=./ \
		-f ./Dockerfile \
		--platform linux/amd64 \
		--tag=$(IMAGE_REPO):latest \
		--tag=$(IMAGE_REPO):$(IMAGE_TAG) ../../cron

.PHONY: docker-push
docker-push:
	docker push $(IMAGE_REPO):$(IMAGE_TAG)

.PHONY: docker-release
docker-release: docker-build docker-push
