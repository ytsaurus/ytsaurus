COMMIT_SHA_FETCH := $(shell git rev-parse HEAD)

DOCKER_REGISTRY = ghcr.io
DOCKER_REPOSITORY = ytsaurus/cron

IMAGE_REPO = $(DOCKER_REGISTRY)/$(DOCKER_REPOSITORY)

COMMIT_SHA = $(COMMIT_SHA_FETCH)
TODAY = $(shell date -u +%Y-%m-%d)
IMAGE_TAG = $(TODAY)-$(COMMIT_SHA)

BUILD_FLAGS = -r

.PHONY: prepare-binaries
prepare-binaries:
	temp_dir=$(shell mktemp -d) && \
	mkdir -p ../../cron/binaries/ && \
	rm -rf ../../cron/binaries/* && \
	../../../ya make $(BUILD_FLAGS) ../../cron/snapshot_processing/validate_master_snapshot/ -o $$temp_dir && \
	../../../ya make $(BUILD_FLAGS) ../../cron/snapshot_processing/export_master_snapshot/ -o $$temp_dir && \
	mv $$temp_dir/yt/cron/snapshot_processing/validate_master_snapshot/validate_master_snapshot ../../cron/binaries/ && \
	mv $$temp_dir/yt/cron/snapshot_processing/export_master_snapshot/export_master_snapshot ../../cron/binaries/ && \
	rm -rf $$temp_dir

.PHONY: docker-build
docker-build: prepare-binaries
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
