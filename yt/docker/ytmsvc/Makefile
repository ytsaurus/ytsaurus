COMMIT_SHA_FETCH := $(shell git rev-parse HEAD)

DOCKER_REGISTRY = ghcr.io
DOCKER_REPOSITORY = ytsaurus/ytmsvc

IMAGE_REPO = $(DOCKER_REGISTRY)/$(DOCKER_REPOSITORY)

COMMIT_SHA = $(COMMIT_SHA_FETCH)
TODAY = $(shell date -u +%Y-%m-%d)
IMAGE_TAG = $(TODAY)-$(COMMIT_SHA)

BUILD_FLAGS = -r

TEMP_DIR := $(shell mktemp -d)

.PHONY: prepare-binaries
prepare-binaries:
	mkdir -p ../../microservices/binaries/
	rm -rf ../../microservices/binaries/*
	../../../ya make $(BUILD_FLAGS) ../../microservices/access_log_viewer/http_service/ \
									../../microservices/access_log_viewer/preprocessing/bin/ \
									../../microservices/access_log_viewer/raw_access_log_preprocessing/cmd/raw_access_log_preprocessing/ \
									../../microservices/bulk_acl_checker_roren/ \
									../../microservices/bulk_acl_checker/http_service_go/ \
									../../microservices/id_to_path_mapping/id_to_path_updater/yt/ \
									../../microservices/resource_usage_roren/ \
									../../microservices/resource_usage/json_api_go/cmd/resource_usage_api/ \
									../../microservices/ytmsvc_initializer/id_to_path_updater/ \
									-o $(TEMP_DIR)
	mv $(TEMP_DIR)/yt/microservices/access_log_viewer/http_service/access_log_viewer ../../microservices/binaries/
	mv $(TEMP_DIR)/yt/microservices/access_log_viewer/preprocessing/bin/access-log-viewer-preprocessor ../../microservices/binaries/
	mv $(TEMP_DIR)/yt/microservices/access_log_viewer/raw_access_log_preprocessing/cmd/raw_access_log_preprocessing/raw_access_log_preprocessing ../../microservices/binaries/
	mv $(TEMP_DIR)/yt/microservices/bulk_acl_checker_roren/bulk_acl_checker_roren ../../microservices/binaries/
	mv $(TEMP_DIR)/yt/microservices/bulk_acl_checker/http_service_go/bulk_acl_checker ../../microservices/binaries/
	mv $(TEMP_DIR)/yt/microservices/id_to_path_mapping/id_to_path_updater/yt/id_to_path_updater ../../microservices/binaries/
	mv $(TEMP_DIR)/yt/microservices/resource_usage_roren/resource_usage_roren ../../microservices/binaries/
	mv $(TEMP_DIR)/yt/microservices/resource_usage/json_api_go/cmd/resource_usage_api/resource_usage_api ../../microservices/binaries/
	mv $(TEMP_DIR)/yt/microservices/ytmsvc_initializer/id_to_path_updater/init_id_to_path_updater ../../microservices/binaries/
	rm -rf $(TEMP_DIR)

.PHONY: docker-build
docker-build: prepare-binaries
	docker build \
		--build-context docker=./ \
		-f ./Dockerfile \
		--platform linux/amd64 \
		--tag=$(IMAGE_REPO):$(IMAGE_TAG) ../../microservices/binaries

.PHONY: docker-push
docker-push:
	docker push $(IMAGE_REPO):$(IMAGE_TAG)

.PHONY: docker-release
docker-release: docker-build docker-push
