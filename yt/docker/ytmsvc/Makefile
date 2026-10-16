DOCKER_REGISTRY = ghcr.io
DOCKER_ORGANIZATION = ytsaurus
DOCKER_REPOSITORY = ytmsvc-nightly
CHART_REPOSITORY = ytmsvc-chart-nightly

IMAGE_REPO = $(DOCKER_REGISTRY)/$(DOCKER_ORGANIZATION)/$(DOCKER_REPOSITORY)
CHART_REPO = oci://$(DOCKER_REGISTRY)/$(DOCKER_ORGANIZATION)

COMMIT_SHA = $(shell git rev-parse HEAD)
VERSION = 0.0.1-dev-$(COMMIT_SHA)

BUILD_FLAGS = -r

CHART_DIR = ../charts/ytmsvc-chart
CHART_PACKAGE = $(CHART_REPOSITORY)-$(VERSION).tgz

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
		--tag=$(IMAGE_REPO):$(VERSION) ../../microservices/binaries

.PHONY: docker-push
docker-push:
	docker push $(IMAGE_REPO):$(VERSION)

.PHONY: docker-release
docker-release: docker-build docker-push

.PHONY: chart-lint
chart-lint:
	helm lint $(CHART_DIR)

.PHONY: chart-package
chart-package: chart-lint
	@echo "Packaging chart..."
	@TMP_DIR=$$(mktemp -d); \
	trap 'rm -rf "$$TMP_DIR"' EXIT; \
	mkdir -p "$$TMP_DIR/chart"; \
	cp -a $(CHART_DIR)/. "$$TMP_DIR/chart/"; \
	sed 's/^name: .*/name: $(CHART_REPOSITORY)/' "$(CHART_DIR)/Chart.yaml" > "$$TMP_DIR/chart/Chart.yaml"; \
	sed -e '/^image:/,/^imagePullSecrets:/ s|repository: .*|repository: $(IMAGE_REPO)|' \
	    -e '/^image:/,/^imagePullSecrets:/ s|tag: .*|tag: "$(VERSION)"|' \
	    "$(CHART_DIR)/values.yaml" > "$$TMP_DIR/chart/values.yaml"; \
	helm package "$$TMP_DIR/chart" \
		--version $(VERSION) \
		--destination .

.PHONY: chart-push
chart-push: chart-package
	helm push $(CHART_PACKAGE) $(CHART_REPO)

.PHONY: clean-chart
clean-chart:
	rm -f $(CHART_PACKAGE)

.PHONY: release-all
release-all: chart-push docker-release
