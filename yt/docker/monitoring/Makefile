DOCKER_REGISTRY = ghcr.io
DOCKER_ORGANIZATION = ytsaurus
DOCKER_REPOSITORY = monitoring-nightly
CHART_REPOSITORY = monitoring-chart-nightly

IMAGE_REPO = $(DOCKER_REGISTRY)/$(DOCKER_ORGANIZATION)/$(DOCKER_REPOSITORY)
CHART_REPO = oci://$(DOCKER_REGISTRY)/$(DOCKER_ORGANIZATION)

COMMIT_SHA = $(shell git rev-parse HEAD)
VERSION = 0.0.1-dev-$(COMMIT_SHA)

TEMP_DIR_BUILD := $(shell mktemp -d)
TEMP_DIR_DASHBOARDS := $(shell mktemp -d)
TEMP_GENERATOR = $(TEMP_DIR_BUILD)/yt/admin/dashboards/yt_dashboards/bin/generate_dashboards

DASHBOARDS = master-accounts \
             scheduler-operation \
             bundle-ui-user-load \
             bundle-ui-resource \
             bundle-ui-cpu \
             bundle-ui-memory \
             bundle-ui-disk \
             bundle-ui-lsm \
             bundle-ui-network \
             bundle-ui-efficiency \
             bundle-ui-rpc-proxy-overview \
             bundle-ui-rpc-proxy \
             scheduler-internal \
             scheduler-pool \
             cluster-resources \
             master-global \
             master-local \
             queue-metrics \
             queue-consumer-metrics \
             http-proxies

CHART_DIR = ../charts/monitoring-chart
CHART_PACKAGE = $(CHART_REPOSITORY)-$(VERSION).tgz

.PHONY: prepare-dashboards
prepare-dashboards:
	../../../ya make ../../../yt/admin/dashboards/yt_dashboards/bin -o $(TEMP_DIR_BUILD)
	mkdir -p $(TEMP_DIR_DASHBOARDS)/grafana
	cd $(TEMP_DIR_DASHBOARDS) && for dashboard in $(DASHBOARDS); do \
		$(TEMP_GENERATOR) json $$dashboard -f --backend grafana || exit 1; \
	done

.PHONY: docker-build
docker-build: prepare-dashboards
	DOCKER_BUILDKIT=1 docker build \
		-f $(CURDIR)/Dockerfile \
		--platform linux/amd64 \
		--build-context generated=$(TEMP_DIR_DASHBOARDS)/generated \
		--tag=$(IMAGE_REPO):$(VERSION) \
		$(CURDIR)
	rm -rf $(TEMP_DIR_BUILD) $(TEMP_DIR_DASHBOARDS)

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
	sed -e '/image:/,/pullPolicy:/ s|repository: .*|repository: $(IMAGE_REPO)|' \
	    -e '/image:/,/pullPolicy:/ s|tag: .*|tag: "$(VERSION)"|' \
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
