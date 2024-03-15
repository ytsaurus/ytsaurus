#!/usr/bin/make -f

# If you update this file, please follow
# https://www.thapaliya.com/en/writings/well-documented-makefiles/

.DEFAULT_GOAL := help

SRC_ROOT:=$(shell pwd)

YATOOL = ./ya
YAMAKE = $(YATOOL) make

## Release | RelWithDebInfo | FastDebug | Debug
BUILD = Debug

## Enable link time optimization: "" | full | thin
LTO =

## Targe platform, see: ./ya make --target-platform=list
TARGET_PLATFORM =

## Enable sanitizer: "" | Address | Memory | Thread | Undefined | Leak
SANITIZE =

## Path for resulting binaries
INSTALL_DIR =

## Verbosity: "" | 0 | 1 | 2
V = 1

ifeq (${OUT},)
## Test output directory, default out/<YYYY-MM-DD-HH-MM-SS>
OUT = out/$(shell date +%Y-%m-%d-%H-%M-%S)
endif

ifneq (${BUILD},)
  BUILD_FLAGS += --build=${BUILD}
endif

ifeq (${LTO},full)
  BUILD_FLAGS += --lto
else ifeq (${LTO},thin)
  BUILD_FLAGS += --thinlto
endif

ifneq (${TARGET_PLATFORM},)
  BUILD_FLAGS += --target-platform=${TARGET_PLATFORM}
endif

ifneq (${SANITIZE},)
  BUILD_FLAGS += --sanitize=${SANITIZE}
endif

ifneq (${INSTALL_DIR},)
  BUILD_FLAGS += --install=${INSTALL_DIR}
endif

ifeq (${V},0)
  BUILD_FLAGS += --no-emit-status
endif

ifeq (${V},1)
  BUILD_FLAGS += --no-emit-status -T
  TEST_FLAGS += --show-passed-tests --test-stdout --test-stderr
endif

ifeq (${V},2)
  BUILD_FLAGS += --no-emit-status -T -v --stat
  TEST_FLAGS += --show-passed-tests --test-stdout --test-stderr --show-metrics
endif

ifneq (${TEST},)
  TEST_FLAGS += -A
endif

ifeq (${TEST},style)
  TEST_FLAGS += --style
else ifeq (${TEST},list)
  TEST_FLAGS += --list-tests
else ifeq (${TEST},fail-fast)
  TEST_FLAGS += --fail-fast
else ifeq (${TEST},failed)
  TEST_FLAGS += --last-failed-tests
else ifeq (${TEST},debug)
  TEST_FLAGS += --last-failed-tests
  TEST_FLAGS += --test-debug
  TEST_FLAGS += --pdb
endif

TEST_FLAGS += --inline-diff
TEST_FLAGS += --dont-merge-split-tests

ifneq (${OUT},)
  TEST_FLAGS += --output=${OUT}
  TEST_FLAGS += --junit=${OUT}/results.xml
endif

ifneq (${TEST_FILTER},)
  TEST_FLAGS += --test-filter="${TEST_FILTER}"
endif

## All reasonable non-broken target directories.
ALL_DIRS ?= yt/yt/ yt/yt_proto/ yt/chyt/ yt/yql/ yt/python/ yt/go/ yt/odin/

YAMAKE_FLAGS = ${BUILD_FLAGS}

ifneq (${TEST},)
  YAMAKE_FLAGS += ${TEST_FLAGS}
endif

## Docker registry for local builds.
DOCKER_REGISTRY = localhost:${REGISTRY_LOCAL_PORT}

## Docker image path.
DOCKER_REPOSITORY = ${USER}

## Docker image suffix.
DOCKER_IMAGE_SUFFIX = -relwithdebinfo

## Target docker image tag, default {branch}-{date}-{commit}.
DOCKER_IMAGE_TAG = $(shell git branch --show-current | tr / -)-$(shell git show -s --pretty=%cs-%H)${DOCKER_IMAGE_SUFFIX}

DOCKER_OVERRIDE_BASE_REPOSITORY =
DOCKER_OVERRIDE_BASE_IMAGE =

YAPACKAGE_FLAGS += --docker
YAPACKAGE_FLAGS += --docker-push
ifneq (${DOCKER_REGISTRY},)
  YAPACKAGE_FLAGS += --docker-registry ${DOCKER_REGISTRY}
endif
ifneq (${DOCKER_REPOSITORY},)
  YAPACKAGE_FLAGS += --docker-repository ${DOCKER_REPOSITORY}
endif
ifneq (${DOCKER_IMAGE_TAG},)
  YAPACKAGE_FLAGS += --custom-version ${DOCKER_IMAGE_TAG}
endif

ifneq (${DOCKER_OVERRIDE_BASE_REPOSITORY},)
  DOCKER_OVERRIDE_FLAGS += --docker-build-arg BASE_REPOSITORY=${DOCKER_OVERRIDE_BASE_REPOSITORY}
endif
ifneq (${DOCKER_OVERRIDE_BASE_IMAGE},)
  DOCKER_OVERRIDE_FLAGS += --docker-build-arg BASE_IMAGE=${DOCKER_OVERRIDE_BASE_IMAGE}
endif


CMAKE = cmake

CMAKE_BUILD_DIR = cmake-build
CMAKE_BUILD_TYPE = ${BUILD}

CMAKE_FLAGS += -G Ninja
CMAKE_FLAGS += -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
CMAKE_FLAGS += -DCMAKE_TOOLCHAIN_FILE=${SRC_ROOT}/clang.toolchain

print_help := printf "  \033[36m%-50s\033[0m  %s\n"
print_help_group := printf "\n\033[1m%s\033[0m\n"

help:  # Display this help
	@${print_help_group} 'Common:'
	@${print_help} '<dir>/' 		'Build everything in sub-tree'
	@${print_help} '<dir>/ TEST=all' 	'Build everything and run all tests in sub-tree'
	@awk 'BEGIN {FS = ":.*##";} /^[0-9A-Za-z_.-]+:.*?##/ { printf "  \033[36m%-50s\033[0m %s\n", $$1, $$2 } /^\$$\([0-9A-Za-z_-]+\):.*?##/ { gsub("_","-", $$1); printf "  \033[36m%-50s\033[0m %s\n", tolower(substr($$1, 3, length($$1)-7)), $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)
	@awk 'BEGIN {FS = " *\??= *"; printf "\n\033[1m%s\033[0m\n", "Options:"} /^## .*/ {C=C substr($$0, 4) " "} /^[A-Z0-9_]* *\??=.*/ && C { printf "  \033[36m%s\033[0m = %*s %s\n", $$1, length($$1)-48, $$2, C} /^[^#]/ { C="" }  END {  }  ' $(MAKEFILE_LIST)
	@${print_help} 'TEST=list'		'List tests.'
	@${print_help} 'TEST=all'		'Run all tests.'
	@${print_help} 'TEST=style'		'Run style tests.'
	@${print_help} 'TEST=fail-fast'		'Run until first fail.'
	@${print_help} 'TEST=failed'		'Run failed tests.'
	@${print_help} 'TEST=debug'		'Debug failed tests.'
	@${print_help} 'TEST_FILTER=<mask>'	'Filter tests by name.'

##@ Devel:

compile_commands.json: FORCE ## Generate database for clangd (cpp language server).
	$(YATOOL) dump compile-commands --force-build-depends --output-file $@ ${ALL_DIRS}

pyrightconfig.json: vscode ## Generate config for pyright (python language server).

vscode: FORCE ## Generate vscode workspace.
	$(YATOOL) ide vscode --cpp --py3 --write-pyright-config --no-codegen --use-arcadia-root --allow-project-inside-arc --project-output=.vscode ${ALL_DIRS}

protobuf: FORCE ## Generate sources from .proto files.
	${YAMAKE} ${YAMAKE_FLAGS} --build-all --force-build-depends --replace-result --add-protobuf-result ${ALL_DIRS}

generate: FORCE ## Generate all source files.
	${YAMAKE} ${YAMAKE_FLAGS} --build-all --force-build-depends --replace-result --add-protobuf-result --add-result=.inc --add-result=.h --add-result=.cpp ${ALL_DIRS} contrib/libs/llvm16/

clangd.idx: FORCE | compile_commands.json generate ## Build static index for clangd.
	clangd-indexer --executor=all-TUs compile_commands.json >$@
	@echo "See: https://clangd.llvm.org/config#external https://clangd.llvm.org/guides/remote-index"

define clangd_idx_config
# https://clangd.llvm.org/config
Index:
  External:
    File: clangd.idx
    # Server: someserver:5900
  Background: Skip
---
# Build fresh index for some files in background.
If:
  PathMatch:
    - ""  # Placeholder to match nothing when list is empty.
    # - yt/yt/core/.*
    # - yt/yt/ytlib/.*
    # - yt/yt/library/.*
    # - yt/yt/server/lib/.*
    # - yt/yt/server/job_proxy/.*
    # - yt/yt/server/controller_agent/.*
    # - yt/yt/server/node/cluster_node/.*
    # - yt/yt/server/node/data_node/.*
    # - yt/yt/server/node/exec_node/.*
    # - yt/yt/server/node/tablet_node/.*
Index:
  Background: Build
---
endef
export clangd_idx_config

clangd.idx-config: FORCE ## Generate .clangd config to use clangd.idx in clangd.
	echo "$$clangd_idx_config" >.clangd

pick-hotfix-23.2:
	git checkout hotfix/23.2
	git reset --hard upstream/stable/23.2
	git cherry-pick -x --keep-redundant-commits `git for-each-ref --sort="-authordate" "--format=%(refname)" "refs/heads/pr/stable/23.2/"` --not upstream/stable/23.2

pick-hotfix-main:
	git checkout hotfix/main
	git reset --hard upstream/main
	git cherry-pick -x --keep-redundant-commits `git for-each-ref --sort="-authordate" "--format=%(refname)" "refs/heads/pr/main/"` --not upstream/main

##@ Targets:

%/: %/ya.make FORCE
	${YAMAKE} ${YAMAKE_FLAGS} $@

%/test-results: %/ya.make FORCE
	$(MAKE) $(dir $@) TEST?=all

ytserver-all: yt/yt/server/all/ ## Build ytserver-all
yt_local: yt/python/yt/local/bin/yt_local_make/ ## Build yt_local
yt-admin: yt/python/yt/wrapper/bin/yt-admin_make/ ## Build yt-admin
yt-client: yt/python/yt/wrapper/bin/yt_make/ ## Build yt client
yt-client-with-rpc: yt/python/yt/wrapper/bin/yt_make_with_rpc/ ## Build yt client with rpc
yt-client-with-driver: yt/python/yt/wrapper/bin/yt_make_with_driver/ ## Build yt client with native rpc

##@ Docker:

docker-ytsaurus: ## Build release docker image.
	$(YATOOL) package ${YAPACKAGE_FLAGS} yt/docker/ya-build/ytsaurus/package.json
	@cat packages.json

docker-ytsaurus-override: ## Override ytsaurus server in docker image.
	$(YATOOL) package ${YAPACKAGE_FLAGS} ${DOCKER_OVERRIDE_FLAGS} yt/docker/ya-build/ytsaurus-server-override/package.json
	@cat packages.json

docker-ytsaurus-override-debug: ## Override ytsaurus server in docker image.
	$(YATOOL) package ${YAPACKAGE_FLAGS} ${DOCKER_OVERRIDE_FLAGS} yt/docker/ya-build/ytsaurus-server-override/package-debug.json
	@cat packages.json

# /usr/include/pythonX/pyconfig.h cannot include ARCH/pythonX/pyconfig.h without -I/usr/include
# Add symlink /usr/include/pythonX/ARCH/pythonX -> /usr/include/ARCH/pythonX
hack-local-python: I=$(shell python3-config --includes | sed -n 's/^-I\(\S\+\) .*/\1/p')
hack-local-python: A=$(shell dpkg-architecture -q DEB_BUILD_MULTIARCH)
hack-local-python: ## Fix for USE_LOCAL_PYTHON in multiarch distro for docker build.
	mkdir -p ${I}/${A}
	ln -s ../../${A}/$(notdir ${I}) ${I}/${A}


# https://distribution.github.io/distribution/about/configuration/

## Port for local docker registry.
REGISTRY_LOCAL_PORT = 5000
REGISTRY_LOCAL_NAME = ${USER}-registry-localhost-${REGISTRY_LOCAL_PORT}

docker-run-local-registry: ## Run local docker registry.
	docker run --name ${REGISTRY_LOCAL_NAME} -d --restart=always \
		--mount type=volume,src=${REGISTRY_LOCAL_NAME},dst=/var/lib/registry \
		-p "127.0.0.1:${REGISTRY_LOCAL_PORT}:5000" \
		registry:2

docker-rm-local-registry: ## Remove local docker registry.
	docker rm -f ${REGISTRY_LOCAL_NAME}
	docker volume rm -f ${REGISTRY_LOCAL_NAME}

##@ Testing:

test-style: ## Run style tests.
	$(MAKE) ${ALL_DIRS} TEST?=style

test-node: ## Run node tests
	$(MAKE) yt/yt/tests/integration/node/ TEST?=all

test-node-cri: ## Run node CRI tests.
	$(MAKE) yt/yt/tests/integration/node/ TEST?=all TEST_FILTER?='*::*Cri*::*'

cmake-build: ## Initiate cmake-build.
	$(CMAKE) -B ${CMAKE_BUILD_DIR} ${CMAKE_FLAGS} ${SRC_ROOT}
	# $(CMAKE) --build ${CMAKE_BUILD_DIR}

cmake-build-ytserver-all: cmake-build FORCE ## Build ytserver-all in cmake.
	ninja -C ${CMAKE_BUILD_DIR} ytserver-all

##@ Clean:

clean-out: ## Clean out/
	rm -fr out/*

clean-cmake: ## Cleanup cmake build.
	rm -fr ${CMAKE_BUILD_DIR}

clean-build-cache: ## Clean build cache.
	rm -fr ${HOME}/.ya/build/cache

clean-tools-cache: ## Clean tools cache.
	rm -fr ${HOME}/.ya/tools

build-cache-size: ## Get build cache size limit.
	@echo $$(( $$( $(YATOOL) gen-config | sed -nE 's/^(# )?cache_size = ([0-9]*)/\2/p') >> 30 )) GiB

build-cache-gc: ## Run build cache gc.
	${YATOOL} gc cache --gc

FORCE:

.PHONY: $(PHONY)
