# If you update this file, please follow
# https://www.thapaliya.com/en/writings/well-documented-makefiles/

.DEFAULT_GOAL := help

SRC_ROOT:=$(shell pwd)

YATOOL = ./ya
YAMAKE = $(YATOOL) make

## Release | RelWithDebInfo | FastDebug | Debug
BUILD = Debug

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
	clangd-indexer --executor=all-TUs compile_commands.json > $@
	@echo "See: https://clangd.llvm.org/config#external https://clangd.llvm.org/guides/remote-index"

define clangd_idx_config
# https://clangd.llvm.org/config
Index:
  External:
    File: clangd.idx
  Background: Skip
---
If:
  PathMatch:
    # - yt/yt/core/.*
    # - yt/yt/server/lib/.*
    # - yt/yt/server/job_proxy/.*
    # - yt/yt/server/node/exec_node/.*
    # - yt/yt/library/containers/.*
Index:
  Background: Build
endef
export clangd_idx_config

clangd.idx-config: FORCE ## Generate .clangd config to use clangd.idx in clangd.
	echo "$$clangd_idx_config" >.clangd

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

build-cache-size: ## Get build cache size.
	@echo $$(( `$(YATOOL) gen-config | sed -nE 's/^(# )?cache_size = ([0-9]*)/\2/p'` >> 30 )) MiB

build-cache-gc: ## Run build cache gc.
	${YATOOL} gc cache --gc

FORCE:

.PHONY: $(PHONY)
