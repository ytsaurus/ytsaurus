
# If you update this file, please follow
# https://www.thapaliya.com/en/writings/well-documented-makefiles/

.DEFAULT_GOAL:=help

SRC_ROOT:=$(shell pwd)

YATOOL=./ya
YAMAKE=$(YATOOL) make

BUILD=Debug
TARGET_PLATFORM=
SANITIZE=
INSTALL_DIR=
V=1

# Default output directory for tests
ifeq (${O},)
  O = out/$(shell date +%Y-%m-%d-%H-%M-%S)
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

ifneq (${O},)
  TEST_FLAGS += --output=${O}
  TEST_FLAGS += --junit=${O}/results.xml
endif

ifneq (${TEST_FILTER},)
  TEST_FLAGS += --test-filter="${TEST_FILTER}"
endif

# All reasonable non-broken target directories
ALL_DIRS ?= yt/yt/ yt/python/ yt/go/ yt/odin/ yt/yql/

YAMAKE_FLAGS = ${BUILD_FLAGS}

ifneq (${TEST},)
  YAMAKE_FLAGS += ${TEST_FLAGS}
endif

CMAKE=cmake

CMAKE_BUILD_DIR=cmake-build
CMAKE_BUILD_TYPE=${BUILD}

CMAKE_FLAGS += -G Ninja
CMAKE_FLAGS += -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
CMAKE_FLAGS += -DCMAKE_TOOLCHAIN_FILE=${SRC_ROOT}/clang.toolchain

print_help := printf "  \033[36m%-50s\033[0m  %s\n"
print_help_group := printf "\n\033[1m%s\033[0m\n"

help:  # Display this help
	@${print_help_group} 'Common:'
	@${print_help} '<dir>/' 		'Build all in tree'
	@${print_help} '<dir>/ TEST=all' 	'Build all and run all tests'
	@awk 'BEGIN {FS = ":.*##";} /^[0-9A-Za-z_.-]+:.*?##/ { printf "  \033[36m%-50s\033[0m %s\n", $$1, $$2 } /^\$$\([0-9A-Za-z_-]+\):.*?##/ { gsub("_","-", $$1); printf "  \033[36m%-50s\033[0m %s\n", tolower(substr($$1, 3, length($$1)-7)), $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)
	@${print_help_group} 'Options:'
	@${print_help} 'BUILD=Release|RelWithDebInfo|FastDebug|Debug' 'default: Debug'
	@${print_help} 'SANITIZE=Address|Memory|Thread|Undefined|Leak' 'enable sanitizer'
	@${print_help} 'V=0|1|2' 		'verbosity, default 1'
	@${print_help} 'O=<dir>' 		'test output directory, default out/<time>'
	@${print_help} 'INSTALL_DIR=<dir>'	'path for resulting binaries'
	@${print_help} 'TEST=list'		'list tests'
	@${print_help} 'TEST=all'		'run all tests'
	@${print_help} 'TEST=style'		'run style tests'
	@${print_help} 'TEST=fail-fast'		'run until first fail'
	@${print_help} 'TEST=failed'		'run failed tests'
	@${print_help} 'TEST=debug'		'debug failed tests'
	@${print_help} 'TEST_FILTER=<mask>'	'filter tests by name'

##@ Devel:

compile_commands.json: FORCE ## Generate database for clangd: cpp language server.
	$(YATOOL) dump compile-commands --force-build-depends --output-file $@ ${ALL_DIRS}

pyrightconfig.json: vscode ## Generate config for pyright: python language server.

vscode: FORCE ## Generate vscode workspace.
	$(YATOOL) ide vscode --cpp --py3 --write-pyright-config --no-codegen --use-arcadia-root --allow-project-inside-arc --project-output=.vscode ${ALL_DIRS}

protobuf: FORCE ## Generate sources from .proto files.
	$(YATOOL) make --add-protobuf-result --replace-result yt/yt

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
