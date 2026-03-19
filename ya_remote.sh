#!/bin/bash

set -euo pipefail

REMOTE_HOST="${REMOTE_HOST:-yt-cloud-dev}"
REMOTE_BASE_DIR="${REMOTE_BASE_DIR:-~/ytsaurus}"
REMOTE_REPO_DIR="${REMOTE_REPO_DIR:-$REMOTE_BASE_DIR/ytsaurus}"
BUILD_TYPE="${BUILD_TYPE:-release}"
TEST_SIZE="${TEST_SIZE:-none}"
JOBS="${JOBS:-}"
TARGETS=()
EXTRA_YA_ARGS=()
REMOTE_RUN_CMD=""
COPY_BACK_PATHS=()
TARGET_WAS_EXPLICIT=0

usage() {
    echo "Usage: $0 [options] [-- <extra ya args>]"
    echo
    echo "Options:"
    echo "  --host <host>             Remote host (default: yt-cloud-dev or REMOTE_HOST)"
    echo "  --remote-dir <dir>        Remote repository directory (default: ~/ytsaurus/ytsaurus)"
    echo "  --target <path>           Ya build target, can be passed multiple times"
    echo "  --build-type <release|debug>"
    echo "                           Build type (default: release or BUILD_TYPE)"
    echo "  --test-size <none|small|medium|large>"
    echo "                           Run tests of selected size (default: none or TEST_SIZE)"
    echo "  --jobs <N>                Pass -jN to ya make"
    echo "  --run-remote <cmd>        Run shell command remotely after ya make (inside repo dir)"
    echo "  --copy-back <path>        Copy file from remote repo back to local (repeatable)"
    echo "  -h, --help                Show this help"
    echo
    echo "Defaults from BUILD.md:"
    echo "  build target: yt/yt"
    echo "  release build: ./ya make -r yt/yt"
    echo "  tests: add -t / -tt / -ttt"
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --host)
            REMOTE_HOST="$2"
            shift 2
            ;;
        --remote-dir)
            REMOTE_REPO_DIR="$2"
            shift 2
            ;;
        --target)
            TARGETS+=("$2")
            TARGET_WAS_EXPLICIT=1
            shift 2
            ;;
        --build-type)
            BUILD_TYPE="$2"
            shift 2
            ;;
        --test-size)
            TEST_SIZE="$2"
            shift 2
            ;;
        --jobs)
            JOBS="$2"
            shift 2
            ;;
        --run-remote)
            REMOTE_RUN_CMD="$2"
            shift 2
            ;;
        --copy-back)
            COPY_BACK_PATHS+=("$2")
            shift 2
            ;;
        --)
            shift
            EXTRA_YA_ARGS+=("$@")
            break
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            echo "Unknown argument: $1" >&2
            usage >&2
            exit 1
            ;;
    esac
done

if [[ ${#TARGETS[@]} -eq 0 ]]; then
    TARGETS=("yt/yt")
fi

case "$BUILD_TYPE" in
    release)
        BUILD_FLAG="-r"
        ;;
    debug)
        BUILD_FLAG="-d"
        ;;
    *)
        echo "Invalid build type: $BUILD_TYPE" >&2
        exit 1
        ;;
esac

case "$TEST_SIZE" in
    none)
        TEST_FLAG=""
        ;;
    small)
        TEST_FLAG="-t"
        ;;
    medium)
        TEST_FLAG="-tt"
        ;;
    large)
        TEST_FLAG="-ttt"
        ;;
    all)
        TEST_FLAG="--run-all-tests"
        ;;
    *)
        echo "Invalid test size: $TEST_SIZE" >&2
        exit 1
        ;;
esac

if [[ -n "$TEST_FLAG" ]]; then
    HAS_RUN_ALL_TESTS=0
    HAS_TEST_FILTER=0
    HAS_INTEGRATION_TARGET=0

    for arg in "${EXTRA_YA_ARGS[@]}"; do
        if [[ "$arg" == "--run-all-tests" || "$arg" == "-A" ]]; then
            HAS_RUN_ALL_TESTS=1
        fi
        if [[ "$arg" == "-F" || "$arg" == --test-filter=* ]]; then
            HAS_TEST_FILTER=1
        fi
    done

    for target in "${TARGETS[@]}"; do
        if [[ "$target" == yt/yt/tests/integration* ]]; then
            HAS_INTEGRATION_TARGET=1
        fi
    done

    if [[ "$TARGET_WAS_EXPLICIT" -ne 1 ]]; then
        echo "For tests, always pass an explicit --target directory." >&2
        exit 1
    fi

    if [[ "$HAS_RUN_ALL_TESTS" -ne 1 ]]; then
        echo "For tests, always pass --run-all-tests after --." >&2
        exit 1
    fi

    if [[ "$HAS_INTEGRATION_TARGET" -eq 1 && "$HAS_TEST_FILTER" -ne 1 ]]; then
        echo "For integration tests, always pass an explicit test filter: -F \"*test_name*\"." >&2
        exit 1
    fi
fi

YA_ARGS=(make "$BUILD_FLAG")

if [[ -n "$TEST_FLAG" ]]; then
    YA_ARGS+=("$TEST_FLAG")
fi

if [[ -n "$JOBS" ]]; then
    YA_ARGS+=("-j$JOBS")
fi

YA_ARGS+=("${TARGETS[@]}")

if [[ ${#EXTRA_YA_ARGS[@]} -gt 0 ]]; then
    YA_ARGS+=("${EXTRA_YA_ARGS[@]}")
fi

RSYNC_EXCLUDES=(
    --exclude .git/
    --exclude .cache/
    --exclude .idea/
    --exclude .venv/
    --exclude '*.pyc'
    --exclude '__pycache__/'
)

LOCAL_REPO_ROOT="$(pwd)"

if [[ ! -x "$LOCAL_REPO_ROOT/ya" ]]; then
    echo "Run this script from the repository root containing ./ya" >&2
    exit 1
fi

REMOTE_HOME="$(ssh "$REMOTE_HOST" 'printf %s "$HOME"')"
REMOTE_REPO_DIR_RESOLVED="${REMOTE_REPO_DIR/#\~/$REMOTE_HOME}"

if [[ -z "$REMOTE_REPO_DIR_RESOLVED" || "$REMOTE_REPO_DIR_RESOLVED" == "/" || "$REMOTE_REPO_DIR_RESOLVED" == "$REMOTE_HOME" ]]; then
    echo "Unsafe remote repo directory for rsync --delete: '$REMOTE_REPO_DIR_RESOLVED'" >&2
    exit 1
fi

for copy_back_path in "${COPY_BACK_PATHS[@]}"; do
    if [[ "$copy_back_path" = /* || "$copy_back_path" == *".."* ]]; then
        echo "Unsafe --copy-back path: '$copy_back_path'. Use a relative path inside repository." >&2
        exit 1
    fi
done

echo "Syncing $LOCAL_REPO_ROOT to $REMOTE_HOST:$REMOTE_REPO_DIR_RESOLVED"
ssh "$REMOTE_HOST" "mkdir -p \"$(dirname "$REMOTE_REPO_DIR_RESOLVED")\" && mkdir -p \"$REMOTE_REPO_DIR_RESOLVED\""
rsync -az --delete "${RSYNC_EXCLUDES[@]}" "$LOCAL_REPO_ROOT/" "$REMOTE_HOST:$REMOTE_REPO_DIR_RESOLVED/"

REMOTE_YA_CMD="./ya"
for arg in "${YA_ARGS[@]}"; do
    REMOTE_YA_CMD+=" $(printf '%q' "$arg")"
done

echo "Running on $REMOTE_HOST: $REMOTE_YA_CMD"
ssh -t "$REMOTE_HOST" "bash -lc 'set -euo pipefail; cd \"$REMOTE_REPO_DIR_RESOLVED\"; $REMOTE_YA_CMD'"

if [[ -n "$REMOTE_RUN_CMD" ]]; then
    echo "Running post-build command on $REMOTE_HOST: $REMOTE_RUN_CMD"
    ssh -t "$REMOTE_HOST" "bash -lc 'set -euo pipefail; cd \"$REMOTE_REPO_DIR_RESOLVED\"; $REMOTE_RUN_CMD'"
fi

if [[ ${#COPY_BACK_PATHS[@]} -gt 0 ]]; then
    for copy_back_path in "${COPY_BACK_PATHS[@]}"; do
        local_dir="$(dirname "$LOCAL_REPO_ROOT/$copy_back_path")"
        mkdir -p "$local_dir"
        echo "Copying back $copy_back_path"
        rsync -az "$REMOTE_HOST:$REMOTE_REPO_DIR_RESOLVED/$copy_back_path" "$LOCAL_REPO_ROOT/$copy_back_path"
    done
fi
