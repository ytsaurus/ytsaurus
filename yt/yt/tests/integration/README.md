## How to run integration tests

**Important:** we suppose that you use virtualenv for python playground: see instruction [how to install it](https://docs.python.org/3/library/venv.html).

1. Prepare [python modules](https://github.com/ytsaurus/ytsaurus/blob/main/yt/python/README.md) of the project. As a result following environment variables should be defined:
  * `$BUILD_ROOT` pointing to the build directory,
  * `$SOURCE_ROOT` pointing to the checkout directory,
  * `$PYTHON_ROOT` pointing to the directory with prepared python modules.
2. Enter tests directory:
```
cd "$SOURCE_ROOT/yt/yt/tests/integration"
```
3. Install python libraries required for tests:
```
pip install -r requirements.txt
```
4. Prepare binary symlinks:
```
mkdir -p ${BUILD_ROOT}/yt/yt/packages/tests_package/
ln -s ${BUILD_ROOT}/yt/yt/server/all/ytserver-all ${BUILD_ROOT}/yt/yt/packages/tests_package/ytserver-all

mkdir -p ${BUILD_ROOT}/yt/python/yt/environment/bin/
ln -s ${SOURCE_ROOT}/yt/python/yt/environment/bin/yt_env_watcher ${BUILD_ROOT}/yt/python/yt/environment/bin/yt_env_watcher

mkdir -p ${BUILD_ROOT}/yt/yt/tools/prepare_scheduling_usage/
ln -s ${SOURCE_ROOT}/yt/yt/tools/prepare_scheduling_usage/__main__.py ${BUILD_ROOT}/yt/yt/tools/prepare_scheduling_usage/prepare_scheduling_usage
```
5. Create sandbox directory for tests and export it as `TESTS_SANDBOX`
```
mkdir <tests_sandbox>
export TESTS_SANDBOX="<tests_sandbox>"
```
6. Congratulations! Everything is prepared to run integration tests.

Run tests marked as opensource:
```
./run_tests.sh -m opensource
```

Run some specific test using pytest filtration:
```
./run_tests.sh -k "test_hot_standby"
```

Note: do not try run all tests, they have no chance to finish in reasonable amount of time.
