Odin is a service to run checks on a cluster periodically, it is used as an external moniroting tool.

## How to build

### Prepare odin libraries

Prepare python libraries with [instruction](https://github.com/ytsaurus/ytsaurus/tree/main/yt/python#preparation-and-installation).

We assume that environment variables `SOURCE_ROOT`, `BUILD_ROOT` and `PYTHON_ROOT` are defined and python  virtual environment is prepared.

To prepare odin do the next steps:

1. Install additional python libraries
```
pip install -r $SOURCE_ROOT/yt/odin/requirements.txt
```
2. Prepare odin modules
```
$SOURCE_ROOT/yt/odin/packages/prepare_python_modules.py --source-root "$SOURCE_ROOT" --output-path "$PYTHON_ROOT"
```
3. Install odin modules
```
cd "$PYTHON_ROOT"
cp "$SOURCE_ROOT/yt/odin/packages/setup.py" .
python3 setup.py install
```

### Run tests
1. Install python libraries required for tests:
```
pip install -r "${SOURCE_ROOT}/yt/odin/tests/requirements.txt"
```
2. Prepare ytserver symlink:
```
mkdir -p ${BUILD_ROOT}/yt/yt/packages/tests_package/
ln -s ${BUILD_ROOT}/yt/yt/server/all/ytserver-all ${BUILD_ROOT}/yt/yt/packages/tests_package/ytserver-all
```
3. Create sandbox directory for tests and export it as `TESTS_SANDBOX`
```
mkdir <tests_sandbox>
export TESTS_SANDBOX="<tests_sandbox>"
```
4. Run tests
```
cd "$SOURCE_ROOT/yt/odin/tests"
YT_BUILD_ROOT="$BUILD_ROOT" YT_TESTS_SANDBOX="$TESTS_SANDBOX" python -m pytest
```

### Generate configuration and run service

This instruction describes how to run Odin service on the cluster. Note that this cluster is also used as a database for the Odin (in production installations we recommend to use a separate cluster for Odin database).

Suppose that `<proxy>` is an address of your cluster, `<token_file>` points to the file with valid token for this cluster and `<odin_dir>` is a working directory for Odin. 

1. Prepare tables for Odin
```
yt_odin_init_db_cluster --table //sys/odin/checks --proxy "<proxy>" --tablet-cell-bundle default
```
2. Go to Odin working directory
```
cd "<odin_dir>"
```
3. Generate configuration for Odin services
```
yt_odin_generate_config --db-cluster-proxy "<proxy>" --db-cluster-token-file "<token_file>" --check-cluster-token-file "<token_file> --check-cluster-proxy "<proxy>" --odin-config-output "din_config.json" --odin-webservice-config-output "odin_webservice_config.json" --odin-checks-directory "odin_checks"
```
4. Run Odin service
```
yt_odin --config odin_config.json
```
5. Run Odin webservice
```
yt_odin_webservice --config odin_webservice_config.json
```


### Docker image

It is possible to build a self-confined Odin image using the provided [Dockerfile](./Dockerfile). The image will be built using a [multi-stage build](https://docs.docker.com/build/building/multi-stage/) approach where only the necessary runtime files and dependencies are preserved in the final stage.

To build the image run the following commands:

```bash
cd $SOURCE_ROOT/yt/odin

# ytsaurus-odin:latest is the label:tag of the resulting image
docker build -t ytsaurus-odin:latest .
```

To run Odin inside a container do the following:

* prepare `odin_config.json`, `checks_config.json` and `odin_webservice.json` files
* run odin containers:

```bash
# run odin checker component
docker run --name odin \
    -v /path/to/odin_config.json:/path/to/odin_config.json:ro \
    -v /path/to/checks_config.json:/path/to/checks_config.json:ro
    --entrypoint yt_odin \
    ytsaurus-odin:latest \
    --config /path/to/odin_config.json

# run odin webservice component
docker run --name odin-webservice \
    -v /path/to/odin_webservice.json:/path/to/odin_webservice.json:ro \
    -p 8080:8080 \
    --entrypoint yt_odin_webservice \
    ytsaurus-odin:latest \
    --config /path/to/odin_webservice.json
```
