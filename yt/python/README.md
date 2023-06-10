## How to use Python API

Just install `ytsaurus-client` using `pip`.

Detailed documentation can be found [here](https://ytsaurus.tech/docs/ru/api/python/start).


## How to build and develop python libraries
In this README we will be assuming you are not root.

### Preparation and installation

We suppose that you use venv for python playground: [how to install it](https://docs.python.org/3/library/venv.html).

Let call project checkout directory as `<source_root>` and project build directory as `<build_root>`.

```
export SOURCE_ROOT=<source_root>
export BUILD_ROOT=<build_root>
```

To work with ytsaurus python libraries you should prepare environment and modules:
  1. [Build](https://github.com/ytsaurus/ytsaurus/blob/main/BUILD.md) native libraries presented by cmake targets `yson_lib`, `driver_lib`, `driver_rpc_lib`
```
cd "$BUILD_ROOT" && ninja yson_lib driver_lib driver_rpc_lib
```

  2. Create directory to store prepared modules, let call it <ytsaurus_python> 
```
mkdir "$PYTHON_ROOT"
```

  3. Install wheel package `pip3 install wheel`

  4. Install `yt_setup` package in editable mode, this library provide instruments to prepare modules layout.
```
cd "$SOURCE_ROOT" && pip install -e yt/python/packages
```
  
  5. Run `generate_python_proto` to build proto modules (it requires protoc to be installed, see [build instruction](https://github.com/ytsaurus/ytsaurus/blob/main/BUILD.md)).
```
generate_python_proto --source-root "$SOURCE_ROOT" --output "$PYTHON_ROOT"
```

  6. Run `prepare_python_modules` to prepare all python modules
```
prepare_python_modules --source-root "$SOURCE_ROOT" --build-root "$BUILD_ROOT" --output-path "$PYTHON_ROOT" --prepare-bindings-libraries
```

After that ytsaurus python libraries can be used by adding `<ytsaurus_python>` to PYTHONPATH.

### Packaging

To build package you need to copy `setup.py` from `yt/python/packages/<package_name>/` to the `<ytsaurus_python>` and run setup script.

For pure python libraries (ytsaurus-client, ytsaurus-local) you should run `python3 setup.py bdist_wheel --universal`, for binary libraries (ytsaurus-yson, ytsaurus-rpc-driver) you should run `python3 setup.py bdist_wheel --py-limited-api cp34`

You will find wheel files .whl files in `$PYTHON_ROOT/dist` directory, alternatively you can specify `--dist-dir` option.

### Building pydoc

To build python docs in HTML format perform following steps:

  1. Install `sphinx` library
```
pip install sphinx
```

  2. Create directory for documentation files
```
export DOCS_ROOT=`<docs_root>`
mkdir "$DOCS_ROOT"
```

  3. Run following commands
```
cd "$PYTHON_ROOT""
PYTHONPATH="$PYTHON_ROOT" sphinx-apidoc -F -o "$DOCS_ROOT" yt yt/packages yt/test_helpers yt/testlib yt/tool yt/environment yt/local yt/entry
PYTHONPATH="$PYTHON_ROOT" sphinx-build -b html "$DOCS_ROOT" "$DOCS_ROOT/_html"
```

After that you can find a cluster of HTML documents in `$DOCS_ROOT/_html`
