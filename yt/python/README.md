## How to use Python API

Just install `ytsaurus-client` using `pip`.

Detailed documentation can be found [here](https://ytsaurus.tech/docs/ru/api/python/start).


## How to build and develop python libraries

### Preparation

To work with python libraries from you should prepare environment and modules layout:
  1. [Build](BUILD.md) native libraries presented by cmake targets `yson_lib`, `driver_lib`, `driver_rpc_lib` 
  2. Install [virtualenv](https://virtualenv.pypa.io/en/latest/) and [protoc](https://github.com/protocolbuffers/protobuf/releases)
  3. Install `yt_setup` in editable mode `pip install -e yt/python/packages`, this library provide instruments to prepare modules layout
  4. Create directory `<ytsaurus_python>` to store prepared modules
  5. Run `generate_python_proto --source-root <checkout_directory> --output <ytsaurus_python>` to build proto modules (it requires protoc to be installed)
  6. Run `prepare_source_tree --source-root <checkout_directory> --build-root <build_root> --output-path <ytsaurus_python> --prepare-bindings-libraries` to prepare all python modules

After that you can add `<ytsaurus_python>` to PYTHONPATH and ytsaurus python libraries.

### Packaging

To build package just copy files from `yt/python/packaging/<package_name>/` to the `<ytsaurus_python>` and run setup script.

For pure python libraries we recommend to run `python3 setup.py bdist_wheel --universal`, for binary libraries to run `python3 setup.py bdist_wheel ---py-limited-api cp34``
