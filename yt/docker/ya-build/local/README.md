# Building the ytsaurus local image from source

This configuration allows building the ytsaurus local image from source.
The bundled python packages are also built from source.

Example: `ya package package.json --docker-registry my-registry.com`

The `--docker-registry` parameter only impacts the resulting image name, which will be `my-registry.com/local:local-<commit-SHA>` in this case.

The `--custom-version` parameter can be used to override the version template specified in `package.json` with your custom string.
