# Building the chyt core image from source

This configuration allows building the `chyt` image from source.

Example: `ytsaurus/ya package package.json --docker-registry my-registry.com`

The `--docker-registry` parameter only impacts the resulting image name, which will be `my-registry.com/chyt:local-<commit-SHA>` in this case.

The `--custom-version` parameter can be used to override the version template specified in `package.json` with your custom string.
