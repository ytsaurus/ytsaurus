# Building the ytsaurus core image by overriding server binaries

This configuration allows overriding the server binaries in an existing `ytsaurus` core image by local ones built from source.

Example: `ytsaurus/ya package package.json --docker-registry my-registry.com --docker-build-arg "BASE_IMAGE=dev-24.1"`

The base image used by default is `ytsaurus:dev`.
The purpose of the base image is to provide yt python/cli packages with support for all drivers, including the native driver. The native driver is necessary if you are running YTsaurus with its k8s-operator.

The `--docker-registry` parameter only impacts the resulting image name, which will be `my-registry.com/ytsaurus:local-<commit-SHA>` in this case.

The `--custom-version` parameter can be used to override the version template specified in `package.json` with your custom string.
