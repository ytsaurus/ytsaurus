# Building the ytsaurus core image from source

This configuration allows building the core ytsaurus image from source.
The bundled python packages are also built from source.

Example: `ytsaurus/ya package package.json --docker --docker-registry my-registry.com`

The `--docker-registry` parameter only impacts the resulting image name, which will be `my-registry.com/ytsaurus:local-<commit-SHA>` in this case.

The `--custom-version` parameter can be used to override the version template specified in `package.json` with your custom string.

## A note on python libararies

YTsaurus python libraries require pre-compiled .so-files, which are built using the local python headers on your host machine.
More specifically, `python3` and `python3-config` from your host machine are used.
If you want to use another python version for building your image, you can override these parameters in `package.json`.

### Multi-arch systems

Currently, [multi-arch](https://wiki.debian.org/Multiarch/Implementation) systems and `USE_LOCAL_PYTHON=yes` in `ya make` do not work together well.
In order for the build to work, you need to provide python-specific includes (e.g. `/usr/include/<arch-triple>/python3.x`) as a separate include-dir in such a way, that no other headers will clash.
A simple way to do this is to copy `/usr/include/<arch-triple>/python3.x` into `/my-temp-dir/<arch-triple>/python3.x` and run the `ya package` command with `-DCFLAGS="-I/my/temp-dir`.

