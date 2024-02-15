# Versioning rules

SPYT includes two components:
- A cluster with Spark pre-installed.
- A client where the `ytsaurus-spyt` package is installed via `pip`.

SPYT is built from the following artifacts:
- A `.tar` archive with patched Spark that is deployed to {{product-name}} and read at cluster startup;
- `ytsaurus-pyspark` pip package with patched Spark files as in the `.tar` above;
- `ytsaurus-spyt` pip package which in turn depends on `ytsaurus-pyspark` package. It contains the assembly of all SPYT modules and their dependencies that are not included in Spark distributive. The package is deployed to pypi repository and client hosts. It also contains CLI utilities for launching an inner Spark standalone cluster and for submitting applications to it.

When a new version of `ytsaurus-spyt` is installed `ytsaurus-pyspark` updates automatically.

In most cases, a `ytsaurus-spyt` update is sufficient. However, sometimes, to make new functionality work, you will have to update your cluster. You can find out the update procedure and scope based on the specific version.


## Update procedure { #how }


- Cluster and client version number consists of three parts. Update of the last (minor) part means that backward compatibility is preserved and the change affects only one component.
- When a middle (major) part of the version is updated backward compatibility with clusters of previous versions may not be preserved so it is recommended to use a client with the same major version as cluster.
- Cluster version can be chosen on launching via `spark-launch-yt` in `--spyt-version` parameter. When the version is not specified the last release version will be used.
- Client version in `client mode` (Jupyter notebooks) is the same as `ytsaurus-spyt` package version.
- Client version in `cluster mode` can be specified using `--spyt-version` parameter for `spark-submit-yt` command. When the version is not specified the last compatible with the cluster version will be used.


