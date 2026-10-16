# Building an offline Docker image of SPYT

During a normal SPYT deployment, the Spark distribution and supporting artifacts are downloaded from the internet (Apache archive, Maven Central), and the control image is pulled from the `ghcr.io` registry. In isolated environments without internet access, this is not possible.

To deploy SPYT under such conditions, an **offline image** is used — a self-contained Docker image `ghcr.io/ytsaurus/spyt:<spyt-version>-pyspark-<spark-version>` that includes the required SPYT build and the Spark distribution. When deploying, this image does not need internet access: all artifacts are taken from the image itself.

{% note warning %}

The Spark version must be `3.4.0` or upper — the distribution embedded into the image only for such kind of versions. The offline image is not supported for lower Spark versions.

{% endnote %}

## How to build the image {#build}

Build executon runs from the [ytsaurus-spyt](https://github.com/ytsaurus/ytsaurus-spyt) repository at the required release tag. First, clone the repository:

```bash
git clone https://github.com/ytsaurus/ytsaurus-spyt.git
cd ytsaurus-spyt
```

The repository includes a helper script [`tools/release/build_offline_image.sh`](https://github.com/ytsaurus/ytsaurus-spyt/blob/main/tools/release/build_offline_image.sh) performing all the build steps. You should run it from the root of the cloned repository (or specify the path to it with the `--repo <dir>` flag).

For example, to build an image for SPYT `2.9.2` and Spark `3.5.8` run the command:

```bash
tools/release/build_offline_image.sh --spyt-version 2.9.2 --spark-version 3.5.8 --checkout
```

The output will be represented as a local image `ghcr.io/ytsaurus/spyt:2.9.2-pyspark-3.5.8`.

You should know that the `--checkout` flag is **optional**. If you pass it, the script will switch the repository to the release tag `spyt/<spyt-version>` (`git fetch --tags` + `git checkout`). All the tracked files must have the committed changes only, otherwise the script stops with an error. 

If you do not pass the flag, the build will run from the current state of the working copy — this is convenient when you are on the required tag already or building an image from your own branch. Anyway the `--spyt-version` and `--spark-version` flags are required: they specify the SPYT artifact version, the version of the embedded Spark distribution, and the image tag.

### Build steps {#build-steps}

The script performs the next steps sequentially:

1. `git checkout tags/spyt/<spyt-version>` — switches to the release tag (with the `--checkout` flag only).
2. `./gradlew assemble -PcustomSpytVersion=<spyt-version>` — builds the SPYT artifacts (requires JDK 17).
3. Downloads the supporting Livy distribution, if required by the Dockerfile for this version.
4. `tools/release/spyt_image/build.sh --spyt-version <spyt-version> --spark-version <spark-version>` — builds the image with the embedded Spark distribution.
5. Verifies the image built successfully.

Also you can perform these steps manually without the script if needed.

## Publishing to a custom registry {#registry}

The built image is only available locally. In an isolated environment, the deployment host usually does not have access to `ghcr.io` but does have access to an internal (corporate) Docker registry. Publish the offline image to such a registry to deploy SPYT from it instead of from the public `ghcr.io`.

Tag the image with your registry and push it using the command:

```bash
docker tag ghcr.io/ytsaurus/spyt:<spyt-version>-pyspark-<spark-version> \
  <your-registry>/ytsaurus/spyt:<spyt-version>-pyspark-<spark-version>
docker push <your-registry>/ytsaurus/spyt:<spyt-version>-pyspark-<spark-version>
```

For example, for SPYT `2.9.2`, Spark `3.5.8`, and registry `registry.example.com`:

```bash
docker tag ghcr.io/ytsaurus/spyt:2.9.2-pyspark-3.5.8 \
  registry.example.com/ytsaurus/spyt:2.9.2-pyspark-3.5.8
docker push registry.example.com/ytsaurus/spyt:2.9.2-pyspark-3.5.8
```

Alternatively, build the image directly for the required registry — pass its prefix into `--image-cr`, and `--push` will publish the result:

```bash
tools/release/build_offline_image.sh \
  --spyt-version <spyt-version> --spark-version <spark-version> \
  --image-cr <your-registry>/ --push --checkout
```

For example:

```bash
tools/release/build_offline_image.sh \
  --spyt-version 2.9.2 --spark-version 3.5.8 \
  --image-cr registry.example.com/ --push --checkout
```

Specify the resulting image `<your-registry>/ytsaurus/spyt:<spyt-version>-pyspark-<spark-version>` during the deployment.

## How to use {#deploy}

The ready image deploys SPYT and the Spark distribution to a {{product-name}} cluster. Run a container from the image you built and published, specifying the proxy address and access parameters:

```bash
docker run --network=host \
  -e YT_PROXY=<cluster-proxy> \
  -e YT_USER=<user> \
  -e YT_TOKEN=<token> \
  -e SPARK_DISTRIB_OFFLINE=true \
  -e SPARK_DISTRIB_VERSIONS=<spark-version> \
  --rm <your-registry>/ytsaurus/spyt:<spyt-version>-pyspark-<spark-version>
```

The last argument is the image to run. In an isolated environment, this is the image from your registry (see [Publishing to a custom registry](#registry)). The `--rm` flag is optional — it removes the container after it finishes. The deployment is one-time, the container runs and exits.

If you did not publish to a registry and you are building and deploying on the same machine, specify the locally built image (its default tag is `ghcr.io/ytsaurus/spyt:<spyt-version>-pyspark-<spark-version>`). `docker run` will use the ready image from the local Docker cache on that machine and will not download anything.

For example, for SPYT `2.9.2`, Spark `3.5.8`, and registry `registry.example.com`:

```bash
docker run --network=host \
  -e YT_PROXY=cluster.example.com \
  -e YT_USER=spyt-deployer \
  -e YT_TOKEN=<token> \
  -e SPARK_DISTRIB_OFFLINE=true \
  -e SPARK_DISTRIB_VERSIONS=3.5.8 \
  --rm registry.example.com/ytsaurus/spyt:2.9.2-pyspark-3.5.8
```

Environment variables control the publication of the Spark distribution to the cluster:

| Variable | Purpose |
| --- | --- |
| `SPARK_DISTRIB_OFFLINE=true` | Full offline mode: use only the Spark distribution embedded in the image, do not download anything. |
| `SPARK_DISTRIB_VERSIONS` | Space-separated list of Spark versions to publish to the cluster. |

## How to verify that deployment was successful {#check}

The container runs and exits with a zero exit code. The message `Publication finished successfully` in the container log means that the SPYT files have been published. After that, the container uploads the Spark distribution to the cluster. A non-zero exit code means that the deployment did not succeed.

Additionally, verify that the artifacts appeared on the cluster (paths are shown for SPYT `2.9.2` and Spark `3.5.8`):

```bash
yt list //home/spark/spyt/releases/2.9.2      # spyt-package.zip, setup-spyt-env.sh
yt list //home/spark/conf/releases/2.9.2      # spark-launch-conf and sidecar configs
yt list //home/spark/distrib/3/5/8            # spark-3.5.8-bin-hadoop3.tgz, spark-connect_2.12-3.5.8.jar
```

After that, you can [launch Spark applications](../../../../user-guide/data-processing/spyt/launch.md) as usual.
