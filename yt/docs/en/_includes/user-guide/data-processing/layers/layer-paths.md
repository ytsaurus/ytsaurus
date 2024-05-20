# Root file system images

A root file system image contains a stand-alone OS distribution together with essential apps and libraries for running user commands in an isolated container.

Depending on the cluster configuration (`exec_node/slot_manager/job_environment/type` in the exec node configuration file), the execution environment can support different image formats and sources:
- **simple**: Doesn't support isolation.
- **cri**: Docker image.
- **porto**: Porto layers, Docker image from the Internal Docker Registry.

## Docker images { #docker_images }

### Terminology

- **Docker layer**: a fragment of data from a root file system image.
- **Docker image**: an image of a root file system consisting of metadata and an ordered set of layers.
- **Docker registry**: a server for storing and distributing Docker image data and metadata.
- **Internal Docker Registry**: a Docker registry that stores images directly in [Cypress](../../../../user-guide/storage/cypress.md).

Docker images are referenced by name in the following format: `[REGISTRY/]IMAGE[:TAG|@DIGEST]`, where `REGISTRY` represents the `FQDN[:PORT]` of the Docker registry server. Registry `FQDN` must has at least one "." or `PORT`. By default, `TAG` is set to **latest**. [More](https://docs.docker.com/engine/reference/commandline/pull/)

Docker images without a specified `REGISTRY` are assumed to be from the **Internal Docker Registry**, where `IMAGE` corresponds to the `//IMAGE` path pointing to the directory with the image metadata in Cypress.

The Internal Docker Registry can be accessed via the standard Docker registry protocol for uploading images using Docker or Docker-compatible tools. It doesn't support referring to Docker images with a `DIGEST`.

To use docker images from [Docker Hub](https://hub.docker.com), provide their full name in the following format: `docker.io/[library/]IMAGE[:TAG|@DIGEST]`.

To run jobs using Docker images from an external Docker registry, the cluster's exec nodes require network access to said registry. The Internal Docker Registry is accessed from within the cluster.

When running a job, each exec node downloads the image to its local cache. Because of this, large clusters can generate a significant load on the Docker registry.

### Authorization { #docker_auth }

To specify the secret to access a private Docker registry, use the `docker_auth` key in the `secure_vault` option:
`secure_vault = { docker_auth = { username = "..."; password = "..."; auth = "..."; }; }`.

## Porto layers { #porto_layers }

This section contains information about Porto layers.

### Terminology

- **Porto layer**: a fragment of a root file system image for [Porto](https://github.com/ten-nancy/porto).
- {% if lang == "ru" %}**[RootFS image](http://wiki.rosalab.ru/ru/index.php/Образ_rootfs)**{% else %}**RootFS image**{% endif %}: a root file system image.
- **Basic layer** **(basic image)**: an autonomous layer that contains the core components of the environment (/bin/bash, libc6, etc.). By selecting a basic layer, you're confirming the Ubuntu version (Precise, Trusty, Xenial, Bionic, etc.) for running your code.
- **Delta layer**: a layer that supplements the basic layer and usually contains additional libraries or binary artifacts.

# Using file system images to run operations in {{product-name}} { #job_rootfs }

A MapReduce operation must be run with the file system image specified if its execution requires libraries or resources that aren't available in {{product-name}} clusters and if there is no way to prepare a binary file with this operation.

To specify a Docker image, use the `docker_image = "[REGISTRY/]IMAGE:TAG"` parameter in the operation specifications.

To use Porto layers, go to the mapper or reducer section in the operation specifications and specify the following: `layer_paths = ["//path/to/upper_layer"; ... ; "//path/to/lower_layer"]`. Layers are listed from top to bottom.
