# Job file environment: layers and files

This section describes how to work with the job file environment in {{product-name}}. You’ll learn what layers and individual files are, how to specify them in the operation specification, how to prepare and upload them to Cypress, and how to speed up layer reads and diagnose issues.

## What is the file environment { #file-environment }

A job’s file environment is a set of directories and files where the user process runs. The environment is represented as a root file system: a tree of directories and files, similar to any Unix system.

You need dependencies to run code: interpreters, libraries, binary files, configs, and data. If your code accesses a dependency—a directory or file that isn’t in the environment—an error occurs. That’s why you build the environment for a specific task.

The file environment consists of two parts:

- Root file system—built from layers listed in the [layer_paths](../../../../user-guide/data-processing/layers/layer-paths.md#job_rootfs) parameter. It’s suitable for a persistent environment that you carry over from operation to operation.
- Individual files—artifacts in the `/slot/sandbox` subtree, listed in the [file_paths](../../../../user-guide/data-processing/operations/operations-options.md#user_script_options) parameter. They’re suitable for one‑time transfers of specific files to a job, such as binaries or configs.

The execution environment builds the file environment—the software environment where the job runs. The execution environment provides access to the file system and runs your code in it. For details on the available environments and their differences, see the [Execution environments](#environments) section.

## Layers { #layers }

### What is a layer { #what-is-layer }

A layer is a file that contains a subset of the root file system. The subset is one or more directory subtrees with their nested structure preserved. Physically, a layer is stored as a single file that you upload to {{product-name}}.

The simplest example of a layer is an archive. You prepare the environment on your machine in advance, pack it into an archive, and upload it to {{product-name}}. During operation execution, the system expands the archive and forms the job’s file environment from it.

### How the environment is built from layers { #rootfs }

You can build the root file system from a single layer or from multiple layers. Layers from `layer_paths` are merged into a single root file system using [overlayfs](https://wiki.archlinux.org/title/Overlay_filesystem)—a Linux kernel mechanism that combines several file systems into one.

The root file system is built on the exec node during job preparation, before your code starts. Containerization—Porto or CRI—makes the assembled file system the root `/` of the container and runs your code in it. Without containerization, layers aren’t applied as a root file system.

#### How a layer fits into the tree { #rootfs-tree }

Each layer reproduces part of the tree, from the top level down. The top level of a layer is the folders and files that sit directly in it, not nested in other folders. After assembly, they’re placed directly in the root `/`.

Everything nested inside them ends up at lower levels—inside the corresponding root folders.

For example, if the archive contains the `usr`, `bin`, and `home` folders at the top level, they become `/usr`, `/bin`, and `/home` after expansion. Their nested contents become `/usr/lib`, `/bin/bash`, and so on.

A single layer can contain multiple subtrees at once. For example, one archive can include both `/usr` and `/var`.

#### Base and delta layers { #base-delta }

Most often, you build the environment using a “base layer + delta” scheme. It separates the unchanging foundation from your own files:

- Base layer—a layer with the main part of the environment: system utilities and libraries, for example `/bin/bash` and `libc6`. It’s usually a Linux distribution image, such as Ubuntu, with packages and libraries installed.
- Delta layer—an addition to the base layer with your own libraries or binary artifacts.

A common configuration is one base layer and one or more delta layers: you use a ready‑made image as the base and put your own files in the delta.

### Layer formats { #layer-formats }

{{product-name}} supports two layer formats: tar archive and SquashFS. For SquashFS, network access via [NBD](../../../../admin-guide/nbd.md#how-it-works) is also supported. The table below shows how the formats differ and when to use each:

#|
||**Format**|**What it is**|**When to use**||
||Tar archive|A classic archive with a subset of the file system|When performance isn’t critical and you want to build a layer as simply as possible||
||SquashFS|A file system image mounted without unpacking|When performance matters: it speeds up job preparation and reduces disk load||
||SquashFS over NBD|Reading a SquashFS image over the network, without downloading it to the exec node|When you need the fastest job start and don’t want to copy large images in full||
|#

#### Tar archive { #tar-layer }

A tar archive is a layer in the form of a `.tar`, `.tar.gz`, `.tar.xz`, or `.tar.zstd` archive with a subset of the file system.

The archive is convenient for several reasons:

- You can easily build it with the standard `tar` utility.
- You can easily inspect its contents: download the archive and unpack it.
- It works in any container environment.

During job preparation, the archive is downloaded to the exec node and unpacked to disk. Unpacking loads the CPU and Disk I/O, and the unpacked layer takes up disk space.

#### SquashFS { #squashfs-layer }

SquashFS is a file system image that’s mounted on the exec node without unpacking. The mount point is then used as a layer in overlayfs. Mounting is much faster than unpacking an archive.

Compared to a tar archive, a SquashFS layer offers these benefits:

- It speeds up job preparation—you don’t need to unpack the image.
- It reduces disk load—a valuable resource on clusters.
- It saves disk space—the image isn’t stored in an unpacked form.

#### SquashFS over NBD { #nbd-layer }

NBD (Network Block Device) is a way to use a SquashFS image over the network. Image data is read from data nodes as needed, without pre‑downloading it to the exec node.

A local SquashFS layer is first downloaded in full to the exec node. NBD reads only the blocks that the job accesses from the data nodes. This way, NBD avoids using the resources required by a local layer:

- CPU and Disk I/O to write the image to disk.
- Disk space to store the image.
- Network I/O to transfer image parts that the job doesn’t read.

Already‑read blocks are cached at multiple levels: in the kernel page cache, in the chunk reader caches, and in the data node caches. Subsequent accesses to the same data are served from the cache without new network reads. This can make NBD layers significantly faster than regular layers.

NBD works on top of a SquashFS image. This format compresses data and is read‑only, which matches the purpose of a layer: a layer doesn’t change while the job runs.

{% note warning %}

NBD works only in the Porto environment.

{% endnote %}

## Execution environments { #environments }

The set of available layer formats depends on the execution environment configured on the cluster in the `exec_node/slot_manager/job_environment/type` parameter:

#|
||**Environment**|**Description**|**Supported layer formats**||
||`simple`|No containerization|—||
||`cri`|Docker containers|[Docker images](../../../../user-guide/data-processing/layers/layer-paths.md#docker_images)||
||`porto`|Porto environment|Tar archives, SquashFS, SquashFS over NBD, Docker images||
|#

{% note info %}

SquashFS and NBD aren’t supported in the `simple` and `cri` environments.

{% endnote %}

## Individual files { #files }

### What is an individual file { #what-is-file }

An individual file is a Cypress artifact delivered directly to the job, without building a layer. Files are passed via the `file_paths` parameter and land in the `/slot/sandbox` subtree of the root file system. In effect, files supplement the root file system’s file environment.

Individual files are suitable for transferring configs, binaries, and models to a job. You place the file yourself and know which path to use to access it from your code.

### Placing files in /slot/sandbox { #sandbox }

Each file from `file_paths` lands in the `/slot/sandbox` subtree of the root file system. By default, the file is available to the job at `/slot/sandbox/<file name>`, where the file name matches the original name from Cypress.

To place a file elsewhere in the root file system—for example, in `/bin` or `/usr/lib`—you’ll need to add it via a layer. For more details, see the [Files vs. layers](#files-vs-layers) section.

### Files vs. layers { #files-vs-layers }

Files and layers solve different tasks:

- Files via `file_paths` are suitable for transferring non‑system configs, models, and binaries. You can’t use files to transfer, for example, shared C/C++ libraries or system configs: files land in the `/slot/sandbox` subtree and can’t be placed anywhere else in the root file system.
- Layers via `layer_paths` are needed to build a system environment. This includes everything required to run your program: system libraries, the dynamic linker, system configs, and applications. A layer can contain files in any directory of the root file system—`/usr`, `/bin`, `/lib`, and others.

If you need to place a file outside `/slot/sandbox`—for example, a library in `/usr/lib`—build a delta layer and specify it in `layer_paths`. A library often can’t reside in an arbitrary location: the system requires it to be at a specific path, or it won’t work.

## How to set the job’s file environment in the specification { #set-environment }

You set the job’s file environment in the operation specification using two parameters: `layer_paths` for root filesystem layers, and `file_paths` for individual files.

### The layer_paths parameter { #layer-paths }

You pass layers to the operation via the `layer_paths` parameter — a list of paths to layers in Cypress. Here’s a minimal example with a single-layer environment:

```python
import yt.wrapper as yt

spec = {
    "mapper": {
        "layer_paths": [
            "//path/to/my_layer",
        ]
    }
}

yt.run_map(mapper, source_table, destination_table, spec=spec)
```

#### Layer order { #layer-order }

List layers in `layer_paths` from top to bottom: delta layers first, the base layer last.

Layer order matters only when layers overlap in data — that is, they contain files or folders at the same path. When they overlap, the top layer takes priority: the root filesystem uses the file from the top layer.

That’s why files from the delta override those from the base layer, even if a file with the same path exists in both.

If there’s no overlap, the order doesn’t matter. For example, when one layer contains `/usr` and another contains `/var`.

```python
"layer_paths": [
    "//path/to/delta_layer",   # Top layer, delta
    "//path/to/base_layer",    # Bottom layer, base
]
```

If a file exists at the same path in both layers, the system uses the file from `delta_layer`. It takes not only the file’s contents from the top layer but also its metadata: access rights, owner and group, and timestamps.

#### Layer attributes { #layer-attributes }

The layer format and access method are defined by the file attributes in Cypress:

| Attribute | Values | Description |
| --- | --- | --- |
| `filesystem` | `archive`, `squashfs` | Layer format: `archive` is a tar archive, `squashfs` is a SquashFS filesystem image |
| `access_method` | `local`, `nbd` | Access method for the image |

For a SquashFS layer with NBD access, you can specify `access_method` directly in the layer path:

```python
spec = {
    "mapper": {
        "layer_paths": [
            "<access_method=nbd>//path/to/my_layer.squashfs",  # SquashFS via NBD
            "//path/to/porto_layer",  # Base tar layer
        ]
    }
}
```

The `access_method` attribute in the specification is available starting from version 25.1. It’s handy when you need to use one layer with different access methods or when you don’t have permission to change the file’s attributes.

#### Default layer { #default-layer }

If you don’t specify `layer_paths`, the system uses the default image configured on the cluster. Typically, this is a minimal base Ubuntu image.


### The file_paths parameter { #file-paths }

You pass files to the operation via the `file_paths` parameter — a list of paths to files in Cypress with attributes. Each file goes into the `/slot/sandbox` subtree.

#### File name conflicts { #file-name-conflicts }

If two files in `file_paths` have the same name and you don’t set the `file_name` attribute, the last file overwrites the previous one. Set a unique `file_name` for each file.

#### File attributes { #file-attributes }

You can set attributes for each file in `file_paths`:

| Attribute | Type | Description |
| --- | --- | --- |
| `file_name` | string | The file name in the `/slot/sandbox` subtree. By default, it’s the original file name from Cypress. Supports nested paths, for example `nv_tmpfs/sys/static/bundle.tar.gz` |
| `executable` | bool | Set the executable flag. Default is `false` |
| `bypass_artifact_cache` | bool | Don’t cache the file in the exec node’s artifact cache. Default is `false`. This can be useful, for example, when you need to copy the file directly to `tmpfs` |
| `copy_file` | bool | Copy the file instead of creating a hard link. Default is `false` |

Two attributes are key. The `$value` attribute specifies the path in Cypress where the file comes from. The `file_name` attribute specifies the path in the job’s file environment where to place the file.

Here’s an example specification with file attributes:

```python
spec = {
    "file_paths": [
        {
            "$value": "//path/to/nirvana-bundle.tar.gz",
            "$attributes": {
                "file_name": "nv_tmpfs/sys/static/nirvana-bundle.tar.gz",
                "executable": False,
                "bypass_artifact_cache": True,
            }
        },
        {
            "$value": "//path/to/job_launcher_native",
            "$attributes": {
                "file_name": "nv_tmpfs/sys/static/job_launcher_native",
                "executable": True,
                "bypass_artifact_cache": True,
            }
        },
    ],
    "mapper": {
        "layer_paths": [
            "//path/to/porto_layer",
        ]
    }
}
```

In this example, `nirvana-bundle.tar.gz` is available at `/slot/sandbox/nv_tmpfs/sys/static/nirvana-bundle.tar.gz`. The `job_launcher_native` file is available at `/slot/sandbox/nv_tmpfs/sys/static/job_launcher_native` with execute permission.

#### Default file_paths { #file-paths-default }

If you don’t specify `file_paths`, the `/slot/sandbox` subtree remains without user files.

### Environment preparation order { #preparation-order }

The job environment is built in two steps:

1. **Prepare the root filesystem.** The system downloads layers from `layer_paths`, mounts them using overlayfs, and builds the root filesystem.
2. **Deliver files.** The system downloads files from `file_paths` and places them in the `/slot/sandbox` subtree of the root filesystem.

Your custom code runs only after both steps complete.

### Specification examples { #spec-examples }

You can combine layers and files in one specification. For example, use a base tar layer in `layer_paths` and additional artifacts in `file_paths`:

```python
spec = {
    "file_paths": [
        {
            "$value": "//path/to/executor-config.yaml",
            "$attributes": {
                "file_name": "executor-config.yaml",
                "bypass_artifact_cache": True,
            }
        },
        {
            "$value": "//path/to/my_binary",
            "$attributes": {
                "file_name": "my_binary",
                "executable": True,
            }
        },
    ],
    "mapper": {
        "layer_paths": [
            "//path/to/porto_layer",
        ]
    }
}
```

The files are available at `/slot/sandbox/executor-config.yaml` and `/slot/sandbox/my_binary`.

## Prepare and upload layers and files to Cypress { #prepare-upload }

### Prepare layers { #prepare-layers }

#### Create a tar archive { #create-tar }

You build the archive, for example, by using the `tar` command:

```bash
tar czf delta_layer.tar.gz configs data bins
```

#### Create a SquashFS image { #create-squashfs }

There is no separate “NBD image”: the same SquashFS image is used for NBD access. The layer format and access method are determined not by the file extension, but by the attributes when you upload to Cypress. For more details, see the section [Upload layers to Cypress](#upload-layers).

Build the image from a folder by using `mksquashfs`:

```bash
sudo apt install squashfs-tools
mkdir ~/mnt
# Populate ~/mnt with the required content
mksquashfs ~/mnt /tmp/my_layer.squashfs
```

If you already have a tar layer, you can convert it to SquashFS by using `tar2sqfs`:

```bash
sudo apt install squashfs-tools-ng
tar2sqfs ~/dict.squashfs < ~/dict.tar.gz
```

When you build the image, you can set the SquashFS block size — a key parameter for performance when accessing via NBD. For more details, see the section [Optimize NBD layers](#nbd-optimization).

{% note warning %}

When you convert tar layers to SquashFS, make sure there are no errors transferring the extended attributes `trusted.overlay.*`. Such errors can cause overlayfs to work incorrectly.

{% endnote %}

### Upload layers to Cypress { #upload-layers }

For both tar layers and SquashFS images, you can set the storage medium `ssd_blobs` and the number of replicas — this speeds up layer downloading to exec nodes. For more details, see the section [How to speed up layer reads](#performance).

#### Upload a tar layer { #upload-tar }

```bash
yt write-file //path/to/layer.tar.gz < ~/layer.tar.gz
```

If the filesystem format is not specified, the layer is treated as a tar archive.

#### Upload a SquashFS image { #upload-squashfs }

When you upload a SquashFS image, specify the filesystem format with the `@filesystem` attribute:

```bash
yt write-file //path/to/layer.squashfs < ~/layer.squashfs
yt set //path/to/layer.squashfs/@filesystem squashfs
yt set //path/to/layer.squashfs/@access_method local
```

{% note info %}

If the file has the `.squashfs` extension, the `@filesystem` attribute is determined automatically.

{% endnote %}

To use a SquashFS image over the network, set the `@access_method` attribute to `nbd`:

```bash
yt write-file //home/user/layers/base.squashfs < ~/base.squashfs
yt set //home/user/layers/base.squashfs/@filesystem squashfs
yt set //home/user/layers/base.squashfs/@access_method nbd
```

After that, you can use the layer in operations:

```python
map_spec = {"mapper": {"layer_paths": ["//home/user/layers/base.squashfs"]}}

yt.wrapper.run_map(
    Mapper(),
    source_table=args.input_table,
    destination_table=args.output_table,
    spec=map_spec,
)
```

Access methods:

- `access_method=local` — the image is downloaded to the exec node and mounted locally.
- `access_method=nbd` — the image is read over the network via NBD.

At any given time, the image is used in only one mode: `local` or `nbd`.

### Prepare files { #prepare-files }

Before you upload files to Cypress, prepare them on your local machine:

- Collect the required files: scripts, binaries, configs, and models.
- Check file integrity: compare hashes or checksums if you downloaded the files from external sources.
- Check access rights: files that you plan to run must be executable.
- Organize files into a logical directory structure, for example `/scripts`, `/configs`, `/models`.

### Upload files to Cypress { #upload-files }

Files for `file_paths` are uploaded to Cypress in the same way as layers. You can also configure storage parameters.

#### Choose a medium { #file-medium }

You can increase the file download speed by using the appropriate medium and setting the `primary_medium` attribute. Store frequently used files on SSD, and rarely used files on HDD:

```bash
yt create --type file \
    --attributes '{primary_medium=ssd_blobs;account=sys;}' \
    --path //path/to/my_file
yt write-file //path/to/my_file < ~/my_file
```

For an existing file:

```bash
yt set //path/to/my_file/@primary_medium ssd_blobs
yt set //path/to/my_file/@account sys
```

#### Number of replicas { #file-replication }

You can increase the file download speed by using the number of replicas and setting the `replication_factor` attribute. The default is 3, and the maximum is 20:

```bash
yt create --type file \
    --attributes '{replication_factor=10;}' \
    --path //path/to/my_file
yt write-file //path/to/my_file < ~/my_file
```

For an existing file:

```bash
yt set //path/to/my_file/@replication_factor 10
```

#### Naming and organization recommendations { #file-naming }

To simplify file management in Cypress:

- Use folders based on purpose or version: `//home/<user>/layers/`, `//home/<user>/configs/`.
- Give files descriptive names so you can understand their content without opening them.
- When you update a file, create a new version in a separate folder instead of overwriting the existing one — this way, you can roll back if an error occurs.

#### Check availability { #file-check }

After uploading, check that the file is available:

```bash
yt list //path/to/
yt get //path/to/my_file/@size
```

## Practical scenarios { #scenarios }

### Base layer and files via file_paths { #scenario-base-files }

Use this scenario when you need to add a few artifacts — configs, binaries, or models — to the standard environment without creating a new layer.

```python
spec = {
    "file_paths": [
        {
            "$value": "//path/to/config.yaml",
            "$attributes": {
                "file_name": "config.yaml",
            }
        },
        {
            "$value": "//path/to/my_binary",
            "$attributes": {
                "file_name": "my_binary",
                "executable": True,
            }
        },
    ],
    "mapper": {
        "layer_paths": [
            "//path/to/porto_layer",
        ]
    }
}
```

The files will be available at the paths `/slot/sandbox/config.yaml` and `/slot/sandbox/my_binary`.

### Delta layer instead of multiple file_paths { #scenario-delta }

Use this scenario, for example, when you need to bring complex dependencies along with the files: `pip`, `apt`, `conda`, and shared libraries.

Build a delta layer from your files:

```bash
mkdir -p ~/delta/usr/lib ~/delta/usr/bin
cp ~/my_library.so ~/delta/usr/lib/
cp ~/my_binary ~/delta/usr/bin/
mksquashfs ~/delta /tmp/delta_layer.squashfs
yt write-file //path/to/delta_layer.squashfs < /tmp/delta_layer.squashfs
yt set //path/to/delta_layer.squashfs/@filesystem squashfs
yt set //path/to/delta_layer.squashfs/@access_method nbd
```

Use the delta layer in the operation together with the base layer:

```python
spec = {
    "mapper": {
        "layer_paths": [
            "<access_method=nbd>//path/to/delta_layer.squashfs",  # Delta layer
            "//path/to/porto_layer",  # Base layer
        ]
    }
}
```

Files in the delta layer override files in the base layer when the paths match.

## Optimization and diagnostics { #optimization-diagnostics }

### Job environment preparation stages { #preparation-stages }

Preparing a job’s environment involves several stages. Each stage takes time and consumes resources. You can view preparation metrics in the {{product-name}} web interface:

- On the operation page — go to the **Jobs** tab → check the **Statistics** column.
- On the job page — open the **Job statistics** section → look at the **Prepare** stage.

Key job environment preparation stages:

#|
||**Stage**|**What happens**|**How to optimize**||
||Downloading artifacts|Downloading layers and files from Cypress to the exec node|[Store on SSD](#ssd-storage), [increase the number of replicas](#replication-factor)||
||Preparing artifacts|Preparing downloaded artifacts: unpacking tar, checking integrity|Switch from tar to [SquashFS](#squashfs-layer)||
||Preparing root volume|Mounting layers and building the root filesystem via overlayfs|Switch to [SquashFS over NBD](#nbd-layer)||
||Preparing tmpfs volumes|Preparing tmpfs volumes for the job|Depends on the tmpfs size; configured at the cluster level||
|#

### Stage analysis { #bottleneck-analysis }

To find the bottleneck, compare the time spent on each stage and analyze outliers:

- Open the operation page and go to the **Jobs** tab.
- Find jobs with the longest preparation time — sort by the **Prepare time** column.
- Open the job and check the stage breakdown in the **Job statistics** section.
- Compare the stage times: the stage with the longest time is the bottleneck.
- Apply the optimization methods from the table above to the identified stage.

### Methods to optimize job environment preparation stages { #stage-optimization }

If the **Preparing root volume** stage takes the most time, switching from tar to SquashFS over NBD gives the biggest performance gain. If it’s **Downloading artifacts**, increase the number of replicas and verify that the layer is stored on SSD. If it’s **Preparing artifacts**, switch from tar to SquashFS to avoid unpacking.

### Troubleshooting { #troubleshooting }

#### RootVolumePreparationFailed { #root-volume-error }

This error occurs when mounting a layer fails during root filesystem preparation. Possible causes:

- The layer image is corrupted.
- The filesystem format is incorrect — `@filesystem`.
- The NBD server is unavailable or not configured on the cluster.

NBD‑specific causes are listed in the [Common NBD errors](#nbd-common-errors) section.

#### NbdError { #nbd-error }

This error occurs when reading from an NBD device fails during job execution, for example, due to a lost connection with the data node. The job is automatically aborted and restarted. If the errors keep happening, the operation ends with an error.

#### NBD volume creation error { #nbd-volume-error }

This error occurs when creating an NBD volume on the exec node fails. Possible causes:

- The NBD server isn’t running or isn’t reachable on the exec node.
- The limit for NBD devices on the exec node is exceeded.
- There aren’t enough resources to create the volume.

Contact your cluster administrator for diagnostics.

#### File permission error { #file-permission-error }

If a file ends up in `/slot/sandbox` but your user code can’t run it, check the `executable` attribute. By default, it’s set to `false`. Set `executable: true` for executable files.

#### File name conflict in /slot/sandbox { #file-name-conflict-error }

If two files in `file_paths` have the same name and the `file_name` attribute isn’t set, the last file overwrites the previous one. Set a unique `file_name` for each file.

#### Disk space shortage { #disk-space-error }

Unpacked tar layers take up space on the exec node disk. Switch to SquashFS: the image is mounted without unpacking and isn’t stored in an unpacked form.

#### Artifact download timeout { #download-timeout }

If the **Downloading artifacts** stage takes too long, increase `replication_factor` and verify that the layer is stored on SSD. More replicas let you read data in parallel from different data nodes.

#### Quota and resource issues { #quota-error }

If the job ends with a resource shortage error — CPU, memory, or disk — check the limits in the operation specification. Increase `cpu_limit`, `memory_limit`, or `disk_request` if needed. Also, check that unpacked tar layers aren’t taking up too much space: switch to SquashFS so you don’t store the image in an unpacked form.

## How to speed up layer reads { #performance }

### Store layers on SSD { #ssd-storage }

Reading from SSD is much faster than from HDD. This is especially important for NBD: data is read interactively from data nodes as the job accesses it, and HDD drives are too slow for this.

```bash
# When creating a file
yt create --type file \
    --attributes '{primary_medium=ssd_blobs;account=sys;}' \
    --path //path/to/layer.squashfs
yt write-file //path/to/layer.squashfs < ~/layer.squashfs

# For an existing file
yt set //path/to/layer.squashfs/@primary_medium ssd_blobs
yt set //path/to/layer.squashfs/@account sys
```

The migration to SSD happens in the background and may take time.

### Increase the number of replicas { #replication-factor }

For NBD, three replicas can become a bottleneck when many jobs use the same layer at the same time. More replicas let you read data in parallel from different data nodes. You pay for more replicas with extra storage space.

```bash
# When creating a file
yt create --type file \
    --attributes '{replication_factor=20;}' \
    --path //path/to/layer.squashfs
yt write-file //path/to/layer.squashfs < ~/layer.squashfs

# For an existing file
yt set //path/to/layer.squashfs/@replication_factor 20
```

By default, `replication_factor=3`; the maximum is 20.

### Use the chunk cache { #chunk-cache }

For tar and local SquashFS layers with `access_method=local`, the exec node caches downloaded layers in the chunk cache. When you rerun jobs with the same layers, they’re taken from the cache without downloading from data nodes.

Two factors affect cache efficiency:

- Standard base layers are highly likely to be already cached on exec nodes.
- Changing the layer file invalidates the cache — the cached copy stops being used.

## NBD layers { #nbd }

### Optimizing NBD layers { #nbd-optimization }

#### SquashFS block size { #squashfs-block-size }

The SquashFS filesystem works with blocks. By default, the block size is 128 KB. The larger the block, the more data the kernel reads at once and the fewer NBD requests are sent to data nodes.

Set the block size when building the image:

```bash
mksquashfs ~/mnt /tmp/my_layer.squashfs -b 1M
```

For example, for self‑driving systems, switching from a 128 KB block to a 1 MB block sped up `md5sum` calculation for a large binary from 250–350 seconds to 40 seconds.

{% note info %}

A block that’s too large increases the read volume when accessing small files: you have to read “almost empty” blocks. Also, in the current Linux implementation of SquashFS, increasing the block size raises RAM consumption when mounting the image. The optimal size depends on the file access pattern in the layer.

{% endnote %}

#### Chunk block size { #chunk-block-size }

Don’t confuse the SquashFS block size with the chunk block size. A file in Cypress consists of chunks, and a chunk is read from data nodes in blocks. By default, the chunk block size is 16 MB.

The kernel reads data from NBD devices in small chunks, usually no more than 8 KB. So, with NBD access, a smaller chunk block size — `512K`, `1M`, or `2M` — reduces unnecessary reads.

Set the block size when uploading the file:

```bash
yt write-file \
    --file-writer '{block_size=2097152;}' \
    //path/to/layer.squashfs < ~/layer.squashfs
```

For example, for tasklets, reducing the chunk block size from 16 MB to 2 MB noticeably lowered the read volume.

{% note warning %}

`block_size` is set once when writing the file. To change it, rewrite the file.

{% endnote %}

### Monitoring and diagnosing NBD { #nbd-monitoring }

#### Solomon sensors { #sensors }

The following NBD metrics are available on exec nodes.

Server metrics:

#|
||**Metric**|**Description**||
||`nbd/server/count`|Current number of NBD servers||
||`nbd/server/created`|Number of NBD servers created||
|#

Device metrics. The `file_path` tag is the path to the layer file:

#|
||**Metric**|**Description**||
||`nbd/device/count`|Current number of NBD devices||
||`nbd/device/created` / `nbd/device/removed`|Devices created and removed||
||`nbd/device/read_count`|Number of read requests||
||`nbd/device/read_bytes`|Bytes read||
||`nbd/device/read_time`|Read time, histogram||
||`nbd/device/read_block_bytes_from_cache`|Bytes read from the block cache||
||`nbd/device/read_block_bytes_from_disk`|Bytes read from the data node disk||
|#

Volume metrics. The tag is `type=nbd` or `type=squashfs`:

#|
||**Metric**|**Description**||
||`volumes/count`|Current number of volumes||
||`volumes/created`|Volumes created||
||`volumes/create_errors`|Volume creation errors||
|#

Volume cache metrics:

#|
||**Metric**|**Description**||
||`exec_node/ronbd_volume_cache/missed_count`|Misses in the RO NBD volume cache||
||`exec_node/ronbd_volume_cache/hit_count`|Hits in the cache. Tag `hit_type=sync\|async`||
||`exec_node/squashfs_volume_cache/missed_count`|Misses in the SquashFS volume cache||
||`exec_node/squashfs_volume_cache/hit_count`|Hits in the SquashFS volume cache||
|#

### Common NBD errors { #nbd-common-errors }

Below are errors specific to NBD. General job environment preparation errors `RootVolumePreparationFailed` and `NbdError` are described in the [Troubleshooting](#troubleshooting) section.

#### NBD server is not present { #nbd-server-missing }

This error occurs when you try to use NBD on a cluster where it isn’t enabled, or in an environment other than the porto environment. Contact your cluster administrator to enable NBD.

#### Incorrect layer attributes { #nbd-wrong-attributes }

To access a layer via NBD, it must have the `@filesystem=squashfs` and `@access_method=nbd` attributes set. If the filesystem format is specified incorrectly, mounting the layer will fail with the `RootVolumePreparationFailed` error.

<style>
.dc-mini-toc__section_child {
    display: none;
}

@media screen and (max-width: 768px) {
    .dc-doc-page__content-mini-toc ul li ul {
        display: none;
    }
}
</style>
