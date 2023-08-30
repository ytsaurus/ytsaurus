# Files

This section contains information on {{product-name}} system objects designed to store large amounts of binary data. You can also store binary data in [tables](../../../user-guide/storage/blobtables.md).

## General information { #common }

A file is a [Cypress](../../../user-guide/storage/cypress.md) node of type **file** designed to store large binary data in the system.

You can store binary data both in files and in tables.

Sometimes, jobs need to save large binary data. The obvious solution is to write the file from each job to Cypress. However, this method has some drawbacks:

- It creates high proxy server load.
- A large number of objects appears in Cypress which makes working with them less efficient.

There is another solution that we propose: writing binary data to a single table.

## Usage { #usage }

Files support appends: `write_file` command. This performs a write by adding [chunks](../../../user-guide/storage/chunks.md); therefore, we recommend against performing many small writes.

We use the `read_file` command to read files. You can either read an entire file or specify a read range in the form of an offset in bytes. To do this, use the `offset` selector in the [YPath](../../../user-guide/storage/ypath.md) language.
Each read requires a request for metadata from the master server, so multiple reads are not recommended.

### Using in jobs { #usage_in_jobs }

One way to use files is to deliver the same data, such as lookups, for jobs. This causes file chunks to be downloaded into the local cache of every node requesting the file.

If a file consists of 2 or more chunks, or has `compression_codec` set, the cache rebuilds the original file from the chunks. This file is referred to as an artifact. There are two ways to deliver an artifact into a file in a job `sandbox`, which is a special directory that every job creates:

- By default, a [link](../../../user-guide/storage/links.md) to the artifact will be created in the job `sandbox`. This behavior is preferred since it works faster and creates less hard drive IO load.

- If you include the `copy_file` option in the specification, the file will be copied into the `sandbox`. It does not make sense to use this behavior unless you need to mount the entire `sandbox` in [tmpfs](https://en.wikipedia.org/wiki/Tmpfs).

If a file's `executable` attribute is set to `true`, it will be executable in the job `sandbox`. By default, the attribute is set to `false`.

You can use the `file_name` attribute to manage the name of a file or link created in the `sandbox`. If this attribute is absent, the Cypress filename is used.

## System attributes { #attributes }

Any file has the attributes shown in the table below.
The number of bytes in a file is written in the `uncompressed_data_size` attribute that every chunk owner has.

| **Attribute** | **Type** | **Description** | **Mandatory** |
| ------------ | --------- | -------------------------------------------------- |------------------|
| `executable` | `bool` | Whether the file is executable. | No |
| `file_name` | `string` | Filename when a file is moved into a job `sandbox`. | No |

All the files are chunk owners, so they get the appropriate [attributes](../../../user-guide/storage/chunks.md#attributes).



