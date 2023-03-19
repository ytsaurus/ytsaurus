# File cache

This section provides information on the file cache.

## General information { #common }

The file cache enables you to upload a file to [Cypress](../../../user-guide/storage/cypress.md) once and then obtain a path to the file multiple times via a specified [MD5 hash](https://ru.wikipedia.org/wiki/MD5).
The file cache is required to run operations. When users run tasks, this uploads an executable and all its dependencies.
Using Python, for instance, requires loading a large number of modules and dynamic link libraries. To avoid loading identical files multiple times, you only need to read them into the cache and request the path to these files in Cypress using an MD5 hash for any subsequent runs.

## Computing an MD5 hash for uploaded files { #compute_md5 }

To request an MD5 hash computation for an uploaded file, send `compute_md5=True` to the `write_file` command.

There are some constraints:

1. When using `append`, you cannot request an MD5 computation unless there was a previous write to the file with an MD5 computation.
2. A computed MD5 hash is reset if there is a call to the `concatenate` command and its output is written to the relevant file.

## API commands

For working with the file cache, there are the `put_file_to_cache` and the `get_file_from_cache` commands.
