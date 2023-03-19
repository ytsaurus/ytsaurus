# Media

This section describes media types and provides example data storage settings for various media.

## General information

Different media types (HDD, SDD, RAM) are logically combined in special entities referred to as media.
Media types differ by the amount of available space and read and write performance. The higher the performance of a medium, the smaller the medium is.

Each medium has a [chunk locator](../../../user-guide/storage/chunks.md). For a disk, the chunk locator is a file system partition. Media divide the entire set of chunk locations into non-intersecting subsets. At the same time, a single cluster node may have locations on different media. Each such subset does its own chunk balancing and handles its own quota. An account's disk quota is issued for a specific medium. A user may select the medium to use to store different types of data.

{{product-name}} clusters host the following media types:

- default: set of HDDs in a cluster.
- ssd_blobs: set of SATA SSDs in a cluster.
- ssd_journals: set of SATA SSDs or NVMe SSDs for storing dynamic table logs.

Retrieving a list of a cluster's media:

CLI
```bash
yt list //sys/media
```

## Assigning a medium

To configure the media store chunk owner (table, file, or log) chunks, set a value for the `media` attribute.
The `media` attribute value is a dictionary where medium name is a key for every entry, and the value is a dictionary of two entries:

- Key: `"replication_factor"`; value: replication factor for the specific medium.
- Key: `"data_parts_only"`; value: a flag. On media with `"data_parts_only" = %true`, [erasure coded](../../../user-guide/storage/replication.md#erasure) object only store data parts and do not store erasure parts. For example:

CLI
```bash
yt get //path/to/my/table/@media
```
```
{
  "default" = {
    "replication_factor" = 3;
    "data_parts_only" = %false;
  };
  "ssd_blobs" = {
    "replication_factor" = 1;
    "data_parts_only" = %true;
  };
}
```

To add replica tables to another medium, set the `media` attribute:

CLI
```bash
yt set //path/to/my/table/@media '
{
  "default" = {
    "replication_factor" = 3;
    "data_parts_only" = %false;
  };
  "ssd_blobs" = {
    "replication_factor" = 1;
    "data_parts_only" = %true;
   };
}'
```

[Cypress](../../../user-guide/storage/cypress.md) enables you to read individual records from a dictionary without reading the entire dictionary.
To retrieve the `default` medium replication factor, for instance, run the command below:

CLI
```bash
yt get //path/to/my/table/@media/default/replication_factor
```
```
3
```

To remove a table from a medium, delete the corresponding entry in the `media` attribute:

CLI
```bash
yt remove //path/to/my/table/@media/default
```

### Primary medium { #primary }

Each chunk owner has a special medium specified that is considered primary. It is to this medium that chunks belonging to the owner in question will be written. Chunks find their way to other media through replication and balancing.

To find out a table's primary medium, read its `primary_medium` attribute. You can change a primary medium by overwriting the attribute. The object will write its new chunks to the new primary medium while the actual movement of the old chunks occurs in the background and may take some time.

By default, the `"default"` medium is specified as primary on create.

The `replication_factor` attribute is a synonym for `@media/<primary_medium_name>/replication_factor`. The attribute has been kept for backward compatibility and ease of access.

### Limitations

Editing a table's `media` attribute is a potentially dangerous action. If you specify a medium that is out of space, for example, a table will become locked for writing. Editing `media` results in large amounts of data being moved, which may impact performance.


Therefore, a number of limitations exist.

- There must be at least one medium that holds all the data, that is the `replication_factor > 0 && !data_parts_only` condition must be satisfied.
- A medium with `data_parts_only == true` cannot be primary.
- An attempt to make primary a medium not being used by a table is considered a request to move the table from an old primary medium to a new one.

## Transient media

RAM is a different type of medium from HDD and SDD in that storing data in RAM is unreliable. RAM is power-dependent and will lose all data in the event of an emergency loss of power.

The {{product-name}} system refers to such media as **transient**. To notify the system that a medium is transient, you need to set its `transient` attribute equal to `%true`.

CLI
```bash
yt set //sys/media/ram/@transient "%true"
```

This property being set does not affect replication or balancing. The system does not prevent chunk storage on transient media only. This may be required to hold intermediate computation output in memory for a time.

Chunks replicated to transient media only are referred to as **unsteady**. You can retrieve their count as follows:

CLI
```bash
yt get //sys/precarious_chunks 42
```
```
yt get //sys/precarious_vital_chunks 0
```

## Attributes

A medium object has the following attributes:

| **Attribute** | **Type** | **Description** |
| ----------- | --------- | ------------------------------------------------------------ |
| `name` | `string` | The medium name is a non-empty unique string. |
| `index` | `integer` | The index is a small integer that serves as a unique identifier for housekeeping purposes. |
| `transient` | `bool` | Medium transient nature. |