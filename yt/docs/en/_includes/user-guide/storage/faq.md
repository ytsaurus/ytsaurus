#### **Q: Can I put an arbitrary YSON structure in a table cell?**

**A:** Yes. You can use columns of type `any`.

------

#### **Q: Can I modify a table's schema?**

**A:** Yes. You can find examples and limits in the [Table schema](../../../user-guide/storage/static-schema.md#create) section.

------

#### **Q: How large are the files the system supports?**

**A:** The formal limit on file length is just a 64-bit integer. In actual fact, it is the amount of free space on the cluster's nodes. Since a file is broken down into chunks, it does not have to be stored on a single node. Therefore, it can be larger than the size of a single hard drive. On the other hand, files this large cannot be fed to jobs in their entirety, so any processing will be limited to using the read_file command and to reading by ranges.

------
#### **Q: How do I move a table from an HDD to an SSD?**

**A:** You can change the medium type for a table (same as for a file) by editing the table's primary medium via the `primary_medium` attribute. Before modifying this attribute, you need to unmount your table (and probably remount it when done to return things to the way they were). For example:

```bash
yt unmount-table --sync //home/path/to/table
yt set //home/path/to/table/@primary_medium ssd_blobs
yt mount-table --sync //home/path/to/table
```
Immediately after you set the attribute, new data will start writing to an SSD while old data will be moved in the background. For more information about controlling this process and tracking its progress, please see the section on [static tables](../../../user-guide/storage/static-tables.md#medium).

------
#### **Q: What do I do if table reads are slow?**

**A:** There is a [dedicated page](../../../user-guide/problems/slow-read.md) about this.


------
#### **Q: How do I reduce the number of chunks I am using in my quota?**

**A:** If these chunks are taken up by tables (which is the most common scenario), you need to run a Merge operation with `combine_chunks = %true`.
This will rebuild your table from larger chunks, thereby reducing the use of chunks within your quota.  You can use something like the command below to start the operation from the command line.
```
yt merge --src table --dst table --spec "{combine_chunks=true}"
```

There is also a way of monitoring chunk usage without running a separate operation. For more information, see Merging chunks automatically on operation exit todo.

In certain cases, files may use large numbers of chunks (for instance, when you are always appending small fragments to existing files).  At present, there are no ready-made recipes (similar to a merge operation) for combining file chunks. The simplest advice that we can give is to run the following combination: `yt read-file //path/to/file | yt write-file //path/to/file`.  This will make the entire data stream go through the client, of course.

------
#### **Q: I am getting the Format "YamredDsv" is disabled error. What should I do?**

**A:** It is true that the `YAMRED_DSV` format is no longer supported. Use a different [format](../../../user-guide/storage/formats.md#formaty-predstavleniya-tablichnyh-dannyh).
