#### **Q: Can I put an arbitrary YSON structure in a table cell?**

**A:** Yes. You can use columns of type `any`.

------

#### **Q: Can I modify a table's schema?**

**A:** Yes. You can find examples and limits in the [Table schema](../../user-guide/storage/static-schema.md#create) section.

------

#### **Q: How large are the files the system supports?**

**A:** The formal limit on file length is just a 64-bit integer. In actual fact, it is the amount of free space on the cluster's nodes. Since a file is broken down into chunks, it does not have to be stored on a single node. Therefore, it can be larger than the size of a single hard drive. A single job cannot process files this large; therefore, the relevant activity will boil down to using the read_file command and reading by ranges.

------
#### **Q: How do I move a table from an HDD to an SSD?**

**A:** You can change medium type for a table or a file by editing the table's primary medium via the `primary_medium` attribute. Before modifying this attribute, you need to unmount your table (and remount it when done to return things to the way they were). For example:

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

**A**: If these chunks are taken up by tables (which is the most common scenario), you need to run a [Merge](../../../user-guide/data-processing/operations/merge.md) with `combine_chunks = %true`.
This will rebuild your table from larger chunks, thereby reducing the use of chunks within your quota. You can use a command-line command to run the operation replacing `src table` and `dst table`:

```
yt merge --src table --dst table --spec "{combine_chunks=true}"
```

There is also a way of monitoring chunk usage without running a separate operation. For more information, see Merging chunks automatically on operation exit.

Also, files may use many chunks in certain situations. For instance, when small fragments are being continuously appended to existing files. At the moment, there no ready-made method similar to Merge to combine file chunks. You can run `yt read-file //path/to/file | yt write-file //path/to/file` in combination.  This will make the entire data stream go through the client.

------
#### **Q: I am getting the Format "YamredDsv" is disabled error. What should I do?**

**A:** The `YAMRED_DSV` format is no longer supported. Use a different [format](../../../user-guide/storage/formats.md#formaty-predstavleniya-tablichnyh-dannyh).
