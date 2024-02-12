# How to prepare, build and run dynamic_table_nbd_server

## Create dynamic table that will be storing NBD devices

```bash
user@host:~$ yt create table //path/nbd_devices --attributes '{dynamic=%true;schema=[{name=device_id;type=int64;sort_order=ascending}; {name=block_id;type=int64;sort_order=ascending}; {name=block_payload;type=string;}]}'
```

where `//path/nbd_devices` is the path to the dynamic table on cluster. It's important to use schema exactly like in the example above.

## Set SSD primary medium for dynamic table

```bash
user@host:~$ yt set //path/nbd_devices/@primary_medium "ssd_blobs"
```

## Mount dynamic table on cluster

```bash
user@host:~$ yt mount-table //path/nbd_devices --sync
```

where `//path/nbd_devices` is the path to the table on cluster.

## Add cluster connection to config

Take cluster connection section from the `//sys/@cluster_connection` of the corresponding cluster and add it to config. Use existing example configs as reference.

## Add NBD device to config

Add configuration for your NBD device to config. Here is an example configuration of NBD device:

```
"dynamic_table_block_devices" = {
    "device_id" = {
        "size" = 4294967296;
        "block_size" = 4096;
        "read_batch_size" = 16;
        "write_batch_size" = 16;
        "table_path" = "//path/nbd_devices";
    };
};
```

where `//path/nbd_devices` is a dynamic table that stores NBD devices. Once set, the `size`, `block_size` fields can not be modified later on.

## Build dynamic_table_nbd_server

```bash
user@host:~$ ya make yt/yt/experiments/public/dynamic_table_nbd_server -r
```

## Run dynamic_table_nbd_server

```bash
user@host:~$ ./yt/yt/experiments/public/dynamic_table_nbd_server/dynamic_table_nbd_server --config /yt/yt/experiments/public/dynamic_table_nbd_server/freud.yson 2>/tmp/nbd_stderr.txt &
```

Use path to your config in the example command

## Install nbd-client from package

```bash
user@host:~$ sudo apt-get install nbd-client
```

## Connect /dev/nbdX to your NBD device

```bash
user@host:~$ sudo nbd-client -u /tmp/nbd.sock -N device_id /dev/nbd0
```

where `device_id` is the ID of NBD device previously added to config.

## Create filesystem on connected NBD device

```
user@host:~$ sudo mkfs -t ext /dev/nbd0
mke2fs 1.46.5 (30-Dec-2021)
Creating filesystem with 1048576 4k blocks and 262144 inodes
Filesystem UUID: 985f2f22-69be-4704-8d3f-6716d0d52949
Superblock backups stored on blocks:
	32768, 98304, 163840, 229376, 294912, 819200, 884736

Allocating group tables: done
Writing inode tables: done
Creating journal (16384 blocks): done
Writing superblocks and filesystem accounting information: done 

user@host:~$
```

## Mount NBD device

```bash
user@host:~$ mkdir ~/mnt
user@host:~$ sudo mount -t ext4 -o /dev/nbd0 ~/mnt
```

## Use it

```bash
user@host:~$ ls ~/mnt
user@host:~$ # ...
```

## Unmount NBD device

```bash
user@host:~$ sudo umount ~/mnt
```

## Disconnect /dev/nbdX from your NBD device

```bash
user@host:~$ sudo nbd-client -d /dev/nbd0
```

## Shutdown dynamic_table_nbd_server

```bash
user@host:~$ killall dynamic_table_nbd_server
```
