# How to prepare, build and run cypress_nbd_server

## Create and upload filesystem image to cypress

To create an ext4 filesystem image, perform the following steps:

```bash
user@host:~$ dd if=/dev/zero of=/tmp/1gb_ext4.img bs=1024 count=1048576
user@host:~$ mkfs -t ext4 /tmp/1gb_ext4.img
user@host:~$ mkdir ~/mnt
user@host:~$ sudo mount -t ext4 /tmp/1gb_ext4.img ~/mnt
user@host:~$ # fill in ~/mnt directory
user@host:~$ # ...
user@host:~$ sudo umount ~/mnt
user@host:~$ yt --proxy [cluster] create --type file --attributes '{replication_factor=10;primary_medium=ssd_blobs;account=sys;}' --path //tmp/1gb_ext4.img # optional optimization step
user@host:~$ yt --proxy [cluster] write-file //tmp/1gb_ext4.img < /tmp/1gb_ext4.img
user@host:~$ yt --proxy [cluster] set //tmp/1gb_ext4.img/@filesystem ext4
```

To create a squashfs filesystem image, perform the following steps:

```bash
user@host:~$ sudo apt install squashfs-tools
user@host:~$ mkdir ~/mnt
user@host:~$ # fill in ~/mnt directory
user@host:~$ # ...
user@host:~$ mksquashfs ~/mnt /tmp/1gb_squashfs.img
user@host:~$ yt --proxy [cluster] write-file //tmp/1gb_squashfs.img < /tmp/1gb_squashfs.img
user@host:~$ yt --proxy [cluster] set //tmp/1gb_squashfs.img/@filesystem squashfs
```

## Add filesystem image to cypress_nbd_server config

You should need a config consisting of (at least) the following two fields; take the first one from
`//sys/@cluster_connection` of the corresponding cluster:
```
cluster_connection
file_exports
```

Add filesystem image to ```file_exports``` section. For example the following lines

```
file_exports = {
    1gb_ext4 = {
        path = "//tmp/1gb_ext4.img";
    };
};
```

add ```//tmp/1gb_ext4.img``` file located in cypress as a ```1gb_ext4``` NBD export.

## Build cypress_nbd_server

```bash
user@host:~$ ya make yt/yt/tools/cypress_nbd_server/ -r
```

## Run cypress_nbd_server

```bash
user@host:~$ ./yt/yt/tools/cypress_nbd_server/cypress_nbd_server --config config.yson 2>/tmp/err.txt &
```

## Install nbd-client from package

```bash
user@host:~$ sudo apt-get install nbd-client
```

## Connect NBD device to cypress_nbd_server file export

```bash
user@host:~$ sudo nbd-client localhost -N 1gb_ext4 /dev/nbd0
```

## Mount NBD device in read only mode

```bash
user@host:~$ mkdir ~/mnt
user@host:~$ sudo mount -t ext4 -o ro /dev/nbd0 ~/mnt
```

## Use filesystem image

```bash
user@host:~$ ls ~/mnt
user@host:~$ # ...
```

## Unmount filesystem image

```bash
user@host:~$ sudo umount ~/mnt
```

## Free NBD device

```bash
user@host:~$ sudo nbd-client -d /dev/nbd0
```

## Shutdown cypress_nbd_server

```bash
user@host:~$ killall cypress_nbd_server
```
