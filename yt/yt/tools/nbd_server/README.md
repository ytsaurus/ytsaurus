# nbd_server

A single NBD server that serves **multiple devices**, each of an independently
chosen backend type. Each device is described with a polymorphic YSON struct — a
`type` discriminator selects the concrete backend config.

## Supported device backends

| `type`          | Backing store             | Mode | Needs a client |
|-----------------|---------------------------|------|----------------|
| `memory`        | process memory            | RW   | no             |
| `dynamic_table` | a mounted dynamic table   | RW   | yes            |
| `file`          | a Cypress file (its bytes)| RO   | yes (native)   |
| `chunk`         | a data node chunk         | RW   | yes (native)   |

The `file` backend serves the raw bytes of a Cypress file read-only — point it at
a filesystem image and mount the device directly, no `mkfs`. It reads the file's
chunks, so it needs a native client, and requires the file's `@filesystem`
attribute to be `ext4` or `squashfs`.

The `chunk` backend creates a read-write chunk on a data node via the NBD service.
It connects to the data node at `data_node_nbd_service_address` using the cluster
client's authenticated channel factory (which injects TVM service tickets), so
it requires `YT_PROXY` and `native_authentication_manager` in the config. It
creates a chunk-backed block device with an optional write-back page cache. The
data node must have the NBD service enabled.

The chunk's medium is set with `medium` (a medium name, e.g. `"default"` or
`"ssd_nbd"`) — resolved to an index via the cluster's medium directory. The
numeric `medium_index` is still accepted as a fallback when no cluster client is
available, but `medium` is preferred.

## Client

Only device backends that talk to a cluster need a client. Set `YT_PROXY` and the
server bootstraps one: it connects over RPC (all a proxy url needs), reads the
cluster's own `//sys/@cluster_connection`, and builds a full native client from
it. Leave `YT_PROXY` unset for a memory-only server.

The native client authenticates via TVM, so the config carries a
`native_authentication_manager` section and needs, in the environment:

- `YT_TOKEN` / `YT_USER` — user credentials for the RPC bootstrap and requests.
- `TVM_CLIENT_SECRET` — the TVM app secret (referenced by `client_self_secret_env`).

## Config

Example configs live in [configs/](configs/):

- [configs/memory.yson](configs/memory.yson) — memory-only, no cluster; runs out of the box.
- [configs/dynamic_table.yson](configs/dynamic_table.yson) — a dynamic-table device; needs a cluster + TVM.
- [configs/file.yson](configs/file.yson) — a read-only device over a Cypress file; needs a cluster + TVM.
- [configs/chunk.yson](configs/chunk.yson) — a read-write chunk device on a data node; needs a cluster + TVM.

For the `dynamic_table` device, first create and mount the table:

```bash
yt create table //tmp/nbd_devices --attributes '{dynamic=%true;schema=[{name=device_id;type=int64;sort_order=ascending};{name=block_id;type=int64;sort_order=ascending};{name=block_payload;type=string}]}'
yt set //tmp/nbd_devices/@primary_medium ssd_blobs
yt mount-table //tmp/nbd_devices --sync
```

For the `file` device, upload a filesystem image to a Cypress file and mark its
`@filesystem`. To build an **ext4** image:

```bash
dd if=/dev/zero of=/tmp/image.ext4 bs=1M count=1024
mkfs -t ext4 /tmp/image.ext4
mkdir ~/mnt && sudo mount /tmp/image.ext4 ~/mnt
# ... fill ~/mnt ...
sudo umount ~/mnt
yt write-file //tmp/nbd_image < /tmp/image.ext4
yt set //tmp/nbd_image/@filesystem ext4
```

or a **squashfs** image:

```bash
sudo apt install squashfs-tools
mksquashfs ~/mnt /tmp/image.squashfs
yt write-file //tmp/nbd_image < /tmp/image.squashfs
yt set //tmp/nbd_image/@filesystem squashfs
```

## Tuning the cluster connection

The server bootstraps the native client from `//sys/@cluster_connection`, which
leaves the client block cache at zero capacity. Use the optional `connection_patch`
config field to patch the (dynamic part of the) fetched connection — e.g. to size
the block cache so repeated reads of a file image are cached:

```
connection_patch = {
    block_cache = { compressed_data = { capacity = 536870912 } };
};
```

## HTTP API

Set `http_port` in the config to expose an HTTP server with a REST API for
managing devices at runtime, plus a status endpoint.

Devices are a REST resource under `/devices`, keyed by name:

| Method & path          | Action                          | Success | On conflict/absence     |
|------------------------|---------------------------------|---------|-------------------------|
| `GET /devices`         | list all devices (orchid)       | 200     |                         |
| `GET /devices/<name>`  | one device's orchid             | 200     | 404 if unknown          |
| `PUT /devices/<name>`  | add a device; body is its config| 201     | 409 if it already exists|
| `DELETE /devices/<name>`| remove a device                | 204     | 404 if unknown          |

`GET /status` dumps the full server orchid (`server_id`, `address`, the
`connections` submap and the `devices` submap); subpaths navigate into it, e.g.
`GET /status/devices/<name>`.

Both request and response bodies honor content negotiation, defaulting to YSON:
a request body is parsed as JSON when its `Content-Type` mentions `json`, and the
response is JSON when `Accept` mentions `json` (otherwise text YSON).

```bash
# Add a memory device from a JSON config.
curl -X PUT http://localhost:9000/devices/ram_disk \
    -H 'Content-Type: application/json' \
    -d '{"type":"memory","size":1073741824}'

# Inspect it (as YSON).
curl http://localhost:9000/devices/ram_disk

# Inspect the whole server (as JSON).
curl -H 'Accept: application/json' http://localhost:9000/status

# Remove it.
curl -X DELETE http://localhost:9000/devices/ram_disk
```

## Build and run

```bash
ya make yt/yt/tools/nbd_server -r

# Memory device (no cluster):
./yt/yt/tools/nbd_server/nbd_server --config .../configs/memory.yson

# Cluster-backed device (dynamic_table / file / chunk) — cluster via YT_PROXY, TVM secret from a file:
YT_PROXY=hume YT_USER="$USER" YT_TOKEN="$(cat ~/.yt/token)" TVM_CLIENT_SECRET="$(cat ~/.tvm/client_secret)" \
    ./yt/yt/tools/nbd_server/nbd_server --config .../configs/dynamic_table.yson
```

## Use a device

```bash
sudo apt-get install nbd-client
# List devices exported by the server.
sudo nbd-client localhost 10809 -l
# Connect one to /dev/nbd0 (device name is the key from `devices`).
sudo nbd-client localhost 10809 -N ram_disk /dev/nbd0
sudo mkfs -t ext4 /dev/nbd0
mkdir ~/mnt && sudo mount /dev/nbd0 ~/mnt
# ... use ~/mnt ...
sudo umount ~/mnt
sudo nbd-client -d /dev/nbd0
```
