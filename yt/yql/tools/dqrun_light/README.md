# dqrun_light - Utility for Local Debugging of Distributed SQL Engine

`dqrun_light` is a stripped-down variant of `dqrun` for local debugging of a distributed SQL engine. It allows you to run all components of a distributed engine in a single process for more convenient debugging.

Unlike `dqrun`, this tool only supports the `dq` and `yt` gateways and the `hybrid` mode. Support for `solomon`, `pq`, `generic`, `ydb`, `clickhouse`, `s3`, the database resolver, the token accessor and the HTTP gateway has been removed.

## Command-Line Options

- `-s`: if specified, SQL is used; if not specified, the query plan execution specified in s-expression is used.
- `-p <file>`: specify a file with an SQL query or a s-expression.
- `--gateways-cfg <file>`: specify a file with the engine configuration. (Example: [examples/gateways.conf](examples/gateways.conf))
- `--fs-cfg <file>`: specify a file with the file cache configuration. (Example: [examples/fs.conf](examples/fs.conf))
- `--bindings-file <file>`: specify a file with the data schema.
- `--dq-host <host>`: set the host to connect to the test utilities `service_node` and `worker_node`.
- `--dq-port <port>`: set the port to connect to the test utilities `service_node` and `worker_node`.
- `-E, --emulate-yt`: emulate YT tables locally.
- `-t <table@file>`: table mapping for emulated YT tables.
- `-C <cluster@service>`: cluster to service mapping.

## Example of Local Usage

```bash
dqrun_light -s -p query.sql --gateways-cfg examples/gateways.conf --fs-cfg examples/fs.conf -E -t yt.plato.Input@input.txt -C plato@yt
```

In this example, `dqrun_light` will use SQL from the file `query.sql`, engine configuration from the file [examples/gateways.conf](examples/gateways.conf), file cache configuration from the file [examples/fs.conf](examples/fs.conf), and will emulate the YT table `plato.Input` from the local file `input.txt`.

## Example of Usage as a Client to Test Utilities

```bash
dqrun_light --dq-host localhost --dq-port 8080 -s -p query.sql --gateways-cfg examples/gateways.conf --fs-cfg examples/fs.conf
```

In this example, `dqrun_light` will use SQL from the file `query.sql`, engine configuration from the file [examples/gateways.conf](examples/gateways.conf), file cache configuration from the file [examples/fs.conf](examples/fs.conf). Additionally, the utility will act as a client to the test utilities `service_node` and `worker_node`, using the specified host and port.

**Example of gateways.conf:**

```conf
Dq {
    DefaultSettings {
        Name: "HashJoinMode"
        Value: "grace"
    }

    DefaultSettings {
        Name: "UseOOBTransport"
        Value: "true"
    }

    DefaultSettings {
        Name: "UseWideChannels"
        Value: "true"
    }
}
```

In the `Dq` section, parameters for the distributed engine are specified. The complete list of parameters can be found [here](../../../../contrib/ydb/library/yql/providers/dq/common/yql_dq_settings.h).

To use the Solomon gateway, use the full `dqrun` tool instead — add a `Solomon` section to `gateways.conf` with the relevant cluster mapping.
