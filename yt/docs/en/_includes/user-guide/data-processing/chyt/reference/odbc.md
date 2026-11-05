# Connecting external databases over ODBC

CHYT can act as an ODBC client and query an external database from SQL through the [ClickHouse ODBC bridge](https://clickhouse.com/docs/concepts/features/interfaces/odbc). This works with MySQL, PostgreSQL, MariaDB, and other databases that have a Linux ODBC driver.

{% if audience == "internal" %}

To request another pre-deployed driver, file a ticket in the CHYT support queue.

{% endif %}

## How it works { #how-it-works }

When a clique starts, the controller combines the drivers from `drivers_dir` with `odbc_config` and places the bridge binary, driver libraries, and generated unixODBC configuration in every instance's job sandbox. Each instance runs its own local bridge; before startup, the trampoline expands environment variables and configures the ODBC and library paths.

Drivers and data sources are fixed at clique startup. Configuration changes take effect after the clique restarts.

## Configuring a clique { #speclet }

ODBC is configured through `odbc_config` in the clique [speclet](../../../../../user-guide/data-processing/chyt/cliques/configs.md#speclet). Changing it requires the `manage` permission for the clique.

#|
|| **Option** | **Description** ||

|| `enable` [`false`] | Enables ODBC support ||

|| `drivers_dir` [`//sys/bin/odbc-drivers`] | Cypress directory containing pre-deployed drivers and `config.yson` with their descriptions ||

|| `drivers` | Additional drivers. Each entry has `name`, `description`, `path` to the driver `.so` file, and optional `options` for its `odbcinst.ini` section ||

|| `sources` | Data sources used to generate `odbc.ini`. Each entry has `name`, `description`, `driver`, and driver-specific connection `options` ||

|| `extra_files` | Additional driver files, such as shared libraries or certificates, copied from Cypress to the job sandbox ||

|| `bridge_path` [`//sys/bin/odbc-drivers/clickhouse-odbc-bridge`] | Cypress path to the ODBC bridge binary ||

|#

Most cliques only need `enable` and `sources`. The `drivers_dir`, `drivers`, `extra_files`, and `bridge_path` options are intended for administrators and custom driver setups; keep their defaults unless you need to override the deployed driver set.

The first letter of every key in `options` is capitalized, so `password` and `Password` are equivalent.

The following example uses a pre-deployed MySQL driver:

```yson
{
    odbc_config = {
        enable = %true;
        sources = [
            {
                name = "my_mysql";
                description = "My MySQL database";
                driver = "MySQL";
                options = {
                    Server = "mysql.example.com";
                    Port = "3306";
                    Database = "mydb";
                    User = "reader";
                    Password = "${YT_SECURE_VAULT_mysql_password}";
                };
            };
        ];
    };
}
```

All files from `drivers` and `extra_files` must have unique base names because they share one job sandbox.

## Passwords and other secrets { #secrets }

Do not put passwords directly in the speclet. Store them as clique secrets and reference the corresponding `YT_SECURE_VAULT_*` environment variables from `options`:

```bash
yt clickhouse ctl set-secrets --alias my_clique --secrets '{mysql_password="password"}'
```

The speclet example above contains only the `${YT_SECURE_VAULT_mysql_password}` reference. The trampoline substitutes its value inside the clique job before starting ClickHouse.

## Queries { #queries }

Use the [odbc](https://clickhouse.com/docs/sql-reference/table-functions/odbc) table function to query a configured data source:

```sql
SELECT * FROM odbc('DSN=my_mysql', 'my_table')
```

To specify the external database explicitly:

```sql
SELECT * FROM odbc('DSN=my_mysql', 'mydb', 'my_table')
```

The query coordinator reads data from the external database; the read is not distributed across clique instances. ODBC is therefore intended for dictionaries and small tables rather than large scans.

## Limitations { #limitations }

- Every user with the `use` permission for the clique can query every data source configured in it.
- Access to external data is controlled by the account configured in the data source, not by the identity of the user running the CHYT query.
