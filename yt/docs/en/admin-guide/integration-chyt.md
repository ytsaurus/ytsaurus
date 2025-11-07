# Integrating CHYT with external systems
This instruction demonstrates how you can integrate CHYT with external systems. Like ClickHouse, CHYT supports interfaces for interaction with external systems via HTTP/JDBC.

## Connecting Trino to CHYT
1. [Open ClickHouse ports](../user-guide/data-processing/chyt/try-chyt.md#chyt_port) in the http-proxy configuration.
1. Set up Database-as-Directory.
   Under YT config in the selected clique's speclet, specify the path to the directory that will be represented as a database in ClickHouse terms:
   ```json
   {
       ...
       "database_directories":
       {
           "dbo": "//home/my_db_folder"
       }
   }
   ```
   `dbo`: Sample name of a database that will contain tables from `//home/my_db_folder`.

   Once this setting is specified, queries to the clique will look like this:
   ```sql
   SELECT * FROM dbo.table_name;
   ```
   This will be equivalent to:
   ```sql
   SELECT * FROM `//home/my_db_folder/table_name`
   ```

1. Set up Trino
   Trino connects to CHYT via a built-in ClickHouse connector. Create a connection configuration file `/etc/catalog/ch.properties`:
   ```text
   connector.name=clickhouse
   connection-url=jdbc:clickhouse://my_proxy_url:8123/YT?chyt.clique_alias=my_clique_name
   connection-user=my_clique_name
   connection-password=my_token
   ```
   where `my_proxy_url` is a proxy URL, `my_clique_name` is a clique name, and `my_token` is a {{product-name}} access token.

After that, the `ch` catalog will be available in Trino, with tables located on the {{product-name}} cluster and accessible via the `my_clique_name` clique.
