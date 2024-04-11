In the CHYT CLI, depending on the command, the name of the clique is passed differently: either with or without an asterisk `*`.

1. The `execute` command must always be called with an asterisk and the `--alias` key. Example:

   ```bash
   yt clickhouse execute --proxy <cluster_name> --alias *<clique_name>
   ```

   This is because it's a rather old command that includes logic dependent on the aliases of {{product-name}} operations, which start with an asterisk.

2. You can omit the asterisk `*` in commands from the `yt clickhouse ctl` range. Example:

   ```bash
   yt clickhouse ctl start <clique_name>
   ```

   Note that the old format is still supported for backward compatibility. Example:

   ```bash
   yt clickhouse ctl start *<clique_name>
   ```
