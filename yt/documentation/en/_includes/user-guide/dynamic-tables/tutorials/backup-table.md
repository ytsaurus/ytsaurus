# Creating a dynamic table backup

{% list tabs %}

- CLI

   1. Allow read access and prohibit data write:
      ```bash
      yt freeze-table //path/to/table
      ```

   2. Copy a table:
      ```bash
      yt copy //path/to/table //path/to/table_copy
      ```

   3. Allow data write access:
      ```bash
      yt unfreeze-table //path/to/table
      ```

- Web interface

   - To allow read access and prohibit data write, select `Freeze` in the table context menu.
   - To copy a table, select `Copy` in the context menu and specify the name of the new table.
   - To allow data write, select `Unfreeze`.

- MapReduce

   If you need to give data read and write access when copying a table, dump the contents of the dynamic table into the static table and back again. To do this, use the script described in the [MapReduce for dynamic tables](../../../user-guide/dynamic-tables/mapreduce.md) section.

{% endlist %}

