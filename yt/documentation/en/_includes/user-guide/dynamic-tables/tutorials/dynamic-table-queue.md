# Using a dynamic table as a queue

To implement a queue using dynamic tables, use ordered dynamic tables. Such tables are a sequence of ordered rows, with no key columns.

{% list tabs %}

- CLI

   1. Create a table:
      ```bash
      yt create table //path/to/table --attributes \
      '{dynamic=%true;schema=[{name=first_name;type=string};{name=last_name;type=string}]}'
      ```

   2. Mount a table:
      ```bash
      yt mount-table //path/to/table
      ```

   3. Write data:
      ```bash
      echo '{first_name=Ivan;last_name=Ivanov}' | yt insert-rows //path/to/table --format yson
      ```

   4. Read data:
      ```bash
      yt select-rows '* from [//path/to/table]' --format json
      {"first_name":"Ivan","last_name":"Ivanov"}
      ```


- Python

   1. Create a table:
      ```python
      import yt.wrapper as yt

      schema = [
          {
              'name': 'first_name',
              'type': 'string'
          },
          {
              'name': 'last_name',
              'type': 'string'
          }
      ]

       yt.create('table', '//path/to/table', attributes = {'dynamic': True, 'schema': schema})
      ```

   2. Mount a table:
      ```python
      yt.mount_table('//path/to/table')
      ```

   3. Write data:
      ```python
      data = [{
              'first_name': 'Ivan',
              'last_name': 'Ivanov'
      }]

      client.insert_rows('//path/to/table', data)
      ```

   4. Read data:
      ```python
      for d in client.select_rows('* FROM [//path/to/table]'):
          print(d)
      {'$tablet_index': 0L, 'first_name': 'Ivan', '$row_index': 0L, 'last_name': 'Ivanov'}
      ```

{% endlist %}

For more information about cleaning up and splitting ordered dynamic tables into tablets, see [Ordered dynamic tables](../../../user-guide/dynamic-tables/ordered-dynamic-tables.md).


