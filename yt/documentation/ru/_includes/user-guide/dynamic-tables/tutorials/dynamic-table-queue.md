# Использование динамической таблицы как очереди

Для реализации очереди с помощью динамических таблиц, используйте упорядоченные динамические таблицы. Такие таблицы представляют собой последовательность упорядоченных строк, ключевые колонки в таких таблицах отсутствуют.

{% list tabs %}

- CLI

  1. Создать таблицу:
     ```bash
     yt create table //path/to/table --attributes \
     '{dynamic=%true;schema=[{name=first_name;type=string};{name=last_name;type=string}]}'
     ```

  2. Смонтировать таблицу:
     ```bash
     yt mount-table //path/to/table
     ```

  3. Записать данные:
     ```bash
     echo '{first_name=Ivan;last_name=Ivanov}' | yt insert-rows //path/to/table --format yson
     ```

  4. Прочитать данные:
     ```bash
     yt select-rows '* from [//path/to/table]' --format json
     {"first_name":"Ivan","last_name":"Ivanov"}
     ```


- Python

  1. Создать таблицу:
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

  2. Смонтировать таблицу:
     ```python
     yt.mount_table('//path/to/table')
     ```

  3. Записать данные:
     ```python
     data = [{
             'first_name': 'Ivan',
             'last_name': 'Ivanov'
     }]

     client.insert_rows('//path/to/table', data)
     ```

  4. Прочитать данные:
     ```python
     for d in client.select_rows('* FROM [//path/to/table]'):
         print(d)
     {'$tablet_index': 0L, 'first_name': 'Ivan', '$row_index': 0L, 'last_name': 'Ivanov'}
     ```

{% endlist %}

Подробнее об очистке и о делении упорядоченных динмических таблиц на таблеты можно прочитать в разделе [Упорядоченные динамические таблицы](../../../user-guide/dynamic-tables/ordered-dynamic-tables.md).


