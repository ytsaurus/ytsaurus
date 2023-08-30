# Вставка и удаление данных из консоли

- Создать сортированную динамическую таблицу:
   ```bash
   yt create table //path/to/table --attributes \
   '{dynamic=%true;schema=[{name=id;type=uint64;sort_order=ascending};{name=first_name;type=string};{name=last_name;type=string}]}'
   ```

- Смонтировать таблицу:
   ```bash
   yt mount-table //path/to/table
   ```

- Записать данные:
   ```bash
   echo '{id=1;first_name=Ivan;last_name=Ivanov}; {id=2;first_name=Petr;last_name=Petrov};{id=3;first_name=Sid;last_name=Sidorov}' |
   yt insert-rows //path/to/table --format yson
   ```

- Прочитать данные:
   ```bash
   yt select-rows '* from [//path/to/table]' --format json
   {"id":1,"first_name":"Ivan","last_name":"Ivanov"}
   {"id":2,"first_name":"Petr","last_name":"Petrov"}
   {"id":3,"first_name":"Sid","last_name":"Sidorov"}
   ```

- Удалить данные по ключу:
   ```bash
   echo '{id=1};{id=3}' | yt delete-rows //home/dev/test_dyn_table --format yson
   ```
- Прочитать данные:
   ```bash
   yt select-rows '* from [//path/to/table]' --format json
   {"id":2,"first_name":"Petr","last_name":"Petrov"}
   ```


