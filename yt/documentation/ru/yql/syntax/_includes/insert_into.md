---
vcsPath: ydb/docs/ru/core/yql/reference/yql-core/syntax/_includes/insert_into.md
sourcePath: ydb/docs/ru/core/yql/reference/yql-core/syntax/_includes/insert_into.md
---
# INSERT INTO
Добавляет строки в таблицу.  Если целевая таблица уже существует и не является сортированной, операция `INSERT INTO` дописывает строки в конец таблицы. В случае сортированной таблицы, YQL пытается сохранить сортированность путем запуска сортированного слияния. 

Таблица по имени ищется в базе данных, заданной оператором [USE](../use.md).

`INSERT INTO` позволяет выполнять следующие операции:

* Добавление константных значений с помощью [`VALUES`](../values.md).

  ```sql
  INSERT INTO my_table (Key1, Key2, Value1, Value2)
  VALUES (345987,'ydb', 'Яблочный край', 1414);
  COMMIT;
  ```

  ``` sql
  INSERT INTO my_table (key, value)
  VALUES ("foo", 1), ("bar", 2);
  ```

* Сохранение результата выборки `SELECT`.

  ```sql
  INSERT INTO my_table
  SELECT Key AS Key1, "Empty" AS Key2, Value AS Value1
  FROM my_table1;
  ```

