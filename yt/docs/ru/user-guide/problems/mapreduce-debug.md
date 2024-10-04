# Отладка MapReduce программ

{% include [Локальная эмуляция запуска джоба](../../_includes/user-guide/problems/mapreduce-debug/local-emulation.md) %}

{% include [Получение текущего stderr выполняющегося джоба](../../_includes/user-guide/problems/mapreduce-debug/stderr-running.md) %}

## Получение полных stderr всех джобов операции

В системе {{product-name}} существует возможность сохранить полные stderr всех джобов в таблицу. Можно выгрузить stderr тех джобов, которые не были прерваны (aborted).

Для включения описанного поведения:

{% list tabs %}

- В Python

  Используйте параметр `stderr_table`. Например:

  ```python
  yt.wrapper.run_map_reduce( mapper, reducer, '//path/to/input', '//path/to/output', reduce_by=['some_key'], stderr_table='//path/to/stderr/table', )
  ```
- В С++

  Используйте настройку [StderrTablePath](https://github.com/ytsaurus/ytsaurus/blob/07c9c385ae116f56d8ecce9fa6765fa1a90e95cc/yt/cpp/mapreduce/interface/operation.h#L563).

- В SDK на других языках

  Можно передать настройку `stderr_table_path` напрямую в спецификацию операции. Описание данной опции см. в разделе [Настройки операций](../data-processing/operations/operations-options.md).

{% endlist %}

{% include [Stderr-таблица](../../_includes/user-guide/problems/mapreduce-debug/stderr-table.md) %}
