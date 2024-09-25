Stderr-таблица имеет следующие колонки:

1. `job_id` — id джоба.
2. `part_index` — если stderr одного джоба большой, он разбивается на части. Значение `part_index` указывает на номер конкретной части stderr.
3. `data` — непосредственно сами данные stderr.

Таблица отсортирована по `job_id`, `part_index`.

Прочитать такую таблицу можно с помощью команды [read_blob_table](../../../../user-guide/storage/blobtables.md#read_blob_table).
