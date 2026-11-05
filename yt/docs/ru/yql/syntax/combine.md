# COMBINE

Группирует строки двух входных таблиц по общему ключу и применяет UDF или [лямбда-функцию](expressions.md#lambda) к каждой группе. В отличие от [JOIN](join.md), `COMBINE` не строит декартово произведение совпавших строк: функция получает все строки с одинаковым ключом в виде двух списков и может реализовать произвольную логику сопоставления.

## Синтаксис {#syntax}

```yql
COMBINE input1 AS alias1 [PRESORT presort_expression1 [ASC | DESC], ...]
WITH input2 AS alias2 [PRESORT presort_expression2 [ASC | DESC], ...]
ON alias1.key_expression = alias2.key_expression [AND ...]
USING function(item_expression1, item_expression2)
```

## Доступность {#availability}

`COMBINE` доступен начиная с версии языка [2026.02](../changelog/2026.02.md).

## Описание {#description}

В секции `ON` указывается одно или несколько условий равенства, объединённых с помощью `AND`. Несколько предикатов образуют составной ключ в виде кортежа, один предикат — скалярный ключ.

Для каждого ключа, присутствующего хотя бы в одном из входов, `COMBINE` вызывает функцию с тремя аргументами:

1. Общий ключ из секции `ON`.
2. Список значений первого аргумента `USING` для строк первого входа.
3. Список значений второго аргумента `USING` для строк второго входа.

Если ключ встречается только в одном входе, список для другого входа пуст. Поэтому на уровне групп `COMBINE` имеет семантику `FULL JOIN`.

Чтобы передать строку целиком, используйте `TableRow()`. Другие выражения позволяют выбрать нужные столбцы или вычислить значение. Опциональная секция `PRESORT` задаёт порядок строк внутри группы; без неё порядок элементов не определён.

Функция в `USING` может возвращать те же типы, что и [PROCESS](process.md): структуру, опциональную структуру, список или поток структур. Результат преобразуется в плоскую таблицу.

{% note info "Примечание" %}

`COMBINE` полезен, когда строки с одинаковым ключом нужно обработать вместе без размножения строк обычным соединением, например при сопоставлении временных интервалов.

{% endnote %}

## Примеры {#examples}

```yql
$count_rows = ($key, $left_rows, $right_rows) -> {
    RETURN <|
        key: $key,
        left_count: ListLength($left_rows),
        right_count: ListLength($right_rows)
    |>;
};

COMBINE my_table1 AS L
WITH my_table2 AS R
ON L.key = R.key
USING $count_rows(TableRow(), TableRow());
```

```yql
$zip_rows = ($key, $left_rows, $right_rows) -> {
    RETURN <|
        key: $key.0,
        subkey: $key.1,
        rows: ListZipAll($left_rows, $right_rows)
    |>;
};

COMBINE my_table1 AS L PRESORT L.timestamp
WITH my_table2 AS R PRESORT R.timestamp
ON L.key = R.key AND L.subkey = R.subkey
USING $zip_rows(TableRow(), TableRow());
```
