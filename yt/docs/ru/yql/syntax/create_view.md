# CREATE VIEW

`CREATE VIEW` создаёт независимое [представление](select/view.md).

В YQL over {{product-name}} поддержаны 2 вида представлений: независимые, которые могут обращаться к произвольным таблицам кластера, и представления, которые привязаны к конкретной таблице.

{% if audience == "internal" %}

Подробнее о привязанных представлениях и о том, как хранятся представления в {{product-name}}, написано в [разделе](../misc/schema.md#yql_view).

{% endif %}

## Доступность

`CREATE VIEW` доступен начиная с версии языка [2025.05](../changelog/2025.05.md).

## Синтаксис
```yql
CREATE VIEW [IF NOT EXISTS] [cluster.]`//path/to/view` AS
DO BEGIN
<top_level_statements>
END DO
```

Данный запрос создаст на кластере `cluster`, а если не указано — то на текущем кластере, независимое представление по пути `//path/to/view`, которое состоит из блока команд `<top_level_statements>`.

В `<top_level_statements>` можно использовать только те команды, которые допустимы в [DEFINE SUBQUERY](subquery.md#define-subquery). Нельзя использовать команду `USE` или явное задание кластера для таблиц — в представлении можно обращаться только к таблицам того кластера, на котором это представление находится. В отличие от `DEFINE SUBQUERY`, команды в `CREATE VIEW` не должны ссылаться на какие-либо значения за пределами тела `CREATE VIEW`.

Если по пути `//path/to/view` уже есть какой-то объект, то создание представления завершится с ошибкой.
Если при этом указан модификатор `IF NOT EXISTS`, то ошибка не возникает, но команда `CREATE VIEW` не приводит к созданию представления (завершается без результата).

{% note warning "Внимание" %}

Пока не поддерживается создание представления и его использование в рамках одного YQL запроса.
При создании представлений следует помнить, что [версия языка](../changelog/index.md#langver-desc) для представления будет определяться запросом, в котором это представление используется, а не запросом, в котором это представление создается.
Аналогичное правило действует и для [библиотек](export_import.md).

{% endnote %}

## Пример

```yql
USE cluster;
$now = CurrentUtcDate();

CREATE VIEW `//home/yql/tutorial/users_with_birth_year` AS
DO BEGIN

$now = CurrentUtcDate(); -- нельзя ссылаться на именованные выражения снаружи

$year_of_birth = ($age) -> (DateTime::GetYear(DateTime::ShiftYears($now, -(cast($age as Int32) ?? 0))));

SELECT $year_of_birth(age) AS birth_year, t.* WITHOUT age FROM `//home/yql/tutorial/users` AS t;

END DO;
```

Как использовать созданное представление можно посмотреть в разделе [SELECT VIEW](select/view.md).

## См. также

* [DROP VIEW](drop_view.md)
