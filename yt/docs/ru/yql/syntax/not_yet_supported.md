
# Ещё не поддерживаемые конструкции из классического SQL

## NATURAL JOIN {#natural-join}

Доступный альтернативный вариант — явно перечислить совпадающие с обеих сторон колонки.

## NOW() / CURRENT_TIME() {#now}

Доступный альтернативный вариант — воспользоваться функциями [CurrentUtcDate, CurrentUtcDatetime и CurrentUtcTimestamp](../builtins/basic.md#current-utc).


{% if audience == "internal" %}

## Тикеты на добавление возможностей

* \[NOT\] \[EXISTS|INTERSECT\|EXCEPT] [YQL-84]({{yql.todo-exists}})
* NOW() / CURRENT_TIME() [YQL-511]({{yql.todo-now}})

{% endif %}
