# ClickHouse UDF

ClickHouse UDF предоставляет доступ из YQL к [встроенным функциям](https://clickhouse.tech/docs/ru/sql-reference/functions/) [ClickHouse](https://clickhouse.tech) и [SELECT запросам](https://clickhouse.yandex/docs/en/query_language/select/) на его SQL диалекте.
Также можно использовать функции из [CHYT](https://docs.yandex-team.ru/yt/description/chyt/reference/functions).

{% note warning "Внимание" %}

При использовании в регулярных и production процессах рекомендуется добавить тесты на запросы.

{% endnote %}

Ключевые особенности текущей реализации, которые стоит иметь в виду:

1. Встроенные функции запускаются над каждым значением отдельно, а не блоками, как это происходит в самом ClickHouse. Таким образом по производительности оно как правило медленнее и одноименных функций в ClickHouse и аналогичных функций в YQL. Зато данный механизм позволяет использовать функции, которые доступны в ClickHouse, но по каким-либо причинам не имеют аналогов в YQL.
2. Используется свежий [master ClickHouse на GitHub](https://github.com/ClickHouse/ClickHouse), но часть функциональности отключена из-за расхождений/отсутствия сторонних библиотек в [contrib Аркадии](https://a.yandex-team.ru/arc/trunk/arcadia/contrib). При синхронизации ClickHouse GitHub и Аркадии сейчас отсутствуют механизмы жесткого контроля за обратной совместимостью.
3. В будущем возможно появится более тесная интеграция YQL и ClickHouse, которая будет выглядеть более дружелюбно для пользователей и не будет обладать столь серьезными недостатками.

## Вызов встроенных функций

Встроенные функции ClickHouse доступны для вызова из YQL через префикс `ClickHouse::`. Например, чтобы вызвать ClickHouse функцию length, нужно написать `ClickHouse::length("my_string")`.

Полный список встроенных функций ClickHouse на данный момент [доступен только в его документации](https://clickhouse.tech/docs/ru/sql-reference/functions/). Как упоминалось выше, многие из них сейчас не доступны из Аркадии, но активно ведётся работа по сокращению расхождения.

## ClickHouse::run

Позволяет обрабатывать таблицы SELECT запросом в диалекте ClickHouse через [PROCESS](../../syntax/process.md) или [REDUCE](../../syntax/reduce.md). В этом режиме обработка идёт потоково и при обработке больших таблиц стоит ожидать производительность на более приемлемом уровне по сравнению с вызовом ClickHouse функции по отдельности. В `REDUCE` рекомендуется использовать `TableRows()`, чтобы потоковая обработка запускалась на весь джоб один раз, а не на каждый ключ отдельно, из-за чего при небольшом числе записей на ключ константные расходы на инициализацию могут быть заметны на общем фоне. Также есть накладные расходы на копирование данных на границах между YQL и ClickHouse движками и чем «тяжелее» ClickHouse запрос, тем менее это будет заметно на его фоне.

``` sql
-- Преагрегация данных через PROCESS
PROCESS my_table
USING ClickHouse::run(
    TableRows(),
    "select key as key, count(*) as cnt from Input group by key" -- ClickHouse SQL
);
```

``` sql
-- Финальная агрегация данных по заданному ключу через REDUCE
$callable = ($input) -> {
    return ClickHouse::run(
        $input,
        "select key as key, sum(cnt) as cnt from Input group by key" -- ClickHouse SQL
    )
};

REDUCE my_table
ON key
USING ALL $callable(TableRows());
```

Для обращения к входным данным внутри ClickHouse запроса необходимо обращаться к таблице с константным именем `Input`.

## ClickHouse::source

Позволяет выполнить запрос в диалекте ClickHouse без входных данных.

``` sql
SELECT * FROM AS_TABLE(()->(ClickHouse::source(
    "select * from system.numbers limit 5" -- ClickHouse SQL
)));
```

## Соответствие типов данных

| Тип данных ClickHouse | Тип данных YQL | Комментарий
| --- | --- | --- |
| IntN | IntN | |
| UIntN | UintN | |**
| Float32 | Float | |
| Float64 | Double | |
| String | String | Индексация элементов в ClickHouse начинается с 1 |
| FixedString(N) | String | Индексация элементов в ClickHouse начинается с 1, поддерживается только как выходной тип |
| Date | Date | |
| DateTime | DateTime | |
| UUID | Uuid | |
| Nullable | Optional | |
| Array | List | Индексация элементов в ClickHouse начинается с 1 |
| Tuple | Tuple | Индексация элементов в ClickHouse начинается с 1 |
| AggregateFunction(...) | Tagged<String,'AggregateFunction(...)'> | |
| Enum(S1=V1,S2=V2,...) | Enum<S1,S2,...> | |

Остальные типы данных не поддерживаются.
