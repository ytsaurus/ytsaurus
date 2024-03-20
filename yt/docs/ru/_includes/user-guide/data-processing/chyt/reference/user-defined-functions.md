# Пользовательские SQL функции

## Создание и удаление SQL UDF { #create_delete_udf }

[CREATE FUNCTION](https://clickhouse.com/docs/en/sql-reference/statements/create/function) - создать функцию из лямбда-выражения. Функции, созданные таким образом, доступны только в рамках используемой клики всем пользователям.
```sql
CREATE FUNCTION linearEquation AS (x, k, b) -> k*x + b;
SELECT number, linearEquation(number, 2, 1) FROM numbers(3);
```

[DROP FUNCTION](https://clickhouse.com/docs/en/sql-reference/statements/drop#drop-function) - удалить пользовательскую SQL функцию.
```sql
DROP FUNCTION linearEquation;
```

Для использования обеих команд необходимо право `Manage` на клику.

## Использование функции в запросе { #udf_usage }
Пользовательская функция хранится в состоянии клики, поэтому нет нужды пересоздавать ее в каждом запросе. Ее можно вызвать так же, как и любую встроенную функцию ClickHouse.

Если во время исполнения запроса кто-нибудь пересоздаст используемую UDF, то вызовы данной функции могут иметь разные реализации. Нет никаких гарантий, какая из них будет выбрана на каждом инстансе клики.

## Просмотр всех SQL UDF клики { #get_all_udfs }
```sql
SELECT name, create_query FROM system.functions
WHERE origin = 'SQLUserDefined'
```
Результат:
|name            |create_query |
| -------------- | ----------- |
| linearEquation | CREATE FUNCTION linearEquation AS (x, k, b) -> ((k * x) + b) |
