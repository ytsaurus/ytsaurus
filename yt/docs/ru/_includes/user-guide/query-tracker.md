# Query tracker

Query tracker — компонент {{product-name}}, позволяющий работать с системой с помощью человекочитаемых запросов на SQL-подобных языках. Пользователь отправляет запросы в Query tracker, они исполняются в движке, и пользователю возвращается результат исполнения.

Query tracker позволяет:

+ Отправлять произвольные запросы на исполнение.
+ Отслеживать исполнение.
+ Сохранять результаты.
+ Смотреть историю запросов.

[Пример работы с Query tracker](#example).

Запросы характеризуются движком и текстом. Движок контролирует исполнение запроса. Текст запроса специфичен для движка.

Таким образом, можно работать с данными, используя разные языки и среды выполнения, достигая заданных гарантий и производительности.

## Движки {#engines}

На текущий момент поддерживаются следующие движки исполнения:

+ [YT QL](../../user-guide/dynamic-tables/dyn-query-language.md)
  + Встроенный в YT язык запросов. Поддерживает только динамические таблицы.
+ [YQL](../../yql/index.md)
  + Запрос исполняется в YQL агентах. Они разбивают запрос в YT операции (map, reduce, ...), запускают их, собирают и возвращают результат.
+ [CHYT](../../user-guide/data-processing/chyt/about-chyt.md)
  + Запрос исполняется в клике.
+ [SPYT](../../user-guide/data-processing/spyt/overview.md)
  + Запрос исполняется в Spark кластере.

## API {#api}

Все операции принимают опциональный параметр `query_tracker_stage` — он позволяет выбрать инсталляцию Query tracker, в которой будут запускаться запросы. Значение по умолчанию — `production`.

### start_query {#start-query}

Отправляет запрос на исполнение. Возвращает идентификатор запроса.

Обязательные параметры:

+ `engine` — движок для исполнения запроса. Поддерживаются `ql`, `yql`, `chyt`, `spyt`.
+ `query` — текст запроса.

Опциональные параметры:

+ `files` — список файлов для запроса в формате YSON.
+ `settings` — дополнительные параметры для запроса в формате YSON.
  + В [CHYT](../../user-guide/data-processing/chyt/about-chyt.md) необходимо указывать параметр `clique` — алиас клики. По умолчанию используется публичная клика.
  + В [SPYT](../../user-guide/data-processing/spyt/overview.md) необходимо указывать параметр `discovery_path` — директория для служебных данных существующего кластера Spark.
+ `draft` — является ли запрос черновиком. Такие запросы завершаются автоматически без исполнения.
+ `annotations` — произвольные аннотации к запросу. Позволяют осуществлять удобный поиск по запросам. В формате YSON.
+ `access_control_object` — название объекта в `//sys/access_control_object_namespaces/queries/`, который определяет доступ к запросу для других пользователей.

Пример: `start_query(engine="yql", query="SELECT * FROM my_table", access_control_object="my_aco")`

### abort_query {#abort-query}

Прерывает исполнение запроса. Ничего не возвращает.

Обязательные параметры:

+ `query_id` — идентификатор запроса.

Опциональные параметры:

+ `message` — сообщение о прерывании.

Пример: `abort_query(query_id="my_query_id")`.

### get_query_result {#get-query-result}

Возвращает мета-информацию о результатах исполнения запроса.

Обязательные параметры:

+ `query_id` — идентификатор запроса.
+ `result_index` — идентификатор результата.

Пример: `get_query_result(query_id="my_query_id", result_index=0)`.

### read_query_result {#read-query-result}

Возвращает результаты запроса.

Обязательные параметры:

+ `query_id` — идентификатор запроса.
+ `result_index` — идентификатор результата.

Опциональные параметры:

+ `columns` — список колонок для прочтения.
+ `lower_row_index` — с какой строки результата читать.
+ `upper_row_index` — до какой строки результата читать.

Пример: `read_query_result(query_id="my_query_id", result_index=0)`.

### get_query {#get-query}

Возвращает информацию о запросе.

Обязательные параметры:

+ `query_id` — идентификатор запроса.

Опциональные параметры:

+ `attributes` — фильтр атрибутов для возврата.
+ `timestamp` — информация о запросе будет консистентна с системой в этот момент времени.

Пример: `get_query(query_id="my_query_id")`.

### list_queries {#list-queries}

Получает список запросов по заданным фильтрам.

Опциональные параметры:

+ `from_time` — нижний порог времени запуска запроса.
+ `to_time` — верхний порог времени запуска запроса.
+ `cursor_direction` — порядок сортировки запросов по времени запуска.
+ `cursor_time` — время остановки курсора, действует только при указании `cursor_direction`.
+ `user_filter` — фильтр по автору запроса.
+ `state_filter` — фильтр по состоянию запроса.
+ `engine_filter` — фильтр по движку запроса.
+ `substr_filter` — фильтр по идентификатору запроса, аннотациям и [access control object](#access-control).
+ `limit` — сколько записей возвращать. Значение по умолчанию — 100.
+ `attributes` — фильтр атрибутов для возврата.

Пример: `list_queries()`.

### alter_query {#alter-query}

Изменяет запрос. Ничего не возвращает.

Обязательные параметры:

+ `query_id` — идентификатор запроса.

Опциональные параметры:

+ `annotations` — новые аннотации для запроса.
+ `access_control_object` — новый access control object для запроса.

Пример: `alter_query(query_id="my_query_id", access_control_object="my_new_aco")`.

## Access control {#access-control}

Для управления доступом к запросам и их результатам, в запрос сохраняется опциональная строка `access_control_object`, которая указывает на `//sys/access_control_object_namespace/queries/[access_control_object]`.

Access Control Object (ACO) — это объект с атрибутом `@principal_acl`, который задаёт правила доступа тем же способом, что и `@acl` для нод Кипариса. Подробнее можно почитать в разделе [Контроль доступа](../../user-guide/storage/access-control.md).

Создание ACO возможно через пользовательский интерфейс, либо с помощью вызова команды [create](../../user-guide/storage/cypress-example.md#create):

`yt create access_control_object --attr '{namespace=queries;name=my_aco}'`.

Все API используют ACO для проверки доступа:

+ `start_query` проверяет, что переданный ACO существует.
+ `alter_query` проверяет, что ACO запроса разрешает `Administer` для пользователя и переданный ACO существует.
+ `get_query` проверяет, что ACO запроса разрешает `Use` для пользователя.
+ `list_queries` проверяет, что ACO запроса разрешает `Use` для пользователя.
+ `get_query_result` проверяет, что ACO запроса разрешает `Read` для пользователя.
+ `read_query_result` проверяет, что ACO запроса разрешает `Read` для пользователя.
+ `abort_query` проверяет, что ACO запроса разрешает `Administer` для пользователя.

Также есть несколько особенностей:

+ Создатель запроса всегда имеет доступ к своим запросам.
+ Если у запроса нет ACO, для проверки используется ACO по умолчанию — `nobody`.

## Пример {#example}

Типичный сценарий работы с Query tracker выглядит следующим образом:

1. Отправляем запрос на исполнение.
   + `start_query(engine="yql", query="SELECT * from //home/me/my_table", access_control_object="my_team_aco")`.
2. Получаем список запросов, доступных для нас.
   + `list_queries()`.
3. Получаем информацию о нашем запросе.
   + `get_query(query_id="my_query_id")`.
4. Дожидаемся завершения запроса.
   + `get_query(query_id="my_query_id").state == "completed"`.
5. Получаем мета-информацию о результатах (размер результата, схему таблицы).
   + `get_query_result(query_id="my_query_id", result_index=0)`.
6. Читаем результат.
   + `read_query_result(query_id="my_query_id", result_index=0)`.
7. Меняем ACO запроса, открывая доступ для всех.
   + `alter_query(query_id="my_query_id", access_control_object="share_with_everyone_aco")`.
