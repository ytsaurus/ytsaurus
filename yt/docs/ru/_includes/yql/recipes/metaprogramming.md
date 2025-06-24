# Практическое применение метапрограммирования на YQL

## Введение
Из-за отсутствия в {{product-name}} нативной поддержки партиционированных таблиц, типовой ситуацией является хранение логически единой таблицы в виде множества физических таблиц, разбитых по датам, часам или остатку от деления хеша какого-либо из полей. Таким образом, в YQL over {{product-name}} запросы часто работают с огромным количеством таблиц (десятки, сотни и тысячи).

По меркам стандарта SQL это исключительная ситуация и общепринятых механизмов для описания таких запросов им не предоставляется. YQL обладает многими расширениями стандарта, спроектированных специально для подобных специфичных задач. Также для регулярно или многократно запускаемых запросов в большинстве случаев они позволяют обойтись совсем без генерации текстов запросов на клиенте. Каждый механизм в отдельности [подробно описан в основном руководстве по синтаксису YQL](../../../yql/syntax/index.md), а цель данной статьи &mdash; продемонстрировать, как их можно комбинировать для достижения практически полезных результатов на приближенных к реальности примерах.{% if audience == "internal" %} В [основной Tutorial]({{yql.link}}/Tutorial/yt_01_Select_all_columns) эти примеры по ряду причин пока не включены, но в будущем вероятно будут.{% endif %}

## Сценарии

### Выбор таблиц по датам
Архив подавляющего большинства логов хранится на {{product-name}} в плоской директории, где каждой дате соответствует отдельная таблица. Чтобы выполнить расчёт по интересующим датам и при этом не подставлять их в текст запроса вручную, можно воспользоваться табличной функцией `FILTER` в сочетании с выбирающей нужные даты lambda функцией.

``` yql
USE {{production-cluster}};

$log_type = "yql-log";
$base_folder = "logs/" || $log_type || "/1d";
$today = CurrentUtcDate();
$yesterday = $today - Interval("P1D");
$a_week_ago = $today - Interval("P1W");

$is_during_last_week = ($table_name) -> {
    $date = CAST($table_name AS Date);
    RETURN ($date BETWEEN $a_week_ago AND $yesterday) ?? false;
};

SELECT COUNT(*)
FROM FILTER($base_folder, $is_during_last_week);
```

Ссылки на документацию:

* [CurrentUtcDate](../../../yql/builtins/basic.md#currentutcdate)
* [Lambda функции](../../../yql/syntax/expressions.md#lambda)
* [FILTER](../../../yql/syntax/select/index.md)

### Обработка таблиц скользящим окном с кешированием
Допустим, вам необходимо ежедневно считать какой-то агрегат по данным за последнюю неделю примерно как в предыдущем примере. Так как по своей природе большинство таблиц с логами перестают меняться уже начиная с ночи следующего дня, то условно 6/7-х вычислительных ресурсов каждый день тратилось на повторное вычисление того, что уже считалось в предыдущий раз. В YQL есть механизм [автоматического кеширования промежуточных результатов вычислений](../../../yql/syntax/pragma.md#yt.querycachemode), но так как он оперирует на уровне MapReduce операций целиком, то в предыдущем запросе cache hit был бы нулевой: каждый день получался бы свой набор входных таблиц в первую Map операцию, а он участвует в вычислении ключа в кеше.

Чтобы дать механизму автоматического кеширования шанс на успех (или даже гарантировать его, если переопределить [PRAGMA yt.TmpFolder](../../../yql/syntax/pragma.md#yt.tmpfolder) в свою директорию/квоту вместо `//tmp`), необходимо считать агрегат по каждому дню по отдельности, а затем объединять их в одно значения для получения требуемого результата, благо для большинства агрегатных функций это возможно.

``` yql
USE {{production-cluster}};

$log_type = "visit-log";
$base_folder = "logs/" || $log_type || "/1d/";
$today = CurrentUtcDate();
$range = ListFromRange(1, 9);

$shift_back_by_days = ($days_count) -> {
    RETURN Unwrap(
        $today - $days_count * Interval("P1D"),
        "Failed to shift today back by " || CAST($days_count AS String) || " days"
    );
};
$dates = ListMap($range, $shift_back_by_days);

DEFINE ACTION $max_income($date) AS
    $table_path = $base_folder || CAST($date AS String);
    INSERT INTO @daily_results
    SELECT MAX(CAST(Income AS Double)) AS MaxIncome
    FROM $table_path;
END DEFINE;

EVALUATE FOR $date IN $dates DO $max_income($date);

COMMIT;

SELECT MAX(MaxIncome) AS MaxIncome
FROM @daily_results;

DISCARD SELECT
    Ensure(
        NULL,
        COUNT(*) == 7,
        "Unexpected number of aggregates to be merged: " || CAST(COUNT(*) AS String) || " instead of 7"
    )
FROM @daily_results;
```

В данном примере, в отличие от предыдущего пути к таблицам не выбираются из содержимого директории, а генерируются «из воздуха», основываясь на принятой для данного лога схеме именования таблиц. Объявляется «действие» для обработки каждой таблицы в отдельности (`DEFINE ACTION`), которое затем в цикле применяется к каждой дате, которая должна попасть в расчет (`EVALUATE FOR`).

Также в данном запросе демонстрируется несколько способов «уронить» запрос с содержательной ошибкой вместо выдачи некорректного результата, если вдруг начало получаться что-то странное:

* Арифметика над датами может вернуть NULL в практически невозможных для данного сценария ситуациях выхода за диапазон поддерживаемых значений, что делает достаточно безопасным использование Unwrap — превращения Optional значения в не-Optional (точно не NULL), с ошибкой времени выполнения если в Optional таки лежал NULL.
* Ensure позволяет проверить выполнение произвольных условий во время выполнения запроса и вернуть ошибку, если условие не выполнено. Его можно вставлять и прямо по ходу вычисления, но в сочетании с `DISCARD` (посчитать что-то и выкинуть результат) можно и сделать отдельную проверку сбоку, например по агрегату от временной или итоговой таблицы как здесь. В варианте с `DISCARD` первый аргумент Ensure, который обычно становится результатом его вызова, особого смысла не имеет и туда можно писать любую константу, например NULL.

Ссылки на документацию:

* [Lambda функции](../../../yql/syntax/expressions.md#lambda)
* [CurrentUtcDate](../../../yql/builtins/basic.md#currentutcdate)
* [ListFromRange](../../../yql/builtins/list.md#listfromrange) / [ListMap](../../../yql/builtins/list.md#listmap) / [ListLength](../../../yql/builtins/list.md#listlength)
* [Unwrap](../../../yql/builtins/basic.md#unwrap)
* [DEFINE ACTION](../../../yql/syntax/action.md)
* [EVALUATE FOR](../../../yql/syntax/action.md#evaluate-for)
* [@foo](../../../yql/syntax/select/temporary_table.md)
* [COMMIT](../../../yql/syntax/commit.md)
* [DISCARD](../../../yql/syntax/discard.md)
* [Ensure](../../../yql/builtins/basic.md#ensure)


### Сценарий с обработкой заданий
Допустим у вас есть некий внешний процесс, записывающий в определённую директорию таблицы, которые по сути являются заданиями для обработки: необходимо запускать на основе содержимого какой такой таблицы вычисление, записывать результат в другую директорию, а обработанные таблицы удалять — и всё это транзакционно.

{% cut "Как можно сэмулировать такой внешний процесс на YQL для теста" %}

``` yql
USE {{production-cluster}};

$root_folder = "//tmp/tasks/";
$values = ListFromRange(1, 10);

DEFINE ACTION $create_task($i) AS
    $path = $root_folder || CAST($i AS String);
    INSERT INTO $path SELECT $i AS Task;
END DEFINE;

EVALUATE FOR $value IN $values DO $create_task($value);
```

{% endcut %}

Собственно запуск обработки:
``` yql
USE {{production-cluster}};

$tasks_folder = "tmp/tasks";
$results_folder = "tmp/results";

DEFINE ACTION $process_task($input_path) AS
    $output_path = String::ReplaceAll($input_path, $tasks_folder, $results_folder);

    INSERT INTO $output_path WITH TRUNCATE
    SELECT Task * Task AS Result
    FROM $input_path;
    COMMIT;
    DROP TABLE $input_path;
END DEFINE;

$tasks = (
    SELECT AGGREGATE_LIST(Path)
    FROM FOLDER($tasks_folder, "row_count")
    WHERE Type == "table" AND
        Yson::LookupInt64(Attributes, "row_count") == 1
);

EVALUATE FOR $task IN $tasks ?? ListCreate(TypeOf($tasks)) DO $process_task($task);
```
Здесь также используется механизм «действий» с циклом по списку, но при этом список входных таблиц берётся не «из воздуха», а из табличной функции `FOLDER`, которая предоставляет доступ к содержимому произвольной директории в {{product-name}}. Так как `FOLDER` выдаёт не только таблицы, но и другие виды узлов, то если нет уверенности, что в директории лежат только таблицы, то лучше добавлять фильтр по Type или по произвольным мета атрибутам таблицы (здесь row_count для примера). Для этого надо заказать их во втором аргументе `FOLDER` (если несколько, то через точку с запятой) и затем обратиться к колонке Attributes типа Yson.

ListExtract делает из списка структур с одним элементом список сразу строк с путями, а можно было бы доставать Path и уже внутри действия.

Ссылки на документацию:

* [String UDF](../../../yql/udf/list/string.md) / [Yson UDF](../../../yql/udf/list/yson.md)
* [ListFromRange](../../../yql/builtins/list.md#listfromrange) / [ListExtract](../../../yql/builtins/list.md#listextract)
* [FOLDER](../../../yql/syntax/select/folder.md)
* [DROP TABLE](../../../yql/syntax/drop_table.md)
