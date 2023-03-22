# Python API на примерах

Перед запуском примеров рекомендуется прочитать [инструкцию](../../../user-guide/storage/auth.md) по получению токена.

Все программы рекомендуется запускать из-под `Linux`.

## Базовый уровень { #base }

Современный способ работы с Python API — типизированный.
Данные, хранящиеся в таблицах и обрабатываемые операциями, представляются в коде [классами](userdoc.md#dataclass) с типизированными полями (аналог [dataclasses](https://docs.python.org/3/library/dataclasses.html)). Нерекомендуемый (но иногда неизбежный, особенно при работе со старыми таблицами) способ — нетипизированный, когда строки таблиц представляются словарями.
Такой способ гораздо более медленный, чреват ошибками и неудобен при работе со сложными типами. Поэтому в данном разделе документации приведены примеры работы с типизированным API, а нетипизированные примеры можно найти в [соответствующем разделе](#untyped_tutorial).

### Чтение и запись таблиц { #read_write }

{{product-name}} позволяет записывать данные в таблицы, а также дописывать данные в конец существующих таблиц. Для чтения таблиц доступны несколько режимов: чтение всей таблицы, чтение отдельных диапазонов по номеру строки или ключу.

Пример находится в [yt/cpp/mapreduce/examples/typed-python-tutorial/table_read_write](https://github.com/ytsaurus/ytsaurus/tree/main/yt/cpp/mapreduce/examples/typed-python-tutorial/table_read_write).

### Простой map { #simple_map }

Предположим, имеется таблица с колонками login, name, uid по адресу //home/dev/tutorial/staff_unsorted.
Необходимо сделать таблицу с email адресами, вычислив их из логина: `email = login + "@ytsaurus.tech"`.

Для решения задачи подойдёт простой mapper.

Пример находится в [yt/cpp/mapreduce/examples/typed-python-tutorial/simple_map](https://github.com/ytsaurus/ytsaurus/tree/main/yt/cpp/mapreduce/examples/typed-python-tutorial/simple_map)

### Сортировка таблицы и простая операция reduce { #sort_and_reduce }

По той же таблице что и в предыдущем примере можно посчитать статистику — сколько раз встречается то или иное имя и самый длинный логин среди всех людей с одним именем. Для решения данной задачи хорошо подходит операция Reduce. Так как операцию Reduce можно запускать только на сортированных таблицах, сперва необходимо отсортировать исходную таблицу.

Пример находится в [yt/cpp/mapreduce/examples/typed-python-tutorial/simple_reduce](https://github.com/ytsaurus/ytsaurus/tree/main/yt/cpp/mapreduce/examples/typed-python-tutorial/simple_reduce).

### Reduce с несколькими входными таблицами { #reduce_multiple_output }

Предположим, кроме таблицы с пользователями есть другая таблице, в которой записано какой пользователь является служебным. Поле is_robot принимает значения true или false.

Следующая программа формирует таблицу, в которой остаются только служебные пользователи.

Пример находится в [yt/cpp/mapreduce/examples/typed-python-tutorial/multiple_input_reduce](https://github.com/ytsaurus/ytsaurus/tree/main/yt/cpp/mapreduce/examples/typed-python-tutorial/multiple_input_reduce).

### Reduce с несколькими входными и несколькими выходными таблицами { #reduce_multiple_input_output }

Данный пример повторяет предыдущий с той разницей, что запись будет производиться сразу в две выходные таблицы — и с пользователями-людьми, и со служебными пользователями.

Пример находится в [yt/cpp/mapreduce/examples/typed-python-tutorial/multiple_input_multiple_output_reduce](https://github.com/ytsaurus/ytsaurus/tree/main/yt/cpp/mapreduce/examples/typed-python-tutorial/multiple_input_multiple_output_reduce).

### Схемы таблиц { #table_schema }

У всех таблиц в {{product-name}} есть [схема](userdoc.md#table_schema).
В коде продемонстрированы примеры работы со схемой таблиц.

Пример находится в [yt/cpp/mapreduce/examples/typed-python-tutorial/table_schema](https://github.com/ytsaurus/ytsaurus/tree/main/yt/cpp/mapreduce/examples/typed-python-tutorial/table_schema).

### MapReduce { #map_reduce }

В {{product-name}} реализована слитная операция MapReduce, которая работает быстрее, нежели Map + Sort + Reduce. Попробуем по таблице с пользователями ещё раз посчитать статистику, сколько раз встречается то или иное имя. Перед подсчётом статистики нормализируем имена, приведя их к нижнему регистру, чтобы люди с именами `АРКАДИЙ` и `Аркадий` склеились в нашей статистике.

Пример находится в [yt/cpp/mapreduce/examples/typed-python-tutorial/map_reduce](https://github.com/ytsaurus/ytsaurus/tree/main/yt/cpp/mapreduce/examples/typed-python-tutorial/map_reduce).

### MapReduce с несколькими промежуточными таблицами { #map_reduce_multiple_intermediate_streams }

Промежуточные данные (между стадиями map и reduce) в операции MapReduce могут "течь" в несколько потоков и иметь разные типы. В данном примере на входе операции две таблицы: первая отображает uid в имя, а вторая содержит информация о событиях, связанных с пользователем с данным uid. Mapper отбирает события типа "клик" и посылает их в один выходной поток, а всех пользователей отправляет в другой. Reducer считает все клики по данному пользователю.

Пример находится в [yt/cpp/mapreduce/examples/typed-python-tutorial/map_reduce_multiple_intermediate_streams](https://github.com/ytsaurus/ytsaurus/tree/main/yt/cpp/mapreduce/examples/typed-python-tutorial/map_reduce_multiple_intermediate_streams).

### Декораторы для классов-джобов { #job_decorators }

Существует возможность пометить функции или классы джобов специальными декораторами, меняющими ожидаемый интерфейс взаимодействия с джобами.
Примеры декораторов: `with_context`, `aggregator`, `reduce_aggregator`, `raw`, `raw_io`. Полное описание можно найти в [документации](../../../api/python/userdoc.md#python_decorators)

Пример находится в [yt/cpp/mapreduce/examples/typed-python-tutorial/job_decorators](https://github.com/ytsaurus/ytsaurus/tree/main/yt/cpp/mapreduce/examples/typed-python-tutorial/job_decorators).

### Работа с файлами на клиенте и в операциях { #files }
Более полная информация в [документации](../../../api/python/userdoc.md#file_commands).
Подробнее про файлы в Кипарисе можно прочитать в [разделе](../../../user-guide/storage/files.md).

Пример находится в [yt/cpp/mapreduce/examples/typed-python-tutorial/files](https://github.com/ytsaurus/ytsaurus/tree/main/yt/cpp/mapreduce/examples/typed-python-tutorial/files).

### Генеричный grep { #grep }
Типизированное API позволяет работать с достаточно произвольными данными, используя один класс данных и один класс операции. В качестве примера рассмотрим задачу фильтрации таблицы по условию совпадения регулярного выражения с заданным строковым полем.

Пример находится в [yt/cpp/mapreduce/examples/typed-python-tutorial/grep](https://github.com/ytsaurus/ytsaurus/tree/main/yt/cpp/mapreduce/examples/typed-python-tutorial/grep).

## Продвинутый уровень { #advanced }

### Batch запросы { #batch_queries }

Существует возможность исполнять "лёгкие" запросы (создать/удалить таблицу, проверить её существование и так далее) группами. Разумно пользоваться данным способом, если необходимо выполнить большое количество однотипных операций, batch запросы могут заметно сэкономить время выполнения.

Пример находится в [yt/cpp/mapreduce/examples/python-tutorial/batch_client](https://github.com/ytsaurus/ytsaurus/tree/main/yt/cpp/mapreduce/examples/python-tutorial/batch_client).

### Использование RPC { #rpc }

Использование RPC с помощью CLI.

```bash
yt list / --proxy cluster_name --config '{backend=rpc}'
cooked_logs
home
...
```

Аналогичный пример (полный код в [yt/cpp/mapreduce/examples/python-tutorial/simple_rpc](https://github.com/ytsaurus/ytsaurus/tree/main/yt/cpp/mapreduce/examples/python-tutorial/simple_rpc)) можно соорудить из питона (в аркадийной сборке):

Обратите внимание на дополнительный `PEERDIR(yt/python/client_with_rpc)` в `ya.make`.

C помощью RPC можно работать с динамическими таблицами.
Пример находится в [yt/cpp/mapreduce/examples/python-tutorial/dynamic_tables_rpc](https://github.com/ytsaurus/ytsaurus/tree/main/yt/cpp/mapreduce/examples/python-tutorial/dynamic_tables_rpc).

### Указание типов строк с помощью prepare_operation { #prepare_operation }

Помимо использования type hints, для указания Python-типов строк таблиц можно определить метод `prepare_operation`, где все типы указываются с использованием специальных методов. При наличии в классе джоба метода `prepare_operation` библиотека будет использовать типы, указанные внутри метода, и попытка вывести типы строк из type hints производиться не будет.

Пример находится в [yt/cpp/mapreduce/examples/typed-python-tutorial/prepare_operation](https://github.com/ytsaurus/ytsaurus/tree/main/yt/cpp/mapreduce/examples/typed-python-tutorial/prepare_operation).

## Разные примеры { #misc }

### Датаклассы { #dataclass }

В данном примере более подробно продемонстрированы возможности и особенности работы с [классами данных](userdoc.md#dataclass).

Пример находится в [yt/cpp/mapreduce/examples/typed-python-tutorial/dataclass](https://github.com/ytsaurus/ytsaurus/tree/main/yt/cpp/mapreduce/examples/typed-python-tutorial/dataclass).

### Контекст и управление записью в выходные таблицы { #table_switches }

Для выбора, в какую выходную таблицу записать строку, необходимо использовать класс-обёртку `OutputRow`, а именно аргумент `table_index` его конструктора. Имея любой итератор на классы данных (который вернулся из `read_table_structured()` или передан в метод `__call__()` джоба), с помощью метода `.with_context()` можно сделать из него итератор на пары `(row, context)`. Объект `context` имеет методы `.get_table_index()`, `.get_row_index()` и `.get_range_index()`.

При написании класса джоба, в метод `__call__()` которого передаётся не итератор, а отдельная строка (например, mapper-ы), можно добавить к классу декоратор `@yt.wrapper.with_context`, в таком случае метод `__call__()` должен принимать третьим аргументом `context`  (смотрите [документацию](../../../api/python/userdoc.md#python_decorators)).

В reducer, mapper-агрегаторах и при чтении таблиц — всегда, когда есть итератор на строки, используйте метод `with_context` итератора.

Пример находится в [yt/cpp/mapreduce/examples/typed-python-tutorial/table_switches](https://github.com/ytsaurus/ytsaurus/tree/main/yt/cpp/mapreduce/examples/typed-python-tutorial/table_switches).

### Spec builders { #spec_builder }

Используйте [spec builder](../../../api/python/userdoc.md#spec_builder) для описания спецификации операций чтобы избегать ошибок.

Пример находится в [yt/cpp/mapreduce/examples/typed-python-tutorial/spec_builder](https://github.com/ytsaurus/ytsaurus/tree/main/yt/cpp/mapreduce/examples/typed-python-tutorial/spec_builder).

### Использование gevent { #gevent }

Документация содержится [в отдельном разделе](../../../api/python/userdoc.md#gevent).
Пример находится в [yt/cpp/mapreduce/examples/python-tutorial/gevent](https://github.com/ytsaurus/ytsaurus/tree/main/yt/cpp/mapreduce/examples/python-tutorial/gevent).
