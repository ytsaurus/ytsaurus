# HTTP-прокси

В данном разделе содержится демонстрация работы с {{product-name}} через HTTP-прокси, на примере запуска задачи [Word Count](https://en.wikipedia.org/wiki/Word_count).

Полное описание всех команд {{product-name}} можно найти в разделе [Команды](../../../api/commands.md). 

{% note info "Примечание" %}

Для понимания изложенного в разделе материала требуется знание устройства [HTTP протокола](https://ru.wikipedia.org/wiki/HTTP), принципов работы через HTTP, основ [хранения данных](../../../user-guide/storage/cypress.md) в системе {{product-name}}.

{% endnote %}


## Подготовка { #prepare }

Пример Word Count — это типичная задача, которую считают в любой системе [MapReduce](../../../user-guide/data-processing/operations/mapreduce.md). Общая схема следующая: 

1. По исходному тексту выполняется [Map](../../../user-guide/data-processing/operations/map.md) операция, выдающая по каждому слову пару (слово, 1);
2. Результат сортируется по первой координате;
3. По первой координате выполняется [Reduce](../../../user-guide/data-processing/operations/reduce.md) операция, суммирующая вторую координату. На выходе получается набор пар (слово, количество упоминаний слов).

Далее в тексте примера используется утилита [curl](https://en.wikipedia.org/wiki/CURL) для работы через HTTP. Для удобства записи были выставлены переменные окружения, представленные ниже.{% if audience == "internal" %} Подробнее о получение токена можно прочитать в разделе [Начало работы](../../overview/try-yt.md).{% endif %}

Установка переменных окружения:

```bash
$ export YT_PROXY=cluster-name
$ export YT_TOKEN=`cat ~/.yt/token`
$ export YT_HOME=//tmp/yt_examples
```

Для большей наглядности и читаемости примеров в них удалена несущественная и дублирующаяся информация.

## Загрузка данных { #upload }

Данные в системе {{product-name}} хранятся в [таблицах](../../../user-guide/storage/static-tables.md), поэтому необходимо создать таблицу. Таблица содержит две колонки: номер строки и содержимое строки. Для создания таблицы используется команда [create](../../../api/commands.md#create).

Создание каталога и таблицы:

```bash
# Создание рабочего каталога
$ curl -v -X POST "http://$YT_PROXY/api/v3/create?path=$YT_HOME&type=map_node" \
    -H "Accept: application/json" -H "Authorization: OAuth $YT_TOKEN"
> POST /api/v3/create?path=//tmp/yt_examples&type=map_node HTTP/1.1

< HTTP/1.1 200 OK
"0-3c35f-12f-8c397340"

# Создание таблицы для исходных данных
$ curl -v -X POST "http://$YT_PROXY/api/v3/create?path=$YT_HOME/input&type=table" \
    -H "Accept: application/json" -H "Authorization: OAuth $YT_TOKEN"
> POST /api/v3/create?path=//tmp/yt_examples/input&type=table HTTP/1.1

< HTTP/1.1 200 OK
"0-6a7-191-8075d1f7"
```

 В результате выполнения команд в ответ возвращаются идентификаторы (GUID) созданных объектов в Кипарисе: `0-3c35f-12f-8c397340` и `0-6a7-191-8075d1f7`.

Теперь в таблицу необходимо загрузить данные, для этого используется команда [write_table](../../../api/commands.md#write_table). В качестве формата данных используется tab-separated формат: 

- строки таблицы отделяются друг от друга переводом строки ( `\n` );
- колонки отделяются друг от друга табуляцией ( `\t` );
- имя колонки и содержимое колонки отделяются друг от друга  знаком равенства.

К примеру, строка: `lineno=1\tsize=6\tvalue=foobar` описывает строку с колонками *lineno*, *size* и *value* со значениями, соответственно, *1*, *6* и *foobar*. Символы табуляции экранируются.

Загрузка данных в таблицу:

```bash
# Скачивание текста
$ wget -O - http://lib.ru/BULGAKOW/whtguard.txt | iconv -f cp1251 -t utf-8 > source.txt

# Переформатирование и загрузка текста
$ cat source.txt | perl -e '
    $n = 0;
    while(<>) {
      $n++; chomp; s/\t/\\t/g; print "lineno=$n\ttext=$_\n";
    }
  ' > source.tsv

$ HEAVY_YT_PROXY=$(curl -s -H "Accept: text/plain" "http://$YT_PROXY/hosts" | head -n1)
$ curl -v -L -X PUT "http://$HEAVY_YT_PROXY/api/v3/write_table?path=$YT_HOME/input" \
    -H "Accept: application/json" -H "Authorization: OAuth $YT_TOKEN" \
    -H "Content-Type: text/tab-separated-values" \
    -H "Transfer-Encoding: chunked" \
    -T source.tsv
> PUT /api/v3/write_table?path=//tmp/yt_examples/input HTTP/1.1
> Expect: 100-continue

< HTTP/1.1 100 Continue
< HTTP/1.1 200 OK
```

{% note info "Примечание" %}

Обратите внимание, что в данном запросе также явно указан формат входных данных — "text/tab-separated-values".

{% endnote %}

{% note info "Примечание" %}

При написании своего клиента пользователю необходимо эпизодически запрашивать список тяжелых прокси через запрос `/hosts`. В ответе возвращается упорядоченный по приоритету список тяжелых прокси. Приоритет прокси определяется динамически и зависит от ее загрузки (CPU+I/O). Хорошая стратегия — раз в минуту или раз в несколько запросов перезапрашивать список `/hosts` и менять текущую прокси, к которой задаются запросы.

{% endnote %}

Получение списка тяжелых прокси:

```bash
$ curl -v -X GET "http://$YT_PROXY/hosts"
> GET /hosts HTTP/1.1

< HTTP/1.1 200 OK
< Content-Type: application/json
<
["n0008-sas.cluster-name","n0025-sas.cluster-name",...]
```

Чтобы удостовериться в том, что данные записаны, необходимо посмотреть на атрибуты таблицы.

Получение атрибутов таблицы:

```bash
$ curl -v -X GET "http://$YT_PROXY/api/v3/get?path=$YT_HOME/input/@" \
    -H "Accept: application/json" -H "Authorization: OAuth $YT_TOKEN"
> GET /api/v3/get?path=//tmp/yt_examples/input/@ HTTP/1.1

< HTTP/1.1 200 OK
{
  ...
  "uncompressed_data_size" : 993253,
  "compressed_size" : 542240,
  "compression_ratio" : 0.54592334480741566693,
  "row_count" : 8488,
  "sorted" : "false",
  ...
  "channels" : [ ["lineno"], ["text"] ]
}
```

{% note info "Примечание" %}

Не стоит пугаться null-значений. null не значит, что соответствующих данных нет. null значит, или что по данному ключу находится или узел специального типа (например, таблица или файл), или что данные лениво вычисляемые, и за ними нужно явно обратиться (случай выше).

{% endnote %}

{% note info "Примечание" %}

Чтобы загрузить, например, JSON, необходимо добавить немного больше опций. Подробности можно прочитать в разделе [Форматы](../../../user-guide/storage/formats.md#json).

{% endnote %}

Загрузка данных в формате JSON:

```bash
$ cat test.json 
{ "color": "красный", "value": "#f00" }
{ "color": "red", "value": "#f00" }

$ curl -X POST "http://$YT_PROXY/api/v3/create?path=//tmp/test-json&type=table" \
       -H "Accept: application/json" \
       -H "Authorization: OAuth $YT_TOKEN"

$ curl -L -X PUT "http://$HEAVY_YT_PROXY/api/v3/write_table?path=//tmp/test-json&encode_utf8=false" 
          -H "Accept: application/json" -H "Authorization: OAuth $YT_TOKEN" 
          -H "Content-Type: application/json" 
          -H "Transfer-Encoding: chunked" 
          -H "X-YT-Header-Format: <format=text>yson" 
          -H "X-YT-Input-Format: <encode_utf8=%false>json" 
          -T test.json
```

## Загрузка файлов

Для запуска операции [Map](../../../user-guide/data-processing/operations/map.md) или [Reduce](../../../user-guide/data-processing/operations/reduce.md) потребуется загрузить в систему скрипт, который нужно будет исполнить. Скрипт для данной задачи размещен на [GitHub](https://github.com/ytsaurus/ytsaurus/tree/main/yt/cpp/mapreduce/examples/python-misc/http_proxy.py) (пусть локально он сохранен в файл `exec.py` ). Для загрузки файлов в систему {{product-name}} используется команда [write_file](../../../api/commands.md#write_table). Перед загрузкой необходимо создать узел типа «файл», аналогично созданию таблицы на предыдущем шаге.

Загрузка файлов:

```bash
$ curl -v -X POST "http://$YT_PROXY/api/v3/create?path=$YT_HOME/exec.py&type=file" \
    -H "Accept: application/json" -H "Authorization: OAuth $YT_TOKEN"
> POST /api/v3/create?path=//tmp/yt_examples/exec.py&type=file HTTP/1.1

< HTTP/1.1 200 OK
"0-efd-190-88fec34"

$ curl -v -L -X PUT "http://$YT_PROXY/api/v3/write_file?path=$YT_HOME/exec.py" \
    -H "Transfer-Encoding: chunked" -H "Authorization: OAuth $YT_TOKEN" \
    -T exec.py
> PUT /api/v3/write_file?path=//tmp/yt_examples/exec.py HTTP/1.1
> Expect: 100-continue

< HTTP/1.1 100 Continue
< HTTP/1.1 200 OK
```

Для того, чтобы у файла были права на исполнение, необходимо установить для него атрибут *executable*.

Установка атрибутов:

```bash
$ curl -v -L -X PUT "http://$YT_PROXY/api/v3/set?path=$YT_HOME/exec.py/@executable" \
    -H "Accept: application/json" -H "Authorization: OAuth $YT_TOKEN" \
    -H "Content-Type: application/json" \
    --data-binary '"true"'
> PUT /api/v3/set?path=//tmp/yt_examples/exec.py/@executable HTTP/1.1

< HTTP/1.1 200 OK
```

Для проверки корректности загруженного файла можно его прочитать.

Чтение загруженного файла:

```bash
$ curl -v -L -X GET "http://$YT_HEAVY_PROXY/api/v3/read_file?path=$YT_HOME/exec.py" \
    -H "Accept: application/json" -H "Authorization: OAuth $YT_TOKEN"
> GET /api/v3/read_file?path=//tmp/yt_examples/exec.py HTTP/1.1

< HTTP/1.1 200 OK
#!/usr/bin/python

import re
import sys
import itertools
import operator
...
```

При необходимости прочесть диапазон байт из файла, можно использовать дополнительные параметры *offset* и *length* команды *read_file*. Для разнообразия параметры передаются через заголовок `X-YT-Parameters`.

Чтение диапазона байт из файла:

```bash
$ curl -v -L -X GET "http://$YT_HEAVY_PROXY/api/v3/read_file" \
    -H "Authorization: OAuth $YT_TOKEN" \
    -H "X-YT-Parameters: {\"path\": \"$YT_HOME/exec.py\", \"offset\": 11, \"length\": 6}"
> GET /api/v3/read_file HTTP/1.1

< HTTP/1.1 200 OK
python
```

## Запуск Map операции { #launch_map }

В системе {{product-name}} для запуска Map операции существует одноименная [команда](../../../api/commands.md#map). Важный момент, который стоит учитывать, — требование существования выходной таблицы **до** запуска операции. Таблицу можно создать с помощью команды [create](../../../api/commands.md#create).

Создание выходной таблицы:

```bash
$ curl -v -X POST "http://$YT_PROXY/api/v3/create?path=$YT_HOME/output_map&type=table" \
    -H "Accept: application/json" -H "Authorization: OAuth $YT_TOKEN"
> POST /api/v3/create?path=//tmp/yt_examples/output_map&type=table HTTP/1.1

< HTTP/1.1 200 OK
"0-843-191-ae506abe"
```

Теперь можно запустить операцию Map. Обратим внимание на ее спецификацию: видно, что запуск команды map требует задания структурированного набора аргументов. В HTTP-интерфейсе системы кодировать аргументы можно как через query string (что и происходило в примерах выше, см. аргументы *path* и *type*), так и в теле запроса для POST-запросов. Пример описывает спецификацию для запуска в формате JSON.

Запуск Map операции:

```bash
$ cat <<END | curl -v -X POST "http://$YT_PROXY/api/v3/map" \
    -H "Accept: application/json" -H "Authorization: OAuth $YT_TOKEN" \
    -H "Content-Type: application/json" \
    --data-binary @-
{
  "spec" : {
    "mapper" : {
      "command" : "python exec.py map",
      "file_paths" : [ "$YT_HOME/exec.py" ],
      "format" : "dsv"
    },
    "input_table_paths" : [ "$YT_HOME/input" ],
    "output_table_paths" : [ "$YT_HOME/output_map" ]
  }
}
END
> POST /api/v3/map HTTP/1.1

< HTTP/1.1 200 OK
"381eb9-8eee9ec2-70a8370d-ea39f666"
```

{% note info "Примечание" %}

В отличие от операций `set` и `write_file`, возвращаемый GUID операции необходим для отслеживания состояния операции. Успешный ответ HTTP-интерфейса о запуске операции говорит только о том, что операция была запущена, но не о том что она отработала корректно.

{% endnote %}

Посмотреть статус операции можно через веб-интерфейс или с помощью вызова `get_operation`.

Просмотр статус операции

```bash
$ curl -v -X GET "http://$YT_PROXY/api/v3/get_operation?operation_id=381eb9-8eee9ec2-70a8370d-ea39f666" \
    -H "Accept: application/json" -H "Authorization: OAuth $YT_TOKEN"
> GET /api/v3/get_operation?operation_id=381eb9-8eee9ec2-70a8370d-ea39f666 HTTP/1.1

< HTTP/1.1 200 OK
{
  "operation_type" : "map",
  "progress" : {
    "jobs" : { "total" : 1, "pending" : 0, "running" : 0, "completed" : 1, "failed" : 0 },
    "chunks" : { "total" : 1,"running" : 0, "completed" : 1, "pending" : 0, "failed" : 0 },
    ...
  },
  "state" : "completed"
}
```

Чтобы убедиться, что операция отработала корректно, можно прочитать результат ее работы.

Чтение данных

```bash
$ curl -v -L -X GET "http://$YT_HEAVY_PROXY/api/v3/read_table?path=$YT_HOME/output_map" \
    -H "Accept: text/tab-separated-values" -H "Authorization: OAuth $YT_TOKEN" \
  | head
> GET /api/v3/read?path=//tmp/yt_examples/output_map HTTP/1.1


< HTTP/1.1 202 Accepted
word=html count=1
word=head count=1
word=title  count=1
word=михаил count=1
word=булгаков count=1
word=белая  count=1
word=гвардия  count=1
word=title  count=1
word=head count=1
word=body count=1
```

## Запуск сортировки { #launch_sort }

Сортировка запускается командой [sort](../../../user-guide/data-processing/operations/sort.md), которая по входной спецификации немного похожа на [Map](../../../user-guide/data-processing/operations/map.md) и [Reduce](../../../user-guide/data-processing/operations/reduce.md).

Запуск сортировки:

```bash
# Создание выходной таблицы
$ curl -v -X POST "http://$YT_PROXY/api/v3/create?path=$YT_HOME/output_sort&type=table" \
    -H "Accept: application/json" -H "Authorization: OAuth $YT_TOKEN"
> POST /api/v3/create?path=//tmp/yt_examples/output_map&type=table HTTP/1.1

< HTTP/1.1 200 OK
"0-95d-191-8901298a"

# Запуск сортировки
$ cat <<END | curl -v -X POST "http://$YT_PROXY/api/v3/sort" \
    -H "Accept: application/json" -H "Authorization: OAuth $YT_TOKEN" \
    -H "Content-Type: application/json" \
    --data-binary @-
{
  "spec" : {
    "input_table_paths" : [ "$YT_HOME/output_map" ],
    "output_table_path" : "$YT_HOME/output_sort",
    "sort_by" : [ "word" ]
  }
}
END
> POST /api/v3/sort HTTP/1.1

< HTTP/1.1 200 OK
"640310-da8d54f1-6eded631-31c91e76"
```

За процессом выполнения операции можно следить через веб-интерфейс.

## Запуск Reduce операции { #launch_reduce }

Запуск Reduce операции выполняется с помощью одноименной [команды](../../../api/commands.md#reduce).

Запуск Reduce операции:

```bash
# Создание выходной таблицы
$ curl -v -X POST "http://$YT_PROXY/api/v3/create?path=$YT_HOME/output_reduce&type=table" \
    -H "Accept: application/json" -H "Authorization: OAuth $YT_TOKEN"
> POST /api/v3/create?path=//tmp/yt_examples/output_reduce&type=table HTTP/1.1

< HTTP/1.1 200 OK
"0-95f-191-ba3a4055"

# Запуск reduce
$ cat <<END | curl -v -X POST "http://$YT_PROXY/api/v3/reduce" \
    -H "Accept: application/json" -H "Authorization: OAuth $YT_TOKEN" \
    -H "Content-Type: application/json" \
    --data-binary @-
{
  "spec" : {
    "reducer" : {
      "command" : "python exec.py reduce",
      "file_paths" : [ "$YT_HOME/exec.py" ],
      "format" : "dsv"
    },
    "input_table_paths" : [ "$YT_HOME/output_sort" ],
    "output_table_paths" : [ "$YT_HOME/output_reduce" ],
    "sort_by" : [ "word" ],
    "reduce_by" : [ "word" ]
  }
}
END
> POST /api/v3/reduce HTTP/1.1

< HTTP/1.1 200 OK
"658ad8-edf7650f-182e4fd0-a1a0fd03"
```

## Чтение результата { #read_results }

После окончания операции получим подсчитанную статистика по частоте использования слов в "Белой гвардии".
Посмотрим результат ее работы.

Чтение результата:

```bash
$ curl -v -L -X GET "http://$YT_HEAVY_PROXY/api/v3/read_table?path=$YT_HOME/output_reduce" \
    -H "Accept: text/tab-separated-values" -H "Authorization: OAuth $YT_TOKEN" \
  | head
> GET /api/v3/read_table?path=//tmp/yt_examples/output_reduce HTTP/1.1

< HTTP/1.1 200 OK
count=1 word=0
count=1 word=04
count=1 word=05
count=4 word=1
count=2 word=10
count=4 word=11
count=4 word=12
count=4 word=13
count=3 word=14
count=2 word=15
```

