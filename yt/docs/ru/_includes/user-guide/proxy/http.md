# HTTP-прокси

{% note info "Примечание" %}

Для понимания изложенного в разделе материала требуется знание устройства [HTTP протокола](https://ru.wikipedia.org/wiki/HTTP), принципов работы через HTTP, основ [хранения данных](../../../user-guide/storage/cypress.md) в системе {{product-name}}.

{% endnote %}

В данном разделе показан принцип работы с {{product-name}} через HTTP-прокси — на примере запуска задачи [Word Count](https://en.wikipedia.org/wiki/Word_count).

Word Count — это типичная задача, которую считают в любой системе [MapReduce](../../../user-guide/data-processing/operations/mapreduce.md). Общая схема следующая:

1. По исходному тексту выполняется [Map](../../../user-guide/data-processing/operations/map.md) операция, выдающая для каждого слова пару `(<слово>, 1)`.
2. Результат сортируется по первой координате.
3. По первой координате выполняется [Reduce](../../../user-guide/data-processing/operations/reduce.md) операция, суммирующая вторую координату. На выходе получается набор пар `(<слово>, <количество упоминаний слова>)`.

Полное описание всех доступных команд {{product-name}} можно найти в разделе [Команды](../../../api/commands.md).

## Перед началом работы { #prerequisites }
{% if audience == "internal" %}
Все операции из этого раздела будут выполняться на кластере `{{proxy-pages.http.cluster-name}}` в директории `//tmp/`. Для запуска примеров вам понадобятся:

- Сетевой доступ к кластеру, а также доступ для работы с данными в `//tmp/`. У штатных сотрудников все необходимые доступы есть по умолчанию, а для сотрудников КПБ доступы нужно будет получить. Подробнее в разделе [Настройка доступа к данным](../../../user-guide/problems/howtoreport.md#setevye-dostupy).

- Авторизационный токен — его нужно будет передавать во всех запросах к системе {{product-name}}. Как получить токен, написано в разделе [Аутентификация](../../../user-guide/storage/auth).

- Командная оболочка Bash с установленной утилитой [curl](https://en.wikipedia.org/wiki/CURL) — она понадобится для работы с {{product-name}} через HTTP. В некоторых примерах также будет использоваться утилита [jq](https://jqlang.github.io/jq/) — чтобы сделать вывод команд более читабельным.

{% else %}

- Для доступа к системе {{product-name}} вам понадобится токен. Как его получить — написано в разделе [Аутентификация](../../../user-guide/storage/auth).

- Примеры рассчитаны для запуска в командной оболочке Bash. У вас должна быть установлена утилита [curl](https://en.wikipedia.org/wiki/CURL) — она понадобится для работы с {{product-name}} через HTTP. В некоторых примерах также будет использоваться утилита [jq](https://jqlang.github.io/jq/) — чтобы сделать вывод команд более читабельным.

{% endif %}

## Установка переменных окружения { #export-vars }

{% if audience == "internal" %}

```bash
export YT_PROXY={{proxy-pages.http.light-proxy}}
export YT_HOME=//tmp/yt_examples_$USER
export YT_TOKEN=`cat ~/.yt/token`
```

Следует иметь в виду — в директории `//tmp/` данные хранятся только некоторое время. На кластере `{{proxy-pages.http.cluster-name}}` это несколько часов — после этого созданная вами папка `{{proxy-pages.http.tmp-dir}}` будет удалена. На других кластерах время хранения может отличаться.

{% else %}

```bash
export YT_PROXY=<cluster-host>
export YT_HOME=//tmp/yt_examples
export YT_TOKEN=`cat ~/.yt/token`
```

{% endif %}

## Создание исходной таблицы {#prepare}

Прежде всего необходимо создать рабочий каталог, в котором будут размещаться данные для нашего примера. Для создания каталогов в Кипарисе используется команда [create](../../../api/commands.md#create):

```bash
# Создание рабочего каталога
$ curl -v -X POST "http://$YT_PROXY/api/v4/create?path=$YT_HOME&type=map_node" \
    -H "Accept: application/json" -H "Authorization: OAuth $YT_TOKEN"

# Для удобства восприятия в примерах удалена
# несущественная и дублирующаяся информация
< HTTP/1.1 200 OK
"0-3c35f-12f-8c397340"
```

Данные в системе {{product-name}} хранятся в [таблицах](../../../user-guide/storage/static-tables.md), поэтому  необходимо создать таблицу:

```bash
# Создание таблицы — в ней будут храниться исходные данные
$ curl -v -X POST "http://$YT_PROXY/api/v4/create?path=$YT_HOME/input&type=table" \
    -H "Accept: application/json" -H "Authorization: OAuth $YT_TOKEN"

< HTTP/1.1 200 OK
"0-6a7-191-8075d1f7"
```

В результате выполнения команд в ответ возвращаются идентификаторы (GUID) созданных объектов в Кипарисе: `0-3c35f-12f-8c397340` и `0-6a7-191-8075d1f7`.

{% if audience == "internal" %}
{% note info %}

{{proxy-pages.http.http-note}}

{% endnote %}

{% endif %}

## Загрузка данных в таблицу { #upload }

Теперь в таблицу необходимо загрузить данные. В качестве формата загружаемых данных будет использоваться tab-separated формат:

- строки таблицы отделяются друг от друга переводом строки ( `\n` );
- колонки отделяются друг от друга табуляцией ( `\t` );
- имя колонки и содержимое колонки отделяются друг от друга знаком равенства.

К примеру, строка: `lineno=1\tsize=6\tvalue=foobar` описывает строку с колонками *lineno*, *size* и *value* со значениями, соответственно, *1*, *6* и *foobar*. Символы табуляции экранируются.

Ниже приведён пример подготовки исходных данных:

```bash
# Загрузка текста
$ curl http://lib.ru/BULGAKOW/whtguard.txt | iconv -f koi8-r -t utf-8 > source.txt
```
```bash
# Приведение текста к нужному формату
$ cat source.txt | perl -e '
    $n = 0;
    while(<>) {
      $n++; chomp; s/\t/\\t/g; print "lineno=$n\ttext=$_\n";
    }
  ' > source.tsv
```

Для записи данных в таблицу используется команда [write_table](../../../api/commands.md#write_table). Обратите внимание — запросы на загрузку данных выполняются не через обычную прокси `YT_PROXY`, а через [тяжёлую прокси](#get-data-proxy-list):

```bash
# Получение тяжёлой прокси
$ HEAVY_YT_PROXY=$(curl -s -H "Accept: text/plain" "http://$YT_PROXY/hosts" | head -n1)

# Загрузка данных в таблицу
$ curl -v -X PUT "http://$HEAVY_YT_PROXY/api/v4/write_table?path=$YT_HOME/input" \
    -H "Accept: application/json" -H "Authorization: OAuth $YT_TOKEN" \
    -H "Content-Type: text/tab-separated-values" \
    -H "Transfer-Encoding: chunked" \
    -T source.tsv

< HTTP/1.1 200 OK
```

{% note info "Примечание" %}

Обратите внимание, что в данном запросе также явно указан формат входных данных — "text/tab-separated-values".

{% endnote %}

Чтобы удостовериться в том, что данные записаны, необходимо посмотреть на атрибуты таблицы:

```bash
# Получение атрибутов таблицы
$ curl -v -X GET "http://$YT_PROXY/api/v4/get?path=$YT_HOME/input/@" \
    -H "Accept: application/json" -H "Authorization: OAuth $YT_TOKEN" | jq
```
```bash
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

Не стоит пугаться `Null`-значений. `Null` не значит, что соответствующих данных нет. `Null` значит, что либо по данному ключу находится узел специального типа (например, таблица или файл), либо что данные лениво вычисляемые и за ними нужно обратиться явно.

{% endnote %}

{% note info "Примечание" %}

Чтобы загрузить данные в другом формате, например в JSON, необходимо добавить немного больше опций. Подробности можно прочитать в разделе [Форматы](../../../user-guide/storage/formats.md#json).

{% endnote %}

{% cut "Пример загрузки данных в формате JSON" %}

```bash
$ cat test.json
{ "color": "красный", "value": "#f00" }
{ "color": "red", "value": "#f00" }

$ curl -X POST "http://$YT_PROXY/api/v4/create?path=//tmp/test-json&type=table" \
       -H "Accept: application/json" \
       -H "Authorization: OAuth $YT_TOKEN"

$ curl -X PUT "http://$HEAVY_YT_PROXY/api/v4/write_table?path=//tmp/test-json&encode_utf8=false"
          -H "Accept: application/json" -H "Authorization: OAuth $YT_TOKEN"
          -H "Content-Type: application/json"
          -H "Transfer-Encoding: chunked"
          -H "X-YT-Header-Format: <format=text>yson"
          -H "X-YT-Input-Format: <encode_utf8=%false>json"
          -T test.json
```

{% endcut %}

### Получение тяжёлых прокси {#get-data-proxy-list}

При написании своего клиента пользователю необходимо эпизодически запрашивать список [тяжёлых прокси](../../../user-guide/proxy/http-reference.md#hosts) через запрос `/hosts`. В ответе возвращается упорядоченный по приоритету список тяжёлых прокси. Приоритет прокси определяется динамически и зависит от её загрузки (CPU+I/O). Хорошая стратегия — раз в минуту или раз в несколько запросов перезапрашивать список `/hosts` и менять текущую прокси, к которой задаются запросы.

```bash
HEAVY_YT_PROXY=$(curl -s -H "Accept: text/plain" "http://$YT_PROXY/hosts" | head -n1)
```

## Загрузка файла

Для запуска операции [Map](../../../user-guide/data-processing/operations/map.md) или [Reduce](../../../user-guide/data-processing/operations/reduce.md) потребуется загрузить в систему скрипт, который нужно будет исполнить. Скрипт для данной задачи размещен {%if audience == "public" %}на{% else %}в{% endif %} {{proxy-pages.http.mapreduce-example}}. Пусть локально он сохранён в файл `exec.py`.

Для загрузки файлов в систему {{product-name}} используется команда [write_file](../../../api/commands.md#write_table). Перед загрузкой необходимо создать узел типа «файл», аналогично созданию таблицы на предыдущем шаге:

```bash
# Создание узла с типом «файл»
$ curl -v -X POST "http://$YT_PROXY/api/v4/create?path=$YT_HOME/exec.py&type=file" \
    -H "Accept: application/json" -H "Authorization: OAuth $YT_TOKEN"

< HTTP/1.1 200 OK
"0-efd-190-88fec34"
```
```bash
# Загрузка содержимого в файл
$ curl -v -X PUT "http://$HEAVY_YT_PROXY/api/v4/write_file?path=$YT_HOME/exec.py" \
    -H "Transfer-Encoding: chunked" -H "Authorization: OAuth $YT_TOKEN" \
    -H "Accept: application/json" \
    -T exec.py

< HTTP/1.1 100 Continue
< HTTP/1.1 200 OK
```

Для того чтобы у файла были права на исполнение, необходимо установить для него атрибут *executable*:

```bash
# Установка атрибутов
$ curl -v -X PUT "http://$YT_PROXY/api/v4/set?path=$YT_HOME/exec.py/@executable" \
    -H "Accept: application/json" -H "Authorization: OAuth $YT_TOKEN" \
    -H "Content-Type: application/json" \
    --data-binary '"true"'
```

Для проверки корректности загруженного файла можно его прочитать:

```bash
# Чтение загруженного файла
$ curl -v -X GET "http://$HEAVY_YT_PROXY/api/v4/read_file?path=$YT_HOME/exec.py" \
    -H "Accept: application/json" -H "Authorization: OAuth $YT_TOKEN"

< HTTP/1.1 200 OK
#!/usr/bin/python

import re
import sys
import itertools
import operator
...
```

Если необходимо прочесть диапазон байт из файла, можно использовать дополнительные параметры `offset` и `length` команды `read_file`. Для разнообразия в примере ниже параметры передаются через заголовок `X-YT-Parameters`:

```bash
# Чтение диапазона байт из файла
$ curl -v -X GET "http://$HEAVY_YT_PROXY/api/v4/read_file" \
    -H "Authorization: OAuth $YT_TOKEN" \
    -H "X-YT-Parameters: {\"path\": \"$YT_HOME/exec.py\", \"offset\": 11, \"length\": 6}"

< HTTP/1.1 200 OK
python
```

## Запуск Map операции { #launch_map }

Важный момент, который стоит учитывать: требование существования выходной таблицы **до** запуска операции. Создадим таблицу с помощью команды [create](../../../api/commands.md#create):

```bash
# Создание выходной таблицы
$ curl -v -X POST "http://$YT_PROXY/api/v4/create?path=$YT_HOME/output_map&type=table" \
    -H "Accept: application/json" -H "Authorization: OAuth $YT_TOKEN"

< HTTP/1.1 200 OK
"0-843-191-ae506abe"
```

Теперь можно запустить операцию Map. Для этого следует воспользоваться командой [start_operation](../../../api/commands.md#start_operation) с параметром `operation_type`, равным `map`. Обратите внимание на спецификацию операции: запуск операции `map` требует задания структурированного набора аргументов. В HTTP-интерфейсе системы кодировать аргументы можно как через query string (что и происходило в примерах выше, см. аргументы *path* и *type*), так и в теле запроса для POST-запросов. Пример описывает спецификацию для запуска в формате JSON:

```bash
# Запуск Map операции
$ cat <<END | curl -v -X POST "http://$YT_PROXY/api/v4/start_operation" \
    -H "Accept: application/json" -H "Authorization: OAuth $YT_TOKEN" \
    -H "Content-Type: application/json" \
    --data-binary @-
{
  "operation_type": "map",
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
```
```bash
< HTTP/1.1 200 OK
"381eb9-8eee9ec2-70a8370d-ea39f666"
```

{% note info "Примечание" %}

В /api/v3 операции запускаются несколько [иначе](#operations-v3).

{% endnote %}

{% note info "Примечание" %}

В отличие от операций `set` и `write_file`, возвращаемый GUID операции необходим для отслеживания состояния операции. Успешный ответ HTTP-интерфейса о запуске операции говорит только о том, что операция была запущена, но не о том, что она отработала корректно.

{% endnote %}

Посмотреть статус операции можно через веб-интерфейс или с помощью вызова `get_operation`:

```bash
# Просмотр статуса операции
$ curl -v -X GET "http://$YT_PROXY/api/v4/get_operation?operation_id=381eb9-8eee9ec2-70a8370d-ea39f666" \
    -H "Accept: application/json" -H "Authorization: OAuth $YT_TOKEN"

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

Чтобы убедиться, что операция отработала корректно, можно прочитать результат её работы:

```bash
# Чтение данных
$ curl -v -X GET "http://$HEAVY_YT_PROXY/api/v4/read_table?path=$YT_HOME/output_map" \
    -H "Accept: text/tab-separated-values" -H "Authorization: OAuth $YT_TOKEN" \
  | head

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

Сортировка запускается командой [start_operation](../../../api/commands.md#start_operation) с параметром `operation_type`, равным `sort`. Входная спецификация для [Sort](../../../user-guide/data-processing/operations/sort.md) немного похожа на спецификации для [Map](../../../user-guide/data-processing/operations/map.md) и [Reduce](../../../user-guide/data-processing/operations/reduce.md).

```bash
# Создание выходной таблицы
$ curl -v -X POST "http://$YT_PROXY/api/v4/create?path=$YT_HOME/output_sort&type=table" \
    -H "Accept: application/json" -H "Authorization: OAuth $YT_TOKEN"

< HTTP/1.1 200 OK
"0-95d-191-8901298a"

# Запуск сортировки
$ cat <<END | curl -v -X POST "http://$YT_PROXY/api/v4/start_operation" \
    -H "Accept: application/json" -H "Authorization: OAuth $YT_TOKEN" \
    -H "Content-Type: application/json" \
    --data-binary @-
{
  "operation_type": "sort",
  "spec" : {
    "input_table_paths" : [ "$YT_HOME/output_map" ],
    "output_table_path" : "$YT_HOME/output_sort",
    "sort_by" : [ "word" ]
  }
}
END

< HTTP/1.1 200 OK
"640310-da8d54f1-6eded631-31c91e76"
```

Отслеживать процесс выполнения операции можно через веб-интерфейс.

## Запуск Reduce операции { #launch_reduce }

Запуск Reduce операции выполняется с помощью команды [start_operation](../../../api/commands.md#start_operation) с параметром `operation_type`, равным `reduce`:

```bash
# Создание выходной таблицы
$ curl -v -X POST "http://$YT_PROXY/api/v4/create?path=$YT_HOME/output_reduce&type=table" \
    -H "Accept: application/json" -H "Authorization: OAuth $YT_TOKEN"

< HTTP/1.1 200 OK
"0-95f-191-ba3a4055"

# Запуск Reduce операции
$ cat <<END | curl -v -X POST "http://$YT_PROXY/api/v4/start_operation" \
    -H "Accept: application/json" -H "Authorization: OAuth $YT_TOKEN" \
    -H "Content-Type: application/json" \
    --data-binary @-
{
  "operation_type": "reduce",
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

< HTTP/1.1 200 OK
"658ad8-edf7650f-182e4fd0-a1a0fd03"
```

## Чтение результата { #read_results }

После окончания операции получим подсчитанную статистику по частоте использования слов в «Белой гвардии». Посмотрим на результат её работы:

```bash
# Чтение результата
$ curl -v -X GET "http://$HEAVY_YT_PROXY/api/v4/read_table?path=$YT_HOME/output_reduce" \
    -H "Accept: text/tab-separated-values" -H "Authorization: OAuth $YT_TOKEN" \
  | head

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

## Запуск операций в /api/v3 {#operations-v3}

{% note warning %}

Версия /api/v3 является устаревшей — больше она не развивается. Используйте версию /api/v4.

{% endnote %}

В /api/v3 все операции обработки данных (Map, Reduce, Sort и так далее) запускались не командой `start_operation`, а с помощью одноимённых команд — [map](../../../api/commands.md#map), [reduce](../../../api/commands.md#reduce), [sort](../../../api/commands.md#sort). Например, так выглядит запуск Map операции:

```bash
# Запуск Map операции в /api/v3
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
```
