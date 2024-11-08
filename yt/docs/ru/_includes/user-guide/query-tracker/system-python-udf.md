# Python User Defined Functions (UDFs) в YQL

## Введение

Не все задачи можно выразить в чисто декларативном стиле, как в SQL. Иногда удобнее описать обработку данных в императивном стиле. Для этого YQL поддерживает создание и использование пользовательских функций (UDF).

## Python UDF

Python UDF позволяет описывать функции на языке Python в самом запросе и использовать их.

## Использование

Для использования Python UDF в YQL запросе нужно выполнить три шага:

1. Включить в запрос строку с Python-скриптом, в котором определена функция для вызова в качестве UDF.
2. Объявить в запросе имя функции и её сигнатуру (типы входных и выходных данных).
3. Вызвать функцию в теле запроса, передав ей необходимые аргументы.

Каждый из этих шагов подробно описан в следующих разделах.

### Написание Python скрипта

Скрипт можно задать тремя способами:

- Обычный строковый литерал. Например: `$script = "def foo(): return 'bar'"`.
- Многострочный строковый литерал (расширение YQL). Такой литерал ограничивается символами `@@` и работает аналогично тройным кавычкам в Python. Комментарий `#py` включает подсветку синтаксиса Python в редакторе YQL:

  ``` yql
  $script = @@#py
  def foo():
      return b'bar'
  @@
  ```

- Именованный файл, приложенный к запросу. Содержимое файла встраивается в запрос с помощью функции [FileContent](../../../yql/builtins/basic.md#file-content-path): `$script = FileContent("foo.py")`.

### Объявление имени и сигнатуры функции

Поскольку Python является динамически типизированным языком, а YQL — статически типизированным, необходимо заранее зафиксировать типы на их стыке. Имя и сигнатура вызываемой из Python-скрипта функции должны быть известны до выполнения YQL запроса. Если типы, объявленные в сигнатуре, не будут соответствовать фактическим данным, возвращаемым функцией, это приведет к ошибке во время исполнения.

Синтаксис объявления имени и сигнатуры функции рассмотрим на примере:
`$f = SystemPython3_8::foo(Callable<()->String>, $script);`:

- `$f` и `$script` — [именованные выражения YQL](../../../yql/syntax/expressions.md#named-nodes)
  - `$f` — готовая для использования функция.
  - `$script` — скрипт на Python из предыдущего раздела.
- `SystemPython3_8::` — фиксированный префикс для объявления функции на Python. `System` означает, что Python будет взят из окружения во время исполнения. `3_8` указывает на версию Python, которая будет использована.
  - На момент написания доступны версии `3_8` - `3_12`.
- `foo` — имя функции внутри скрипта `$script`, которая будет вызываться.
- `Callable<()->String>` — описание сигнатуры функции. Пустые скобки означают отсутствие аргументов, а `String` после стрелки — строковый результат. Пример сигнатуры функции с аргументами: `Callable<(String, Uint32)->Double>`.

### Использование UDF в запросе

Полученную после объявления сигнатуры функцию можно вызывать так же, как и любую встроенную функцию YQL. Например: `$f()` или `$udf("test", 123)`.

## Типы данных

Доступные типы данных для указания в сигнатуре функций можно помотреть в разделе [Типы данных YQL](../../../yql/types/index.md).

### Контейнеры

Контейнеры преобразуются в Python объекты по следующим правилам:

| **Название** | **Объявление сигнатуры** | **Пример сигнатуры** | **Представление в Python** |
| --- | --- | --- | --- |
| Список | `List<Type>` | `List<Int32>` | `list-like object` [(подробнее)](#python-container-wrappers) |
| Словарь | `Dict<KeyType,ValueType>` | `Dict<String,Int32>` | `dict-like object` [(подробнее)](#python-container-wrappers) |
| Кортеж | `Tuple<Type1,...,TypeN>` | `Tuple<Int32,Int32>` | `tuple` |
| Структура | `Struct<Name1:Type1,...,NameN:TypeN>` | `Struct<Name:String,Age:Int32>` | `StructSequence` |
| Поток | `Stream<Type>` | `Stream<Int32>` | `generator` |
| Вариант над кортежем | `Variant<Type1,Type2>` | `Variant<Int32,String>` | `tuple с индексом и объектом` |
| Вариант над структурой | `Variant<Name1:Type1,Name2:Type2>` | `Variant<value:Int32,error:String>` | `tuple с именем поля и объектом` |

Контейнеры можно вкладывать друг в друга. Пример: `List<Tuple<Int32,Int32>>`.

### Особенности передаваемых в функции объектов для списков и словарей {#python-container-wrappers}

При использовании `List` и `Dict`, в аргументы функции передаются специальные read-only объекты `yql.TList` и `yql.TDict`.

Их особенности:

- Read-only: в них нельзя что-то поменять не скопировав, так как тем же объектом может пользоваться какая-то другая часть текущего запроса, например соседняя функция.
- Ценой потенциально медленного копирования можно получить из них настоящий объект типа list, проитерировавшись для частичного копирования. Для словарей нужно дополнительно вызвать `iteritems()`. Установив значение атрибута `_yql_lazy_input` в False на самой функции можно включить автоматическое копирование списков и словарей в list и dict. Данный механизм работает рекурсивно для вложенных контейнеров.
- Для получения количества элементов можно вызвать `len(my_arg)`.
- Актуальный набор доступных методов можно узнать с помощью вызова `dir(my_arg)`. [Пример](#tlist-methods).

Методы `yql.TList`:

- `has_fast_len()` — можно ли быстро получить длину. Если вернулось False, то список "ленивый".
- `has_items()` — проверка на пустоту.
- `reversed()` — получить перевернутую копию списка.
- `skip(n)` и `take(n)` — аналоги срезов `[n:]` и `[:n]`, соответственно.
- `to_index_dict()` - получить словарь с номерами элементов в качестве ключей, что позволяет осуществлять доступ по индексу.

Пример с `_yql_lazy_input`:

```yql
$u = SystemPython3_8::list_func(Callable<(List<Int32>)->Int32>, @@#py
def list_func(lst):
  return lst.count(1)
list_func._yql_lazy_input = False
@@);
SELECT $u(AsList(1,2,3));
```

## Изменение окружения {#environment}

### Взаимодействие с Python из окружения

`SystemPython` UDF будет вызываться в отдельной операции в рамках пользовательского скрипта.

Взаимодействие с Python происходит через динамическую библиотеку `libpython3.N.so`, линковка с которой происходит во время исполнения. Подмена этой библиотеки в окружении приведёт к изменению используемого Python и доступных библиотек.

Изменение окружения из YQL операции возможно при помощи прагм [yt.DockerImage](../../../yql/syntax/pragma.md#ytdockerimage) и [yt.LayerPaths](../../../yql/syntax/pragma.md#ytlayerpaths).

Они повлияют на соответствующие параметры пользовательского скрипта, описание которых можно посмотреть в разделе [Настройки операций - Параметры пользовательского скрипта](../../../user-guide/data-processing/operations/operations-options.md#user_script_options).

{% note info "Окружение по умолчанию" %}

По умолчанию используется дефолтное окружение для джобов, настраиваемое администратором кластера. Оно может не содержать Python требуемой версии.

{% endnote %}

Пример использования библиотеки tensorflow с помощью `pragma yt.DockerImage`:

```yql
pragma yt.DockerImage = "docker.io/tensorflow/tensorflow:2.16.1";

$script = @@#py
import sys
import tensorflow

def foo():
    return "Python version: " + sys.version + ".\nTensorflow version: " + tensorflow.__version__
@@;

$udf = SystemPython3_11::foo(Callable<()->String>, $script);
SELECT $udf();
-- Python version: 3.11.0rc1 (main, Aug 12 2022, 10:02:14) [GCC 11.2.0].
-- Tensorflow version: 2.16.1
```

## Особенности

При использовании UDF стандартный вывод (stdout) используется для служебных целей, поэтому пользоваться им нельзя.

## Примеры

### Hello World

``` yql
$script = @@#py
def hello(name):
    return b'Hello, %s!' % name
@@;

$udf = SystemPython3_8::hello(Callable<(String)->String>, $script);

SELECT $udf("world"); -- "Hello, world!"
```

### Конкатенация строки и числа

```yql
$script = @@#py
def concat(a, b):
    return a.decode("utf-8") + str(b)
@@;
$concat = SystemPython3_8::concat(Callable<(String?, Int64?)->String>, $script);

SELECT $concat(name, age) FROM `//tmp/sample`;
```

### Получение актуального списка методов `yql.TList` {#tlist-methods}

```yql
$u = SystemPython3_8::get_dir(Callable<(List<Int32>)->List<String>>, @@#py
def get_dir(lst):
  return dir(lst)
@@);
SELECT $u(AsList());
```

### Вызов Llama

Необходимо правильное окружение, которое может быть получено с помощью следующего Dockerfile:

```dockerfile
FROM ollama/ollama:0.3.9
RUN apt install -y python3.11 libpython3.11 python3-pip
RUN python3.11 -m pip install ollama
```

Используя указанный образ, можно вызывать Llama из Python:

```yql
pragma yt.DockerImage = "my.docker.registry/ollama/ollama:0.3.9-with-python3.11-2";
pragma yt.DefaultMemoryLimit = "30G";
pragma yt.OperationSpec = "{max_failed_job_count=1;mapper={cpu_limit=64}}";

$script = @@#py
from ollama import Client
import subprocess

def infer_llama(prompt):
    proc = subprocess.Popen(["/bin/ollama", "serve"], stdout=subprocess.DEVNULL)
    client = Client(host='http://localhost:11434')
    client.pull('llama3.1')
    response = client.chat(model='llama3.1', messages=[
        {
            'role': 'user',
            'content': str(prompt),
        },
    ])
    proc.kill()
    return response['message']['content']
@@;
$infer_llama = SystemPython3_11::infer_llama(Callable<(String?)->String>, $script);

select $infer_llama("Why is the sky blue?");
```

## FAQ

### Я получаю ошибку `Module not loaded for script type: SystemPython3_8`

Это означает, что функциональность не поддержана на кластере. На кластере должны присутствовать QueryTracker и YqlAgents с минимальной версией образа [YTsaurus QueryTracker 0.0.8](https://github.com/ytsaurus/ytsaurus/releases/tag/docker%2Fquery-tracker%2F0.0.8). Попросите администраторов кластера обновить данные компоненты.

### Я получаю ошибку `libpython3.8.so.1.0: cannot open shared object file: No such file or directory`

Это означает, что в окружении джоба, исполняющего функцию, нет динамической библиотеки `libpython3.8.so.1.0`. Необходимо использовать ту версию Python, которая присутствует в окружении джоба. [Подробнее](#environment).

Если используется `pragma DockerImage`, то необходимо убедиться, что кластер поддерживает такой способ изменения окружения. [Подробнее](../../../admin-guide/prepare-spec.md#job-environment).
