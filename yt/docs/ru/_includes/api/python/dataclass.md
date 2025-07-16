# Датаклассы

{% note warning "Внимание" %}

Датаклассы поддерживаются только в Python 3.

{% endnote %}

Основной способ представления строк таблицы — это классы с полями, которые размечены типами (аналог [dataclasses](https://docs.python.org/3/library/dataclasses.html)). См. также [пример](https://github.com/ytsaurus/ytsaurus/tree/main/yt/python/examples/dataclass_typed).

## Мотивация { #motivation }

  1. Удобство: так как система скалярных типов {{product-name}} более богатая, чем в Python, разметка типами помогает явно выразить желаемые типы колонок. Ещё лучше видно удобство при работе со сложными типами, например, структурами.
  2. Строгая типизация позволяет распознавать ошибки в коде до того, как они приведут к появлению некорректных данных и другим неприятным эффектам. В этом также могут помогать линтеры и IDE.
  3. Скорость: за счёт использования формата [Skiff](../../../user-guide/storage/skiff.md) и некоторых других оптимизаций потребление CPU в несколько раз меньше, чем для нетипизированных данных.

## Установка

Для работы с модулем `yt.wrapper.schema` установите пакеты:

{% if audience == "internal" %}
  * `yandex-yt-yson-bindings`
{% else %}
  * `typing_extensions`
  * `ytsaurus-yson`
  * `six`
{% endif %}

## Введение { #introduction }

Для работы с табличными данными нужно объявить класс с декоратором `yt.wrapper.yt_dataclass` и разметить его поля типами. После двоеточия указывается тип поля. Это могут быть:
  - встроенные типы в Python: `int`, `float`, `str` и другие;
  - пользовательские классы с декоратором `@yt_dataclass`;
  - композитные типы из модуля [typing](https://docs.python.org/3/library/typing.html). Сейчас поддерживаются `List`, `Dict`, `Optional`, `Tuple`. В планах поддержка ряда других типов.
  - специальные типы из `yt.wrapper.schema`: `Int8`, `Uint16`, `OtherColumns` и другие (более полный список [ниже](#types)).

Пример:

```python
@yt.wrapper.yt_dataclass
class Row:
    id: int
    name: str
    is_robot: bool = False
```

Для описанного таким образом класса будут сгенерированы конструктор и прочие служебные методы, в частности, `__eq__` и `__repr__`. Для некоторых полей можно указать значение по умолчанию. Оно попадёт в сигнатуру конструктора. Объект данного класса можно создавать обычным образом: `row = Row(id=123, name="foo")`. При этом для всех полей, для которых не указаны значения по умолчанию (как для `robot: bool = False`), необходимо передать соответствующие поля в конструктор, иначе будет порождено исключение.

Для датаклассов поддерживается наследование.

Имея такой класс, можно:

  1. Создать таблицу с соответствующей схемой (можно просто начать писать в пустую или несуществующую таблицу или использовать функцию `TableSchema.from_row_type()`).
  2. Писать и читать [таблицы](../../../api/python/userdoc.md#table_commands), [пример](../../../api/python/examples.md#read_write).
  3. Запускать [операции](../../../api/python/userdoc.md#python_operations), [пример](../../../api/python/examples.md#simple_map).

Также можно создавать датаклассы на основе схемы таблицы явно или автоматически, читая структурированные данные

```python
yt_table_schema = client.get(f"{table_path}/@schema")
dataclass_type = yt.schema.make_dataclass_from_table_schema(yt.schema.TableSchema.from_yson_type(yt_table_schema))

# or

typed_table_data = list(client.read_table_structured(table=table_path, row_type=None))
```


## Специальные типы { #types }

  | `yt.wrapper.schema`    |  Python  | Схема |
  |-------------------------------|----------------|-------------|
  | `Int8,` `Int16`, `Int32`, `Int64`     | `int`            | `int8`, `int16`, `int32`, `int64` |
  | `Uint8`, `Uint16`, `Uint32`, `Uint64` | `int`            | `uint8`, `uint16`, `uint32`, `uint64` |
  | `YsonBytes`                     | `bytes`          | `yson`/`any` |
  | `OtherColumns`                  | `OtherColumns`   | Соответствует нескольким колонкам. |
