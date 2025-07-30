# Data classes

{% note warning "Attention" %}

Data classes are only supported in Python 3.

{% endnote %}

The main method used to represent table rows is to create classes with fields marked by types (similar to [dataclasses](https://docs.python.org/3/library/dataclasses.html)). See also the [example](https://github.com/ytsaurus/ytsaurus/tree/main/yt/python/examples/dataclass_typed).

## Motivation { #motivation }

  1. Convenience: since the {{product-name}} scalar type system is richer than in Python, marking by types helps explicitly express the desired column types. The convenience can be seen even better when working with composite types, such as structures.
  2. Strong typing helps you detect errors in the code before they result in incorrect data and other negative effects. To do this, linters and IDEs can be used.
  3. Speed: due to the [Skiff](../../../user-guide/storage/skiff.md) format and some other optimizations, CPU consumption is several times lower than for untyped data.

## Installation

To work with the `yt.wrapper.schema` module, install the packages:

{% if audience == "internal" %}
  * `yandex-yt-yson-bindings`
{% else %}
  * `typing_extensions`
  * `ytsaurus-yson`
  * `six`
{% endif %}

## Introduction { #introduction }

To work with tabular data, you must declare a class with the `yt.wrapper.yt_dataclass` decorator and mark its fields with types. The field type comes after the colon. It can be:
  - Python built-in types: `int`, `float`, `str`, and others.
  - Custom classes with the `@yt_dataclass` decorator.
  - Composite types from the [typing](https://docs.python.org/3/library/typing.html) module. `List`, `Dict`, `Optional`, and `Tuple` are currently supported. We're also planning to add support for a number of other types in the future.
  - Special types from `yt.wrapper.schema`: `Int8`, `Uint16`, `OtherColumns`, and others (a more complete list is given [below](#types)).

Example:

```python
@yt.wrapper.yt_dataclass
class Row:
    id: int
    name: str
    is_robot: bool = False
```

For the class described in this way, a constructor and other service methods will be generated. In particular, `__eq__` and `__repr__`. You can specify a default value for some fields. It will get into the constructor signature. You can create objects of this class as follows: `row = Row(id=123, name="foo")`. For all the fields without default values (such as `robot: bool = False`), you need to pass relevant fields to the constructor. Otherwise, it will throw an exception.

The data classes support inheritance.

When you have a class like this, you can:

  1. Create a table with a relevant schema (you can just start writing to an empty or a non-existent table or use the `TableSchema.from_row_type()` function).
  2. Write and read [tables](../../../api/python/userdoc.md#table_commands), [example](../../../api/python/examples.md#read_write).
  3. Run [operations](../../../api/python/userdoc.md#python_operations), [example](../../../api/python/examples.md#simple_map).

Data classes can also be created based on table schemas explicitly or automatically, by reading structured data.

```python
yt_table_schema = client.get(f"{table_path}/@schema")
dataclass_type = yt.schema.make_dataclass_from_table_schema(yt.schema.TableSchema.from_yson_type(yt_table_schema))

# or

typed_table_data = list(client.read_table_structured(table=table_path, row_type=None))
```


## Special types { #types }

  | `yt.wrapper.schema` | Python | Diagram |
  |-------------------------------|----------------|-------------|
  | `Int8,` `Int16`, `Int32`, `Int64` | `int` | `int8`, `int16`, `int32`, `int64` |
  | `Uint8`, `Uint16`, `Uint32`, `Uint64` | `int` | `uint8`, `uint16`, `uint32`, `uint64` |
  | `YsonBytes` | `bytes` | `yson`/`any` |
  | `OtherColumns` | `OtherColumns` | Corresponds to several columns. |
