# Сопоставление типов данных: SPYT, Spark и {{product-name}}

В этой статье описано сопоставление [типов данных в {{product-name}}](../../../../../user-guide/storage/data-types.md) и [типов данных в Spark](https://spark.apache.org/docs/latest/sql-ref-datatypes.html) при чтении и записи.

## Особенности

1. **Указания типов**: некоторые сопоставления могут быть изменены [указаниями типов](../../../../../user-guide/data-processing/spyt/thesaurus/read-options.md#schema_hint) в метаданных полей, позволяя более точный контроль над преобразованием типов.

2. **Точность Decimal**: {{product-name}} ограничивает точность `decimal` до 35.[Подробнее про Decimal](../../../../../user-guide/storage/data-types.md#schema_decimal)

3. **Строковые типы**: опция конфигурации `stringToUtf8` контролирует, сопоставляются ли строки с типами `string` или `utf8` в {{product-name}}. Пример:
   ```python
   import spyt
   from pyspark.sql import Row
   from pyspark.sql.types import StructType, StructField, StringType, BinaryType

   spark.createDataFrame(
       spark.sparkContext.parallelize([Row("string", b"binary")]),
       StructType([
           StructField("string", StringType(), nullable=False),
           StructField("binary", BinaryType(), nullable=False),
       ])
   ).write \
   .option("string_to_utf8", "true") \
   .yt("//path/to/table")
   ```
   
4. **Составные типы**: сложные типы наподобие структур, массивов и карт рекурсивно сопоставляются с использованием тех же правил для их элементарных типов.

Подробнее про некоторые специальные методы для работы с типами данных можно почитать в статьях [Опции чтения](../../../../../user-guide/data-processing/spyt/thesaurus/read-options.md) и [Опции записи](../../../../../user-guide/data-processing/spyt/thesaurus/write-options.md).

## Сопоставление типов при чтении из {{product-name}}

| Тип {{product-name}} | Тип Spark (Базовый)  | Тип Spark (Расширенный) | Примечания                                                 |
|----------------------|----------------------|-------------------------|------------------------------------------------------------|
| null                 | NullType             | NullType                |                                                            |
| int64                | LongType             | LongType                |                                                            |
| uint64               | DecimalType(20, 0)   | UInt64Type              |                                                            |
| float                | FloatType/DoubleType | FloatType/DoubleType    | Верхний уровень: FloatType, Внутренний уровень: DoubleType |
| double               | DoubleType           | DoubleType              |                                                            |
| boolean              | BooleanType          | BooleanType             |                                                            |
| string               | StringType           | StringType              |                                                            |
| binary               | BinaryType           | BinaryType              |                                                            |
| any/yson             | BinaryType           | YsonType                |                                                            |
| int8                 | ByteType             | ByteType                |                                                            |
| uint8                | ShortType            | ShortType               |                                                            |
| int16                | ShortType            | ShortType               |                                                            |
| uint16               | IntegerType          | IntegerType             |                                                            |
| int32                | IntegerType          | IntegerType             |                                                            |
| uint32               | LongType             | LongType                |                                                            |
| utf8                 | StringType           | StringType              |                                                            |
| date                 | DateType             | DateType                | Arrow не поддерживается                                    |
| datetime             | DatetimeType         | DatetimeType            | Arrow не поддерживается                                    |
| timestamp            | TimestampType        | TimestampType           | Arrow не поддерживается                                    |
| interval             | LongType             | LongType                | Arrow не поддерживается                                    |
| date32               | Date32Type           | Date32Type              | Arrow не поддерживается                                    |
| datetime64           | Datetime64Type       | Datetime64Type          | Arrow не поддерживается                                    |
| timestamp64          | Timestamp64Type      | Timestamp64Type         | Arrow не поддерживается                                    |
| interval64           | Interval64Type       | Interval64Type          | Arrow не поддерживается                                    |
| uuid                 | StringType           | StringType              | Arrow не поддерживается                                    |
| json                 | StringType           | StringType              | Arrow не поддерживается                                    |
| void                 | NullType             | NullType                |                                                            |

### Композитные типы

| Тип {{product-name}} | Сопоставление с типом Spark                                             |
|----------------------|-------------------------------------------------------------------------|
| Optional(inner)      | inner тип с nullable = true                                             |
| Dict(key, value)     | MapType(key.innerLevel, value.innerLevel, value.nullable)               |
| Array(inner)         | ArrayType(inner.innerLevel, inner.nullable)                             |
| Struct(fields)       | StructType с соответствующими сопоставлениями полей                     |
| Tuple(elements)      | StructType с полями, названными `_1`, `_2` и т.д.                       |
| VariantOverStruct    | StructType с полями, префиксованными `_v` и nullable = true             |
| VariantOverTuple     | StructType с полями, названными `_v_1`, `_v_2` и т.д. и nullable = true |

## Сопоставление типов при записи в {{product-name}}

| Тип Spark            | Тип {{product-name}}                                                          | Примечания             |
|----------------------|-------------------------------------------------------------------------------|------------------------|
| NullType             | null                                                                          |                        |
| ByteType             | int8                                                                          |                        |
| ShortType            | int16 (или uint8 при указании)                                                |                        |
| IntegerType          | int32 (или uint16 при указании)                                               |                        |
| LongType             | int64 (или uint32 при указании)                                               |                        |
| StringType           | string (или utf8/json/uuid при указании, или utf8 если stringToUtf8 включено) |                        |
| FloatType            | float                                                                         |                        |
| DoubleType           | double                                                                        |                        |
| BooleanType          | boolean                                                                       |                        |
| DecimalType          | Decimal(precision, scale)                                                     | Точность ограничена 35 |
| ArrayType            | Array(inner)                                                                  |                        |
| StructType (tuple)   | Tuple(elements)                                                               |                        |
| StructType (variant) | VariantOverStruct/VariantOverTuple                                            |                        |
| StructType           | Struct(fields)                                                                |                        |
| MapType              | Dict(key, value)                                                              |                        |
| BinaryType           | binary                                                                        |                        |
| DateType             | date                                                                          |                        |
| DatetimeType         | datetime                                                                      |                        |
| TimestampType        | timestamp                                                                     |                        |
| Date32Type           | date32                                                                        |                        |
| Datetime64Type       | datetime64                                                                    |                        |
| Timestamp64Type      | timestamp64                                                                   |                        |
| Interval64Type       | interval64                                                                    |                        |
| UInt64Type           | uint64                                                                        |                        |
| YsonType             | any                                                                           |                        |

