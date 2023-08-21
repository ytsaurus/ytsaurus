
## Enum, AsEnum {#enum}

`Enum()` creates an enumeration value.

**Signature**
```
Enum(String, Type<Enum<...>>)->Enum<...>
```

Arguments:

* A string with the field name
* Enumeration type

**Example**
```yql
$enum_type = Enum<Foo, Bar>;
SELECT
   Enum("Foo", $enum_type) as Enum1Value,
   Enum("Bar", $enum_type) as Enum2Value;
```

`AsEnum()` creates an [enumeration](../../../types/containers.md) value with one element. This value can be implicitly cast to any enumeration containing such a name.

**Signature**
```
AsEnum(String)->Enum<'tag'>
```

Arguments:

* A string with the name of an enumeration item

**Example**
```yql
SELECT
   AsEnum("Foo");
```
