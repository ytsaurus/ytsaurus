
## LENGTH {#length}

Returns the length of the string in bytes. This function is also available under the `LEN` name .

**Signature**
```
LENGTH(T)->Uint32
LENGTH(T?)->Uint32?
```

**Examples**
```yql
SELECT LENGTH("foo");
```
```yql
SELECT LEN("bar");
```

{% note info %}

You can use the function [Unicode::GetLength](../../../udf/list/unicode.md) to calculate the length of a string in Unicode characters.<br><br>To get the number of elements in the list, use the function [ListLength](../../list.md#listlength).

{% endnote %}
