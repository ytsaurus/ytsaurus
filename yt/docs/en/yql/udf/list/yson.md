# Yson


[YSON]({{yt-docs-root}}/user-guide/storage/yson) is a data format similar to JSON developed by Yandex.

* Similarities with JSON:

  * Does not have a strict scheme.
  * Besides simple data types, it supports dictionaries and lists in arbitrary combinations.

* Some differences from JSON:

  * It also has a binary representation in addition to the text representation.
  * The text representation uses semicolons instead of commas and equal signs instead of colons.

* The concept of "attributes" is supported, that is, named properties that can be assigned to a node in the tree.

Implementation specifics and functionality of the module:

* Along with YSON, this module also supports standard JSON to expand the application scope in a way.
* It works with a DOM representation of YSON in memory that in YQL terms is passed between functions as a "resource" (see the [description of special data types](../../types/special.md)). Most of the module's functions have the semantics of a query to perform a specified operation with a resource and return an empty [optional](../../types/optional.md) type if the operation failed because the actual data type mismatched the expected one.
* Provides several main classes of functions (find below a complete list and detailed description of functions):

  * `Yson::Parse***`: Getting a resource with a DOM object from serialized data, with all further operations performed on the obtained resource.
  * `Yson::From`: Getting a resource with a DOM object from simple YQL data types or containers (lists or dictionaries).
  * `Yson::ConvertTo***`: Converting a resource to [primitive data types](../../types/primitive.md) or [containers](../../types/containers.md).
  * `Yson::Lookup***`: Getting a single list item or a dictionary with optional conversion to the relevant data type.
  * `Yson::YPath***`: Getting one element from the document tree based on the relative path specified, optionally converting it to the relevant data type.
  * `Yson::Serialize***`: Getting a copy of data from the resource and serializing the data in one of the formats.

* For convenience, when serialized Yson and Json are passed to functions expecting a resource with a DOM object, implicit conversion using `Yson::Parse` or `Yson::ParseJson` is done automatically. In SQL syntax, the dot or square brackets operator automatically adds a `Yson::Lookup` call. To serialize a resource, you still need to call `Yson::ConvertTo***` or `Yson::Serialize***`. It means that, for example, to get the "foo" element as a string from the Yson column named mycolumn and serialized as a dictionary, you can write: `SELECT Yson::ConvertToString(mycolumn["foo"]) FROM mytable;` or `SELECT Yson::ConvertToString(mycolumn.foo) FROM mytable;`. In the variant with a dot, special characters can be escaped by [general rules for IDs](../../syntax/expressions.md#escape).

The module's functions must be considered as "building blocks" from which you can assemble different structures, for example:

* `Yson::Parse*** -> Yson::Serialize***`: Converting from one format to other.
* `Yson::Parse*** -> Yson::Lookup -> Yson::Serialize***`: Extracting the value of the specified subtree in the source YSON tree.
* `Yson::Parse*** -> Yson::ConvertToList -> ListMap -> Yson::Lookup***`: Extracting items by a key from the YSON list.



## Examples

```yql
$node = Json(@@
  {"abc": {"def": 123, "ghi": "hello"}}
@@);
SELECT Yson::SerializeText($node.abc) AS `yson`;
-- {"def"=123;"ghi"="\xD0\xBF\xD1\x80\xD0\xB8\xD0\xB2\xD0\xB5\xD1\x82"}
```

```yql
$node = Yson(@@
  <a=z;x=y>[
    {abc=123; def=456};
    {abc=234; xyz=789};
  ]
@@);
$attrs = Yson::YPath($node, "/@");

SELECT
  ListMap(Yson::ConvertToList($node), ($x) -> { return Yson::LookupInt64($x, "abc") }) AS abcs,
  Yson::ConvertToStringDict($attrs) AS attrs,
  Yson::SerializePretty(Yson::Lookup($node, "7", Yson::Options(false AS Strict))) AS miss;

/*
- abcs: `[123; 234]`
- attrs: `{"a"="z";"x"="y"}`
- miss: `NULL`
*/
```

## Yson::Parse... {#ysonparse}

```yql
Yson::Parse(Yson{Flags:AutoMap}) -> Resource<'Yson2.Node'>
Yson::ParseJson(Json{Flags:AutoMap}) -> Resource<'Yson2.Node'>
Yson::ParseJsonDecodeUtf8(Json{Flags:AutoMap}) -> Resource<'Yson2.Node'>

Yson::Parse(String{Flags:AutoMap}) -> Resource<'Yson2.Node'>? -- accepts YSON in any format
Yson::ParseJson(String{Flags:AutoMap}) -> Resource<'Yson2.Node'>?
Yson::ParseJsonDecodeUtf8(String{Flags:AutoMap}) -> Resource<'Yson2.Node'>?
```

The result of all three functions is non-serializable: it can only be passed as the input to other function from the Yson library. However, you can't save it to a table or return to the client as a result of the operation: such an attempt results in a typing error. You also can't return it outside [subqueries](../../syntax/select/index.md): if you need to do this, call [Yson::Serialize](#ysonserialize), and the optimizer will remove unnecessary serialization and deserialization if materialization isn't needed in the end.

{% note info %}

The `Yson::ParseJsonDecodeUtf8` expects that characters outside the ASCII range must be additionally escaped.

{% endnote %}

## Yson::From {#ysonfrom}

```yql
Yson::From(T) -> Resource<'Yson2.Node'>
```

`Yson::From` is a polymorphic function that converts most primitive data types and containers (lists, dictionaries, tuples, structures, and so on) into a Yson resource. The source object type must be Yson-compatible. For example, in dictionary keys, you can only use the `String` or `Utf8` data types, but not `String?` or `Utf8?` .

#### Example

```yql
SELECT Yson::Serialize(Yson::From(TableRow())) FROM table1;
```

## Yson::WithAttributes

```yql
Yson::WithAttributes(Resource<'Yson2.Node'>{Flags:AutoMap}, Resource<'Yson2.Node'>{Flags:AutoMap}) -> Resource<'Yson2.Node'>?
```

Adds attributes (the second argument) to the Yson node (the first argument). The attributes must constitute a map node.

## Yson::Equals

```yql
Yson::Equals(Resource<'Yson2.Node'>{Flags:AutoMap}, Resource<'Yson2.Node'>{Flags:AutoMap}) -> Bool
```

Checking trees in memory for equality. The operation is tolerant to the source serialization format and the order of keys in dictionaries.

## Yson::GetHash

```yql
Yson::GetHash(Resource<'Yson2.Node'>{Flags:AutoMap}) -> Uint64
```

Calculating a 64-bit hash from an object tree.

## Yson::Is...

```yql
Yson::IsEntity(Resource<'Yson2.Node'>{Flags:AutoMap}) -> bool
Yson::IsString(Resource<'Yson2.Node'>{Flags:AutoMap}) -> bool
Yson::IsDouble(Resource<'Yson2.Node'>{Flags:AutoMap}) -> bool
Yson::IsUint64(Resource<'Yson2.Node'>{Flags:AutoMap}) -> bool
Yson::IsInt64(Resource<'Yson2.Node'>{Flags:AutoMap}) -> bool
Yson::IsBool(Resource<'Yson2.Node'>{Flags:AutoMap}) -> bool
Yson::IsList(Resource<'Yson2.Node'>{Flags:AutoMap}) -> bool
Yson::IsDict(Resource<'Yson2.Node'>{Flags:AutoMap}) -> bool
```

Checking that the current node has the appropriate type. The Entity is `#`.

## Yson::GetLength

```yql
Yson::GetLength(Resource<'Yson2.Node'>{Flags:AutoMap}) -> Uint64?
```

Getting the number of elements in a list or dictionary.

## Yson::ConvertTo... {#ysonconvertto}

```yql
Yson::ConvertTo(Resource<'Yson2.Node'>{Flags:AutoMap}, Type<T>) -> T
Yson::ConvertToBool(Resource<'Yson2.Node'>{Flags:AutoMap}) -> Bool?
Yson::ConvertToInt64(Resource<'Yson2.Node'>{Flags:AutoMap}) -> Int64?
Yson::ConvertToUint64(Resource<'Yson2.Node'>{Flags:AutoMap}) -> Uint64?
Yson::ConvertToDouble(Resource<'Yson2.Node'>{Flags:AutoMap}) -> Double?
Yson::ConvertToString(Resource<'Yson2.Node'>{Flags:AutoMap}) -> String?
Yson::ConvertToList(Resource<'Yson2.Node'>{Flags:AutoMap}) -> List<Resource<'Yson2.Node'>>
Yson::ConvertToBoolList(Resource<'Yson2.Node'>{Flags:AutoMap}) -> List<Bool>
Yson::ConvertToInt64List(Resource<'Yson2.Node'>{Flags:AutoMap}) -> List<Int64>
Yson::ConvertToUint64List(Resource<'Yson2.Node'>{Flags:AutoMap}) -> List<Uint64>
Yson::ConvertToDoubleList(Resource<'Yson2.Node'>{Flags:AutoMap}) -> List<Double>
Yson::ConvertToStringList(Resource<'Yson2.Node'>{Flags:AutoMap}) -> List<String>
Yson::ConvertToDict(Resource<'Yson2.Node'>{Flags:AutoMap}) -> Dict<String,Resource<'Yson2.Node'>>
Yson::ConvertToBoolDict(Resource<'Yson2.Node'>{Flags:AutoMap}) -> Dict<String,Bool>
Yson::ConvertToInt64Dict(Resource<'Yson2.Node'>{Flags:AutoMap}) -> Dict<String,Int64>
Yson::ConvertToUint64Dict(Resource<'Yson2.Node'>{Flags:AutoMap}) -> Dict<String,Uint64>
Yson::ConvertToDoubleDict(Resource<'Yson2.Node'>{Flags:AutoMap}) -> Dict<String,Double>
Yson::ConvertToStringDict(Resource<'Yson2.Node'>{Flags:AutoMap}) -> Dict<String,String>
```

{% note warning %}

These functions do not do implicit type casting by default, that is, the value in the argument must exactly match the function called.

{% endnote %}

`Yson::ConvertTo` is a polymorphic function that converts the data type that is specified in the second argument and supports containers (lists, dictionaries, tuples, structures, and so on) into a Yson resource.

#### Example

```yql
$data = Yson(@@{
    "name" = "Anya";
    "age" = 15u;
    "params" = {
        "ip" = "95.106.17.32";
        "last_time_on_site" = 0.5;
        "region" = 213;
        "user_agent" = "Mozilla/5.0"
    }
}@@);
SELECT Yson::ConvertTo($data,
    Struct<
        name: String,
        age: Uint32,
        params: Dict<String,Yson>
    >
);
```

## Yson::Contains {#ysoncontains}

```yql
Yson::Contains(Resource<'Yson2.Node'>{Flags:AutoMap}, String) -> Bool?
```

Checks for a key in the dictionary. If the object type is a map, then it searches among the keys.
If the object type is a list, then the key must be a decimal number, i.e., an index in the list.

## Yson::Lookup... {#ysonlookup}

```yql
Yson::Lookup(Resource<'Yson2.Node'>{Flags:AutoMap}, String) -> Resource<'Yson2.Node'>?
Yson::LookupBool(Resource<'Yson2.Node'>{Flags:AutoMap}, String) -> Bool?
Yson::LookupInt64(Resource<'Yson2.Node'>{Flags:AutoMap}, String) -> Int64?
Yson::LookupUint64(Resource<'Yson2.Node'>{Flags:AutoMap}, String) -> Uint64?
Yson::LookupDouble(Resource<'Yson2.Node'>{Flags:AutoMap}, String) -> Double?
Yson::LookupString(Resource<'Yson2.Node'>{Flags:AutoMap}, String) -> String?
Yson::LookupDict(Resource<'Yson2.Node'>{Flags:AutoMap}, String) -> Dict<String,Resource<'Yson2.Node'>>?
Yson::LookupList(Resource<'Yson2.Node'>{Flags:AutoMap}, String) -> List<Resource<'Yson2.Node'>>?
```

The above functions are short notations for a typical use case: `Yson::YPath`: go to a level in the dictionary and then extract the value — `Yson::ConvertTo***`. For all the listed functions, the second argument is a key name from the dictionary (unlike YPath, it has no `/`prefix) or an index from the list (for example, `7`). They simplify the query and produce a small gain in speed.

## Yson::YPath {#ysonypath}

```yql
Yson::YPath(Resource<'Yson2.Node'>{Flags:AutoMap}, String) -> Resource<'Yson2.Node'>?
Yson::YPathBool(Resource<'Yson2.Node'>{Flags:AutoMap}, String) -> Bool?
Yson::YPathInt64(Resource<'Yson2.Node'>{Flags:AutoMap}, String) -> Int64?
Yson::YPathUint64(Resource<'Yson2.Node'>{Flags:AutoMap}, String) -> Uint64?
Yson::YPathDouble(Resource<'Yson2.Node'>{Flags:AutoMap}, String) -> Double?
Yson::YPathString(Resource<'Yson2.Node'>{Flags:AutoMap}, String) -> String?
Yson::YPathDict(Resource<'Yson2.Node'>{Flags:AutoMap}, String) -> Dict<String,Resource<'Yson2.Node'>>?
Yson::YPathList(Resource<'Yson2.Node'>{Flags:AutoMap}, String) -> List<Resource<'Yson2.Node'>>?
```

Lets you get a part of the resource based on the source resource and the part's path in YPath format.



## Yson::Attributes {#ysonattributes}

```yql
Yson::Attributes(Resource<'Yson2.Node'>{Flags:AutoMap}) -> Dict<String,Resource<'Yson2.Node'>>
```

Getting all node attributes as a dictionary.

## Yson::Serialize... {#ysonserialize}

```yql
Yson::Serialize(Resource<'Yson2.Node'>{Flags:AutoMap}) -> Yson -- A binary representation
Yson::SerializeText(Resource<'Yson2.Node'>{Flags:AutoMap}) -> Yson
Yson::SerializePretty(Resource<'Yson2.Node'>{Flags:AutoMap}) -> Yson -- To get a text result, wrap it in ToBytes(...)
```

## Yson::SerializeJson {#ysonserializejson}

```yql
Yson::SerializeJson(Resource<'Yson2.Node'>{Flags:AutoMap}, [Resource<'Yson2.Options'>?, SkipMapEntity:Bool?, EncodeUtf8:Bool?, WriteNanAsString:Bool?]) -> Json?
```

* `SkipMapEntity` serializes `#` values in dictionaries. The value of attributes is not affected by the flag. By default, `false`.
* `EncodeUtf8` responsible for escaping non-ASCII characters. By default, `false`.
* `WriteNanAsString` allows serializing `NaN` and `Inf` values as strings. By default, `false`.

The `Yson` and `Json` data types returned by serialization functions are special cases of a string that is known to contain data in the given format (Yson/Json).

## Yson::Options {#ysonoptions}

```yql
Yson::Options([AutoConvert:Bool?, Strict:Bool?]) -> Resource<'Yson2.Options'>
```

It's passed in the last optional argument (omitted for brevity) to the methods `Parse...`, `ConvertTo...`, `Contains`, `Lookup...`, and `YPath...` that accept the result of the `Yson::Options` call. By default, all the `Yson::Options` fields are false and when enabled (true), they modify the behavior as follows:

* **AutoConvert**: If the value passed to Yson doesn't match the result data type exactly, the value is converted where possible. For example, `Yson::ConvertToInt64` in this mode will convert even Double numbers to Int64.
* **Strict**: By default, all functions from the Yson library return an error in case of issues during query execution (for example, an attempt to parse a string that is not Yson/Json, or an attempt to search by a key in a scalar type, or when a conversion to an incompatible data type has been requested, and so on). If you disable the strict mode, `NULL` is returned instead of an error in most cases. When converting to a dictionary or list (`ConvertTo<Type>Dict` or `ConvertTo<Type>List`), improper items are excluded from the resulting collection.

#### Example

```yql
$yson = @@{y = true; x = 5.5}@@y;
SELECT Yson::LookupBool($yson, "z"); --- null
SELECT Yson::LookupBool($yson, "y"); --- true

-- SELECT Yson::LookupInt64($yson, "x"); --- Error occurs
SELECT Yson::LookupInt64($yson, "x", Yson::Options(false as Strict)); --- null
SELECT Yson::LookupInt64($yson, "x", Yson::Options(true as AutoConvert)); --- 5

-- SELECT Yson::ConvertToBoolDict($yson); --- Error occurs
SELECT Yson::ConvertToBoolDict($yson, Yson::Options(false as Strict)); --- { "y": true }
SELECT Yson::ConvertToDoubleDict($yson, Yson::Options(false as Strict)); --- { "x": 5.5 }
```

If you need to use the same Yson library settings throughout the query, it's more convenient to use [PRAGMA yson.AutoConvert;](../../syntax/pragma/yson.md#autoconvert) and/or [PRAGMA yson.Strict;](../../syntax/pragma/yson.md#strict). Only with these `PRAGMA` you can affect implicit calls to the Yson library occurring when you work with Yson/Json data types.

## Yson::Iterate {#ysoniterate}

```yql
Yson::Iterate(Resource<'Yson2.Node'>{Flags:AutoMap}) -> List<Variant<
    'BeginAttributes':Void,
    'BeginList':Void,
    'BeginMap':Void,
    'EndAttributes':Void,
    'EndList':Void,
    'EndMap':Void,
    'Item':Void,
    'Key':String,
    'PostValue':Resource<'Yson2.Node'>,
    'PreValue':Resource<'Yson2.Node'>,
    'Value':Resource<'Yson2.Node'>>>
```

Added in version [2025.05](../../changelog/2025.05.md#yson-module).
Retrieves a full list of events when traversing a Yson tree.
Leaf nodes (`Entity`, `Bool`, `Int64`, `Uint64`, `Double`, `String`) are returned as `Value` events.

For a `List` node, the output contains the following sequence:
* `PreValue` containing the node itself
* `BeginList`
* `Item` (precedes each `List` element)
* Events for the `List` element
* `EndList`
* `PostValue` containing the node itself

For a `Map` node, the output contains the following sequence:
* `PreValue` containing the node itself
* `BeginMap`
* `Key` (precedes each `Map` element)
* Events for the `Map` element
* `EndMap`
* `PostValue` containing the node itself
The order of returned keys is arbitrary.

For a node with non-empty attributes, the output contains the following sequence:
* `PreValue` containing the node itself
* `BeginAttributes`
* `Key` (precedes each attribute name)
* Events for the attribute
* `EndAttributes`
* Events for the node, without attributes
* `PostValue` containing the node itself
The order of returned attributes is arbitrary.

#### Examples

```yql
-- view the all function output Yson::Iterate
$dump = ($x) -> (
    (
        Way($x),
        $x.Key,
        Yson::Serialize($x.PreValue),
        Yson::Serialize($x.Value),
        Yson::Serialize($x.PostValue)
    )
);

SELECT ListMap(Yson::Iterate('{a=1;b=<c="foo">[2u;%true;#;-3.2]}'y), $dump);

/*
Events:
    PreValue [1]
    BeginMap
    Key a
    Value 1
    Key b
    PreValue [2]
    BeginAttributes
    Key c
    Value foo
    EndAttributes
    PreValue [3]
    BeginList
    Item
    Value 2
    Item
    Value %true
    Item
    Value #
    Item
    Value -3.2
    EndList
    PostValue [3]
    PostValue [2]
    EndMap
    PostValue [1]
*/
```

```yql
-- Getting a the list values - opening all the lists
$yson = '[[1;2];[3;4]]'y;
SELECT ListFlatMap(Yson::Iterate($yson), ($x)->(IF($x.Value IS NOT NULL, $x.Value))); -- [1;2;3;4]
```

```yql
-- Searching a key with a given name of any level
$yson = '{a={b={c=1}};e={f=2}}'y;
SELECT ListHasItems(ListFilter(Yson::Iterate($yson), ($x)->($x.Key == 'b'))); -- true
```

```yql
-- Searching a string in a values of any level
$yson = '{a={b={c="x"}};e={f="y"}}'y;
SELECT ListHasItems(ListFilter(Yson::Iterate($yson), ($x)->(Yson::ConvertToString($x.Value) == 'y'))); -- true
```

```yql
-- Getting attributes `name` for all Map nodes without an attribute `children`
$yson = @@{
    name=foo;
    children=[
        {
            name=bar
        }
    ]
}@@y;

SELECT ListFlatMap(Yson::Iterate($yson), ($x)->(
    IF(Yson::IsDict($x.PreValue) and not Yson::Contains($x.PreValue,'children'), Yson::LookupString($x.PreValue, 'name')))); -- [bar]
```

## Yson::As... and Yson::TryAs... {#ysonas}

```yql
Yson::AsString(Resource<'Yson2.Node'>{Flags:AutoMap}) -> String
Yson::AsDouble(Resource<'Yson2.Node'>{Flags:AutoMap}) -> Double
Yson::AsUint64(Resource<'Yson2.Node'>{Flags:AutoMap}) -> Uint64
Yson::AsInt64(Resource<'Yson2.Node'>{Flags:AutoMap}) -> Int64
Yson::AsBool(Resource<'Yson2.Node'>{Flags:AutoMap}) -> Bool
Yson::AsList(Resource<'Yson2.Node'>{Flags:AutoMap}) -> List<Resource<'Yson2.Node'>>
Yson::AsDict(Resource<'Yson2.Node'>{Flags:AutoMap}) -> Dict<String, Resource<'Yson2.Node'>>

Yson::TryAsString(Resource<'Yson2.Node'>{Flags:AutoMap}) -> String?
Yson::TryAsDouble(Resource<'Yson2.Node'>{Flags:AutoMap}) -> Double?
Yson::TryAsUint64(Resource<'Yson2.Node'>{Flags:AutoMap}) -> Uint64?
Yson::TryAsInt64(Resource<'Yson2.Node'>{Flags:AutoMap}) -> Int64?
Yson::TryAsBool(Resource<'Yson2.Node'>{Flags:AutoMap}) -> Bool?
Yson::TryAsList(Resource<'Yson2.Node'>{Flags:AutoMap}) -> List<Resource<'Yson2.Node'>>?
Yson::TryAsDict(Resource<'Yson2.Node'>{Flags:AutoMap}) -> Dict<String, Resource<'Yson2.Node'>>?
```

Added in version [2025.05](../../changelog/2025.05.md#yson-module).
Casts a Yson node to the specified type.
If the specified type isn't a valid Yson node type, `TryAs*` functions return `NULL`, while `As*` functions return a query error.
To process a node with the `Entity` ('#') type, use the [`IsEntity`](#ysonis) function.

## In-place Yson node editing {#yson-modify}

Starting with version [2025.05](../../changelog/2025.05.md#yson-module), you can work with a mutable Yson tree based on [linear](../../types/linear.md) types. Use the [`MutCreate`](#mutcreate) function to create a new tree and the [`Mutate`](#mutate) function to make an existing immutable tree mutable.
Trees should be created/transformed inside the [`Block`](../../builtins/basic.md#block) function. The resulting mutable Yson tree must ultimately be converted into an immutable one using the [`MutFreeze`](#mutfreeze) function.
Each instance of a mutable Yson tree has a single current node that you can move while simultaneously creating new nodes as needed.

## Yson::MutCreate {#mutcreate}

```yql
Yson::MutCreate() -> Linear<Resource<Yson2.MutNode>>
```

Added in version [2025.05](../../changelog/2025.05.md#yson-module).
Creates a new empty tree with a single node in the `Invalid` state, which corresponds to an undefined type. You can change the value of this node, for example, using the [`MutUpsert`](#mutupsert) function. In this case, the root of the resulting tree is set as the current node.

Since this function creates an instance of a linear type, the dependent nodes should be specified via a [`Udf`](../../builtins/basic.md#udf) when using it.

#### Examples

```yql
SELECT Block(($arg)->{
    $m = Udf(Yson::MutCreate, $arg as Depends)();
    return Yson::MutFreeze($m); -- Error: Invalid or deleted node is not allowed
})
```

```yql
SELECT Block(($arg)->{
    $m = Udf(Yson::MutCreate, $arg as Depends)();
    $m = Yson::MutUpsert($m, '1'y);
    return Yson::MutFreeze($m); -- 1
});
```

## Yson::Mutate {#mutate}

```yql
Yson::Mutate(Resource<Yson2.Node>) -> Linear<Resource<Yson2.MutNode>>
```

Added in version [2025.05](../../changelog/2025.05.md#yson-module).
Creates a mutable Yson tree from an immutable one. In this case, the root of the resulting tree is set as the current node.

Since this function creates an instance of a linear type, the dependent nodes should be specified via a [`Udf`](../../builtins/basic.md#udf) when using it.

#### Examples

```yql
SELECT Block(($arg)->{
    $m = Udf(Yson::Mutate, $arg as Depends)('1'y);
    return Yson::MutFreeze($m); -- 1
});
```

## Yson::MutFreeze {#mutfreeze}

```yql
Yson::MutFreeze(Linear<Resource<Yson2.MutNode>>) -> Resource<Yson2.Node>
```

Added in version [2025.05](../../changelog/2025.05.md#yson-module).
Converts a mutable Yson tree to an immutable one. All nodes in the `Deleted` state are removed from maps and lists.
The root node of the mutable tree must not be in the `Invalid` or `Deleted` state, and its child nodes must not be in the `Invalid` state. Otherwise, the conversion will fail.

#### Examples

```yql
SELECT Block(($arg)->{
    $m = Udf(Yson::Mutate, $arg as Depends)('1'y);
    return Yson::MutFreeze($m); -- 1
});
```

```yql
SELECT Block(($arg)->{
    $m = Udf(Yson::MutCreate, $arg as Depends)(); -- Root node has the state Invalid
    return Yson::MutFreeze($m); -- Error: Invalid or deleted node is not allowed
})
```

```yql
SELECT Block(($arg)->{
    $m = Udf(Yson::Mutate, $arg as Depends)('1'y);
    $m = Yson::MutRemove($m); -- Chanching the node state to Deleted
    return Yson::MutFreeze($m); -- Error: Invalid or deleted node is not allowed
})
```

## Yson::MutUpsert {#mutupsert}

```yql
Yson::MutUpsert(Linear<Resource<Yson2.MutNode>>, Resource<Yson2.MutNode>) -> Linear<Resource<Yson2.MutNode>>
```

Added in version [2025.05](../../changelog/2025.05.md#yson-module).
Replaces the current node in a mutable Yson tree with the specified Yson subtree.

#### Examples

```yql
SELECT Block(($arg)->{
    $m = Udf(Yson::Mutate, $arg as Depends)('1'y);
    $m = Yson::MutUpsert($m, '2'y);
    return Yson::MutFreeze($m); -- 2
});
```

## Yson::MutInsert {#mutinsert}

```yql
Yson::MutInsert(Linear<Resource<Yson2.MutNode>>, Resource<Yson2.MutNode>) -> Linear<Resource<Yson2.MutNode>>
```

Added in version [2025.05](../../changelog/2025.05.md#yson-module).
Replaces the current node in a mutable Yson tree with the specified Yson subtree if the current node is in the `Invalid` or `Deleted` state.

#### Examples

```yql
SELECT Block(($arg)->{
    $m = Udf(Yson::MutCreate, $arg as Depends)();
    $m = Yson::MutInsert($m, '1'y);
    $m = Yson::MutInsert($m, '2'y); -- has no effect
    return Yson::MutFreeze($m); -- 1
});
```

## Yson::MutUpdate {#mutupdate}

```yql
Yson::MutUpdate(Linear<Resource<Yson2.MutNode>>, Resource<Yson2.MutNode>) -> Linear<Resource<Yson2.MutNode>>
```

Added in version [2025.05](../../changelog/2025.05.md#yson-module).
Replaces the current node in a mutable Yson tree with the specified Yson subtree if the current node isn't in the `Invalid` or `Deleted` state.

#### Examples

```yql
SELECT Block(($arg)->{
    $m = Udf(Yson::MutCreate, $arg as Depends)();
    $m = Yson::MutUpdate($m, '1'y); -- has no effect
    $m = Yson::MutInsert($m, '2'y);
    $m = Yson::MutUpdate($m, '3'y);
    return Yson::MutFreeze($m); -- 3
});
```

## Yson::MutRemove {#mutremove}

```yql
Yson::MutRemove(Linear<Resource<Yson2.MutNode>>) -> Linear<Resource<Yson2.MutNode>>
```

Added in version [2025.05](../../changelog/2025.05.md#yson-module).
Changes the current node to the `Deleted` state.

#### Examples

```yql
SELECT Block(($arg)->{
    $m = Udf(Yson::Mutate, $arg as Depends)('1'y);
    $m = Yson::MutRemove($m);
    $m = Yson::MutInsert($m, '2'y);
    return Yson::MutFreeze($m); -- 2
});
```

## Yson::MutRewind {#mutrewind}

```yql
Yson::MutRewind(Linear<Resource<Yson2.MutNode>>) -> Linear<Resource<Yson2.MutNode>>
```

Added in version [2025.05](../../changelog/2025.05.md#yson-module).
Moves the current node to the root of the tree.

#### Examples

```yql
SELECT Block(($arg)->{
    $m = Udf(Yson::Mutate, $arg as Depends)('{a=1;b=2}'y);
    $m = Yson::MutDown($m, 'a'); -- Move to key 'a' in the map
    $m = Yson::MutRewind($m); -- Move to the root
    $m = Yson::MutUpsert($m, '1'y); -- Replace the tree starting at the root
    return Yson::MutFreeze($m); -- 1
});
```

## Yson::MutUp {#mutup}

```yql
Yson::MutUp(Linear<Resource<Yson2.MutNode>>) -> Linear<Resource<Yson2.MutNode>>
```

Added in version [2025.05](../../changelog/2025.05.md#yson-module).
Moves the current node one level closer to the root of the tree. If the current node is already at the root of the tree, generates an error.

#### Examples

```yql
SELECT Block(($arg)->{
    $m = Udf(Yson::Mutate, $arg as Depends)('{a=1;b=2}'y);
    $m = Yson::MutDown($m, 'a'); -- Move to key 'a' in the map
    $m = Yson::MutUp($m); -- Move to the root
    $m = Yson::MutUpsert($m, '1'y); -- Replace the tree starting at the root
    return Yson::MutFreeze($m); -- 1
});
```

## Yson::MutDown, Yson::MutDownOrCreate, Yson::MutTryDown {#mutdown}

```yql
Yson::MutDown(Linear<Resource<Yson2.MutNode>>,location:String) -> Linear<Resource<Yson2.MutNode>>
Yson::MutDownOrCreate(Linear<Resource<Yson2.MutNode>>,location:String) -> Linear<Resource<Yson2.MutNode>>
Yson::MutTryDown(Linear<Resource<Yson2.MutNode>>,location:String) -> Tuple<Linear<Resource<Yson2.MutNode>>, Bool>
```

Added in version [2025.05](../../changelog/2025.05.md#yson-module).
Moves the current node to an element within a map or list. This operation can create a new node in the `Invalid` state.

The location string is escaped using a backslash (`\`).
If the location string begins with the operator `<`, `=`, or `>`, it must be followed by a specific zero-based list index in decimal format, `first` (index 0), or `last` (last element in the list). Otherwise, location is interpreted as a map key.
For an empty list, `first` and `last` correspond to positions outside the list.

The `MutDown` function can't create new keys within maps. It can only apply the `=` modifier to lists and generates an error if the current node can't be moved.
Unlike `MutDown`, the `MutTryDown` function returns a tuple where the second element is `false` if the current node can't be moved.
If the current node is in the `Invalid` or `Deleted` state, the `MutDownOrCreate` function first creates it as an empty list or map, depending on the type of location.
If the type of the current node (list or map) doesn't match location, both `MutDown` and `MutDownOrCreate` return an error.

#### Examples

```yql
SELECT Block(($arg)->{
    $m = Udf(Yson::Mutate, $arg as Depends)('{a=1;b=2}'y);
    $m = Yson::MutDown($m, 'a'); -- moved to the key 'a' in a dict
    $m = Yson::MutUpsert($m, '3'y); -- replaced the current node
    return Yson::MutFreeze($m); -- {a=3;b=2}
});
```

```yql
SELECT Block(($arg)->{
    $m = Udf(Yson::Mutate, $arg as Depends)('{a=1;b=2}'y);
    $m = Yson::MutDownOrCreate($m, 'c'); -- moved to the key 'c' in a dict - it was created in the state `Invalid`
    $m = Yson::MutUpsert($m, '3'y); -- replaced the current node
    return Yson::MutFreeze($m); -- {a=1;b=2;c=3}
});
```

```yql
SELECT Block(($arg)->{
    $m = Udf(Yson::Mutate, $arg as Depends)('{a=1;b=2}'y);
    $m = Yson::MutDown($m, 'c'); -- removed to the key 'c' in a dict, error, it doesn`t exists
    return Yson::MutFreeze($m);
});
```

```yql
SELECT Block(($arg)->{
    $m = Udf(Yson::Mutate, $arg as Depends)('{a=1;b=2}'y);
    $m, $ok = Yson::MutTryDown($m, 'c'); -- tried to move to the key 'c' in a dict, returned `false`
    return (Yson::MutFreeze($m), $ok); -- ({a=1;b=2}, false)
});
```

```yql
SELECT Block(($arg)->{
    $m = Udf(Yson::Mutate, $arg as Depends)('[1;2]'y);
    $m = Yson::MutDownOrCreate($m, '>last'); -- added node to the end of a list in a state `Invalid`
    $m = Yson::MutUpsert($m, '3'y); -- replaced the current node
    return Yson::MutFreeze($m); -- [1;2;3]
});
```

```yql
SELECT Block(($arg)->{
    $m = Udf(Yson::Mutate, $arg as Depends)('[1;2]'y);
    $m = Yson::MutDownOrCreate($m, '<1'); -- added node before the node with the index  1 (value '2')
    $m = Yson::MutUpsert($m, '3'y); -- replaced the current node
    return Yson::MutFreeze($m); -- [1;3;2]
});
```

```yql
SELECT Block(($arg)->{
    $m = Udf(Yson::Mutate, $arg as Depends)('[1;2]'y);
    $m = Yson::MutDownOrCreate($m, '<first'); -- added the node to the start of the list
    $m = Yson::MutUpsert($m, '3'y); -- replaced the current node
    return Yson::MutFreeze($m); -- [3;1;2]
});
```

```yql
SELECT Block(($arg)->{
    $m = Udf(Yson::Mutate, $arg as Depends)('[1;2]'y);
    $m = Yson::MutDown($m, '=last'); -- moving to the last node of the list
    $m = Yson::MutUpsert($m, '3'y); -- replaced the current node
    return Yson::MutFreeze($m); -- [1;3]
});
```

```yql
SELECT Block(($arg)->{
    $m = Udf(Yson::Mutate, $arg as Depends)('[1;2]'y);
    $m = Yson::MutDownOrCreate($m, 'a'); -- error, current node is not a dict
    return Yson::MutFreeze($m);
});
```

```yql
SELECT Block(($arg)->{
    $m = Udf(Yson::Mutate, $arg as Depends)('[a=1;b=2]'y);
    $m = Yson::MutDownOrCreate($m, '=0'); -- error, current node is not a list
    return Yson::MutFreeze($m);
});
```

## Yson::MutExists {#mutexists}

```yql
Yson::MutExists(Linear<Resource<Yson2.MutNode>>) -> Tuple<Linear<Resource<Yson2.MutNode>>,Bool>
```

Added in version [2025.05](../../changelog/2025.05.md#yson-module).
Checks whether the current node is in the `Invalid` or `Deleted` state.
This function doesn't modify the tree itself or the position of its current node.

#### Examples

```yql
SELECT Block(($arg)->{
    $m = Udf(Yson::MutCreate, $arg as Depends)();
    $m, $exists = Yson::MutExists($m); -- current node is Invalid
    return LinearDestroy($exists, $m); -- retorning the value $exists=false, absorbing linear type $m
});
```

```yql
SELECT Block(($arg)->{
    $m = Udf(Yson::Mutate, $arg as Depends)('1'y);
    $m, $exists = Yson::MutExists($m); -- current node is not Invalid
    return LinearDestroy($exists, $m); -- returning the value $exists=true, absorbing linear type $m
});
```

## Yson::MutView {#mutview}

```yql
Yson::MutView(Linear<Resource<Yson2.MutNode>>) -> Tuple<Linear<Resource<Yson2.MutNode>>, Optional<Resource<Yson2.Node>>>
```

Added in version [2025.05](../../changelog/2025.05.md#yson-module).
Returns an immutable Yson tree starting from the current node unless it's in the `Invalid` or `Deleted` state. Otherwise, returns `NULL`.
Similar to the `MutFreeze` function, all child nodes of the current node must not be in the `Invalid` state, and `Deleted` nodes are skipped when creating the resulting Yson tree.
This function doesn't modify the tree itself or the position of its current node.

#### Examples

```yql
SELECT Block(($arg)->{
    $m = Udf(Yson::MutCreate, $arg as Depends)();
    $m, $view = Yson::MutView($m); -- Current node is in the Invalid state
    return LinearDestroy($view, $m); -- Return the $view=NULL value and consume the linear type $m
});
```

```yql
SELECT Block(($arg)->{
    $m = Udf(Yson::Mutate, $arg as Depends)('1'y);
    $m, $view = Yson::MutView($m); -- Current node isn't Invalid
    return LinearDestroy($view, $m); -- Return the $view='1'y value and consume the linear type $m
});
```

## See also

* [{#T}](../../recipes/accessing-json.md)
* [{#T}](../../recipes/modifying-json.md)

