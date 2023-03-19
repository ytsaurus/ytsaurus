---
vcsPath: ydb/docs/ru/core/yql/reference/yql-core/builtins/_includes/types.md
sourcePath: ydb/docs/ru/core/yql/reference/yql-core/builtins/_includes/types.md
---
# Functions for working with data types

In addition to the ordinary functions that work with particular values (FIND, COALESCE types), YQL supports functions for working with [types](../../types/index.md).
The functions enable you to find out the type of an arbitrary expression, analyze a container type, and create a new container type based on the existing one.

**Examples**
```yql
$itemType = TypeOf($item);
SELECT CAST($foo AS ListType($itemType));  -- cast $foo to type List<$itemType>
```

## FormatType {#formattype}

**Signature**
```
FormatType(Type)->String
```

Serialization of a type or a type handle into a human-readable string. This is useful for debugging and will also be used in the following examples in this section. [Format documentation](../../types/type_string.md).

## ParseType {#parsetype}

**Signature**
```
ParseType(String)->Type
```
Building a type by a string with its description. [Its format documentation](../../types/type_string.md).

**Examples**
```yql
SELECT FormatType(ParseType("List<Int32>"));  -- List<int32>
```

## TypeOf {#typeof}

**Signature**
```
TypeOf(<any expression>)->Type
```
Getting the value type passed to the argument.

**Examples**
```yql
SELECT FormatType(TypeOf("foo"));  -- String
```
```yql
SELECT FormatType(TypeOf(AsTuple(1, 1u))); -- Tuple<Int32,Uint32>
```

## InstanceOf {#instanceof}

**Signature**
```
InstanceOf(Type)->Type object
```

Returns an instance of the object of the specified type. The resulting object does not have any particular meaning.
InstanceOf can only be used if the result of the expression in which InstanceOf is used depends on the InstanceOf type, but not on the value.   
Otherwise, the operation will be completed with an error.

**Examples**
```yql
SELECT InstanceOf(ParseType("Int32")) + 1.0; -- error (Can't execute InstanceOf): the result depends on the (undefined) value of InstanceOf
SELECT FormatType(TypeOf(
    InstanceOf(ParseType("Int32")) +
    InstanceOf(ParseType("Double"))
)); -- will return Double, because adding Int32 and Double returns Double (InstanceOf is used in the context where only its type is important, but not its value)
```

## DataType {#datatype}

**Signature**
```
DataType(String, [String, ...])->Type
```
Returns the type for [primitive data types](../../types/primitive.md) by its name.
For some types (for example, Decimal), you need to pass type parameters as additional arguments.

**Examples**
```yql
SELECT FormatType(DataType("Bool")); -- Bool
SELECT FormatType(DataType("Decimal","5","1")); -- Decimal(5,1)
```

## OptionalType {#optionaltype}

**Signature**
```
OptionalType(Type)->optional Type
```
Adds the ability to contain `NULL` to the passed type.

**Examples**
```yql
SELECT FormatType(OptionalType(DataType("Bool"))); -- Bool?
SELECT FormatType(OptionalType(ParseType("List<String?>"))); -- List<String?>?
```

## ListType and StreamType {#listtype}

**Signature**
```
ListType(Type)->list type with items of the Type type
StreamType(Type)->stream type with items of the Type type
```

Builds a list or stream type based on the passed item type.

**Examples**
```yql
SELECT FormatType(ListType(DataType("Bool"))); -- List<Bool>
```

## DictType {#dicttype}

**Signature**
```
DictType(Type, Type)->dict type
```

Builds a dict type based on the passed key (first argument) and value (second argument) types.

**Examples**
```yql
SELECT FormatType(DictType(
    DataType("String"),
    DataType("Double")
)); -- Dict<String,Double>
```

## TupleType {#tupletype}

**Signature**
```
TupleType(Type, ...)->tuple type
```
Builds a tuple type based on the passed item types.

**Examples**
```yql
SELECT FormatType(TupleType(
    DataType("String"),
    DataType("Double"),
    OptionalType(DataType("Bool"))
)); -- Tuple<String,Double,Bool?>
```

## StructType {#structtype}

**Signature**
```
StructType(Type AS ElementName1, Type AS ElementName2, ...)->structure type
```
Builds a structure type based on the passed item types. The standard syntax for named arguments is used to specify item names.

**Examples**
```yql
SELECT FormatType(StructType(
    DataType("Bool") AS MyBool,
    ListType(DataType("String")) AS StringList
)); -- Struct<'MyBool':Bool,'StringList':List<String>>
```

## VariantType {#varianttype}

**Signature**
```
VariantType(StructType)->variant type on top of structure
VariantType(TupleType)->variant type on top of tuple
```
Returns the variant type by the underlying type (structure or tuple).

**Examples**
```yql
SELECT FormatType(VariantType(
  ParseType("Struct<foo:Int32,bar:Double>")
)); -- Variant<'bar':Double,'foo':Int32>
```

## ResourceType {#resourcetype}

**Signature**
```
ResourceType(String)->resource type
```
Returns the [resource](../../types/special.md) type by the passed string label.

**Examples**
```yql
SELECT FormatType(ResourceType("Foo")); -- Resource<'Foo'>
```

## CallableType {#callabletype}

**Signature**
```
CallableType(Uint32, Type, [Type, ...])->callable value type
```
Builds the callable value type based on the following arguments:

1. Number of optional arguments (if all are mandatory, then 0).
2. Result type.
3. All subsequent CallableType arguments are interpreted as the argument types of the callable value with a shift of two mandatory ones (for example, the third CallableType argument describes the type of the first argument of the callable value).

**Examples**
```yql
SELECT FormatType(CallableType(
  1, -- optional args count
  DataType("Double"), -- result type
  DataType("String"), -- arg #1 type
  OptionalType(DataType("Int64")) -- arg #2 type
)); -- Callable<(String,[Int64?])->Double>
```

## GenericType, UnitType and VoidType {#generictype}

**Signature**
```
GenericType()->type
UnitType()->type
VoidType()->type
```
Return [special data types](../../types/special.md) with the same name. There are no arguments, because they are not parameterized.

**Examples**
```yql
SELECT FormatType(VoidType()); -- Void
```

## OptionalItemType, ListItemType and StreamItemType {#optionalitemtype}

**Signature**
```
OptionalItemType(OptionalType)->optional item type
ListItemType(ListType)->list item type
StreamItemType(StreamType)->stream item type
```

If a type is passed to these functions, they perform the action opposite to [OptionalType](#optionaltype), [ListType](#listtype), and [StreamType](#streamtype) — they return the item type by the type of the corresponding container.
If a type handle is passed to these functions, they perform the action opposite to [OptionalTypeHandle](#optionaltypehandle), [ListTypeHandle](#listtypehandle), and [StreamTypeHandle](#streamtypehandle) — they return the item type handle by the type handle of the corresponding container.

**Examples**
```yql
SELECT FormatType(ListItemType(
  ParseType("List<Int32>")
)); -- Int32
```
```yql
SELECT FormatType(ListItemType(
  ParseTypeHandle("List<Int32>")
)); -- Int32
```

## DictKeyType and DictPayloadType {#dictkeytype}

**Signature**
```
DictKetType(DictType)->dict key type
DictPayloadType(DictType)->dict meaning type
```
Return the key or meaning type by the dict type.

**Examples**
```yql
SELECT FormatType(DictKeyType(
  ParseType("Dict<Int32,String>")
)); -- Int32
```

## TupleElementType {#tupleelementtype}

**Signature**
```
TupleElementType(TupleType, String)->tuple item type
```
Returns the tuple item type by the tuple type and item index (index from zero).

**Examples**
```yql
SELECT FormatType(TupleElementType(
  ParseType("Tuple<Int32,Double>"), "1"
)); -- Double
```

## StructMemberType {#structmembertype}

**Signature**
```
StructMemberType(StructType, String)->structure item type
```
Returns the structure item type by the structure type and item name.

**Examples**
```yql
SELECT FormatType(StructMemberType(
  ParseType("Struct<foo:Int32,bar:Double>"), "foo"
)); -- Int32
```

## CallableResultType and CallableArgumentType {#callableresulttype}

**Signature**
```
CallableResultType(CallableType)->callable value result type
CallableArgumentType(CallableType, Uint32)->callable value argument type
```
`CallableResultType` returns the result type by the callable value type and `CallableArgumentType` returns the argument type by the callable value type and its index (index from zero).

**Examples**
```yql
$callable_type = ParseType("(String,Bool)->Double");

SELECT FormatType(CallableResultType(
    $callable_type
)), -- Double
FormatType(CallableArgumentType(
    $callable_type, 1
)); -- Bool
```

## VariantUnderlyingType {#variantunderlyingtype}

**Signature**
```
VariantUnderlyingType(VariantType)->underlying variant type
```
If a type is passed to this function, it performs the action opposite to [VariantType](#varianttype) — it returns the underlying type by the variant type.
If a type handle is passed to this function, it performs the action opposite to [VariantTypeHandle](#varianttypehandle) — it returns the underlying type handle by the variant type handle.

**Examples**
```yql
SELECT FormatType(VariantUnderlyingType(
  ParseType("Variant<foo:Int32,bar:Double>")
)), -- Struct<'bar':Double,'foo':Int32>
FormatType(VariantUnderlyingType(
  ParseType("Variant<Int32,Double>")
)); -- Tuple<Int32,Double>
```
```yql
SELECT FormatType(VariantUnderlyingType(
  ParseTypeHandle("Variant<foo:Int32,bar:Double>")
)), -- Struct<'bar':Double,'foo':Int32>
FormatType(VariantUnderlyingType(
  ParseTypeHandle("Variant<Int32,Double>")
)); -- Tuple<Int32,Double>
```

# Functions for working with data types during computations

To work with data types during computations, the type handle mechanism is used — the [resource](../../types/special.md) containing an opaque type description. After constructing a type handle, you can return to a regular type using the [EvaluateType](#evaluatetype) function. For debugging, convert the type handle into a string using the [FormatType](#formattype) function.

## TypeHandle

Getting a type handle from the type passed to the argument.

**Signature**
```
TypeHandle(Type)->хэндл типа
```

**Examples:**
```yql
SELECT FormatType(TypeHandle(TypeOf("foo")));  -- String
```
## EvaluateType

**Signature**
```
EvaluateType(TypeHandle)->type
```
Getting a type from the type handle passed to the argument. The function is computed before the main computation process starts, just like [EvaluateExpr](../basic.md#evaluate_expr_atom).

**Examples:**
```yql
SELECT FormatType(EvaluateType(TypeHandle(TypeOf("foo"))));  -- String
```

## ParseTypeHandle

**Signature**
```
ParseTypeHandle(String)->type handle
```
Building a type handle by a string with its description. [Its format documentation](../../types/type_string.md).

**Examples:**
```yql
SELECT FormatType(ParseTypeHandle("List<Int32>"));  -- List<int32>
```

## TypeKind

**Signature**
```
TypeKind(TypeHandle)->String
```
Getting the name of the top-level type from the type handle passed to the argument.

**Examples:**
```yql
SELECT TypeKind(TypeHandle(TypeOf("foo")));  -- Data
SELECT TypeKind(ParseTypeHandle("List<Int32>"));  -- List
```

## DataTypeComponents

**Signature**
```
DataTypeComponents(DataTypeHandle)->List<String>
```
Getting the name and parameters of the [primitive data type](../../types/primitive.md) from the handle of the primitive type passed to the argument. The reverse function is [DataTypeHandle](#datatypehandle).

**Examples:**
```yql
SELECT DataTypeComponents(TypeHandle(TypeOf("foo")));  -- ["String"]
SELECT DataTypeComponents(ParseTypeHandle("Decimal(4,1)"));  -- ["Decimal", "4", "1"]
```

## DataTypeHandle

**Signature**
```
DataTypeHandle(List<String>)->handle of the primitive data type
```
Building a handle of the [primitive data type](../../types/primitive.md) from its name and parameters passed to the argument as a list. The reverse function is [DataTypeComponents](#datatypecomponents).

**Examples:**
```yql
SELECT FormatType(DataTypeHandle(
    AsList("String")
)); -- String

SELECT FormatType(DataTypeHandle(
    AsList("Decimal", "4", "1")
)); -- Decimal(4,1)
```

## OptionalTypeHandle

**Signature**
```
OptionalTypeHandle(TypeHandle)->optional type handle
```
Adds the ability to contain `NULL` to the passed type handle.

**Examples:**
```yql
SELECT FormatType(OptionalTypeHandle(
    TypeHandle(DataType("Bool"))
)); -- Bool?
```

## ListTypeHandle and StreamTypeHandle {#list-stream-typehandle}

**Signature**
```
ListTypeHandle(TypeHandle)->list type handle
StreamTypeHandle(TypeHandle)->stream type handle
```
Builds a list or stream type handle based on the passed item type handle.

**Examples:**
```yql
SELECT FormatType(ListTypeHandle(
    TypeHandle(DataType("Bool"))
)); -- List<Bool>
```

## EmptyListTypeHandle and EmptyDictTypeHandle

**Signature**
```
EmptyListTypeHandle()->empty list type handle
EmptyDictTypeHandle()->empty dict type handle
```
Builds an empty list or dict type handle.

**Examples:**
```yql
SELECT FormatType(EmptyListTypeHandle()); -- EmptyList
```

## TupleTypeComponents

**Signature**
```
TupleTypeComponents(TupleTypeHandle)->List<TypeHandle>
```
Getting the item type handle list from the tuple type handle passed to the argument. The reverse function is [TupleTypeHandle](#tupletypehandle).

**Examples:**
```yql
SELECT ListMap(
   TupleTypeComponents(
       ParseTypeHandle("Tuple<Int32, String>")
   ),
   ($x)->{
       return FormatType($x)
   }
); -- ["Int32", "String"]
```

## TupleTypeHandle

**Signature**
```
TupleTypeHandle(List<TypeHandle>)->tuple type handle
```
Building a tuple type handle from the item type handles passed to the argument as a list. The reverse function is [TupleTypeComponents](#tupletypecomponents).

**Examples:**
```yql
SELECT FormatType(
    TupleTypeHandle(
        AsList(
            ParseTypeHandle("Int32"),
            ParseTypeHandle("String")
        )
    )
); -- Tuple<Int32,String>
```

## StructTypeComponents

**Signature**
```
StructTypeComponents(StructTypeHandle)->List<Struct<Name:String, Type:TypeHandle>>
```
Getting the item type handle list and their names from the structure type handle passed to the argument. The reverse function is [StructTypeHandle](#structtypehandle).

**Examples:**
```yql
SELECT ListMap(
    StructTypeComponents(
        ParseTypeHandle("Struct<a:Int32, b:String>")
    ),
    ($x) -> {
        return AsTuple(
            FormatType($x.Type),
            $x.Name
        )
    }
); -- [("Int32","a"), ("String","b")]
```

## StructTypeHandle

**Signature**
```
StructTypeHandle(List<Struct<Name:String, Type:TypeHandle>>)->structure type handle
```
Building a structure type handle from the item type handles and names passed to the argument as a list. The reverse function is [StructTypeComponents](#structtypecomponents).

**Examples:**
```yql
SELECT FormatType(
    StructTypeHandle(
        AsList(
            AsStruct(ParseTypeHandle("Int32") as Type,"a" as Name),
            AsStruct(ParseTypeHandle("String") as Type, "b" as Name)
        )
    )
); -- Struct<'a':Int32,'b':String>
```

## DictTypeComponents

**Signature**
```
DictTypeComponents(DictTypeHandle)->Struct<Key:TypeHandle, Payload:TypeHandle>
```
Getting the type-key handle and the type-value handle from the dict type handle passed to the argument. The reverse function is [DictTypeHandle](#dicttypehandle).

**Examples:**
```yql
$d = DictTypeComponents(ParseTypeHandle("Dict<Int32,String>"));

SELECT
    FormatType($d.Key),     -- Int32
    FormatType($d.Payload); -- String
```

## DictTypeHandle

**Signature**
```
DictTypeHandle(TypeHandle, TypeHandle)->dict type handle
```
Building a dict type handle from the type-key and type-value handle passed to the arguments. The reverse function is [DictTypeComponents](#dicttypecomponents).

**Examples:**
```yql
SELECT FormatType(
    DictTypeHandle(
        ParseTypeHandle("Int32"),
        ParseTypeHandle("String")
    )
); -- Dict<Int32, String>
```

## ResourceTypeTag

**Signature**
```
ResourceTypeTag(ResourceTypeHandle)->String
```
Getting the tag from the resource type handle passed to the argument. The reverse function is [ResourceTypeHandle](#resourcetypehandle).

**Examples:**
```yql
SELECT ResourceTypeTag(ParseTypeHandle("Resource<foo>")); -- foo
```

## ResourceTypeHandle

**Signature**
```
ResourceTypeHandle(String)->resource type handle  
```
Building a resource type handle by the value of the tag passed to the argument. The reverse function is [ResourceTypeTag](#resourcetypetag).

**Examples:**
```yql
SELECT FormatType(ResourceTypeHandle("foo")); -- Resource<'foo'>
```

## TaggedTypeComponents

**Signature**
```
TaggedTypeComponents(TaggedTypeHandle)->Struct<Base:TypeHandle, Tag:String>
```
Getting the tag and base type from the tagged type handle passed to the argument. The reverse function is [TaggedTypeHandle](#taggedtypehandle).

**Examples:**
```yql
$t = TaggedTypeComponents(ParseTypeHandle("Tagged<Int32,foo>"));

SELECT FormatType($t.Base), $t.Tag; -- Int32, foo
```

## TaggedTypeHandle

**Signature**
```
TaggedTypeHandle(TypeHandle, String)->tagged type handle  
```
Building a tagged type handle by the base type handle and the tag name passed in the arguments. The reverse function is [TaggedTypeComponents](#taggedtypecomponents).

**Examples:**
```yql
SELECT FormatType(TaggedTypeHandle(
    ParseTypeHandle("Int32"), "foo"
)); -- Tagged<Int32, 'foo'>
```

## VariantTypeHandle

**Signature**
```
VariantTypeHandle(StructTypeHandle)->variant type handle on top of structure
VariantTypeHandle(TupleTypeHandle)->variant type handle on top of tuple
```
Building a variant type handle by the underlying type handle passed to the argument. The reverse function is [VariantUnderlyingType](#variantunderlyingtype).

**Examples:**
```yql
SELECT FormatType(VariantTypeHandle(
    ParseTypeHandle("Tuple<Int32, String>")
)); -- Variant<Int32, String>
```

## VoidTypeHandle and NullTypeHandle

**Signature**
```
VoidTypeHandle()->Void type handle
NullTypeHandle()->Null type handle
```
Building a Void and Null type handle, respectively.

**Examples:**
```yql
SELECT FormatType(VoidTypeHandle()); -- Void
SELECT FormatType(NullTypeHandle()); -- Null
```

## CallableTypeComponents

**Signature**
```
CallableTypeComponents(CallableTypeHandle)->
Struct<
    Arguments:List<Struct<
        Flags:List<String>,
        Name:String,
        Type:TypeHandle>>,
    OptionalArgumentsCount:Uint32,
    Payload:String,
    Result:TypeHandle
>
```
Getting a description of the callable value type handle passed to the argument. The reverse function is [CallableTypeHandle](#callabletypehandle).

**Examples:**
```yql
$formatArgument = ($x) -> {
    return AsStruct(
        FormatType($x.Type) as Type,
        $x.Name as Name,
        $x.Flags as Flags
    )
};

$formatCallable = ($x) -> {
    return AsStruct(
        $x.OptionalArgumentsCount as OptionalArgumentsCount,
        $x.Payload as Payload,
        FormatType($x.Result) as Result,
        ListMap($x.Arguments, $formatArgument) as Arguments
    )
};

SELECT $formatCallable(
    CallableTypeComponents(
        ParseTypeHandle("(Int32,[bar:Double?{Flags:AutoMap}])->String")
    )
);  -- (OptionalArgumentsCount: 1, Payload: "", Result: "String", Arguments: [
    --   (Type: "Int32", Name: "", Flags: []),
    --   (Type: "Double?", Name: "bar", Flags: ["AutoMap"]),
    -- ])
```

## CallableArgument

**Signature**
```
CallableArgument(TypeHandle, [String, [List<String>]])->Struct<Flags:List<String>,Name:String,Type:TypeHandle>
```
Packing the callable value argument description into the structure to pass to the [CallableTypeHandle](#callabletypehandle) function based on the following arguments:

1. Argument type handle.
2. Optional argument name. The default value is an empty string.
3. Optional argument flags as a list of strings. The default value is an empty list. Supported flags: "AutoMap".

## CallableTypeHandle

**Signature**
```
CallableTypeHandle(TypeHandle, List<Struct<Flags:List<String>,Name:String,Type:TypeHandle>>, [Uint32, [String]])->callable value type handle
```
Building a callable value type handle based on the following arguments:

1. Returned value type handle.
2. A list of argument descriptions obtained via the [CallableArgument](#callableargument) function.
3. Optional number of optional arguments in the callable value. The default value is 0.
4. Optional tag for the callable value type. The default value is an empty string.

The reverse function is [CallableTypeComponents](#callabletypecomponents).

**Examples:**
```yql
SELECT FormatType(
    CallableTypeHandle(
        ParseTypeHandle("String"),
        AsList(
            CallableArgument(ParseTypeHandle("Int32")),
            CallableArgument(ParseTypeHandle("Double?"), "bar", AsList("AutoMap"))
        ),
        1
    )
);  -- Callable<(Int32,['bar':Double?{Flags:AutoMap}])->String>
```

## LambdaArgumentsCount

**Signature**
```
LambdaArgumentsCount(LambdaFunction)->Uint32
```
Getting the number of arguments in a lambda function.

**Examples:**
```yql
SELECT LambdaArgumentsCount(($x, $y)->($x+$y))
; -- 2
```

## LambdaOptionalArgumentsCount

**Signature**
```
LambdaOptionalArgumentsCount(LambdaFunction)->Uint32
```
Getting the number of optional arguments in a lambda function.

**Examples:**
```yql
SELECT LambdaOptionalArgumentsCount(($x, $y, $z?)->(if($x,$y,$z)))
; -- 1
```

