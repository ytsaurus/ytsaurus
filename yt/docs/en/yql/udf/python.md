# Instructions for writing Python user-defined functions (UDFs) for YQL

## Introduction
Sometimes it can be difficult to describe tasks using only a declarative language like SQL. You may find it more convenient to express declaratively which data to join and how to group it, and to describe the applied logic of data fragment processing in an imperative style. For this use case, YQL provides a mechanism for writing and calling user-defined functions (UDFs) in Python 3.6 or Python 2.7.


## Defining and calling a UDF
To use a Python UDF in a YQL query, follow these steps:

1. Pass a string containing a Python script that defines the function to the query. This function will be called as a UDF.
2. In your query, declare the function name and its signature (input and output value types).
3. Call the function in the query body and pass the arguments to it.

Each of the three steps is described in detail below.

### String containing a Python script
You can add your script as:

  * Regular string literal. For example: `"def foo(): return 'bar'"`. This option isn't convenient for complex multi-line scripts.
  * Multi-line string literal (YQL extension). This literal is enclosed in `@@` characters and works roughly similar to triple quotes in Python. The `#py` comment enables Python syntax highlighting in the YQL editor:
    ```yql
    @@#py
    def foo():
        return b'bar'
    @@
    ```
  * Named file attached to the query. You can include the script content in your query using the `FileContent("foo.py")` function.

<!--См. инструкции, как приложить файл к запросу [в веб-интерфейсе](../interfaces/web.md#attach) или [в консольном клиенте](../interfaces/cli.md#attach).-->

### Declare function name and signature
Since Python is a dynamically typed language and YQL is statically typed, you need a method to define the types.
The **name and signature** of the function called from the Python script must be known in advance before executing a YQL query. If the declared signature doesn't match the actual data that the function returns, the query will result in a runtime error — it's up to the user to ensure consistency.
There are three ways to specify the function **signature**:

  * In the query itself.
  * As an annotation of arguments and the output value (for Python 3 only).
  * As docstring in a Python function.

Since running arbitrary Python code in YQL isn't safe, the second and third options require running a separate Map operation on YT, which takes about 30–60 seconds.
{% cut "Notes on implementation" %}


Calling a separate function from the script body rather than running the entire script is implemented to ensure that not only a string value, but more complex data types (more on them below) can be returned in a UDF. So the approach `if __name__ == '__main__': ...` that is popular in Python console utilities is of no use in this case, since the returned value can only be obtained through the standard script output (stdout).

{% endcut %}

Example of declaring a function name and signature:
`$f = Python3::foo(Callable<()->String>, $script);`
Where

* `$f` and `$script` are [named YQL expressions](../syntax/expressions.md#named-nodes). In this example:
    * `$f` is a ready-to-use function (contains the function name, its signature, and the script body).
    * `$script` is the Python script body defined in one of the ways described in the previous section.
* `Python3::` is a fixed namespace for Python 3.x functions. It's used to distinguish this construct from UDFs in other interpreted languages or C++. The `Python::` alias is available as well. Specify `Python2::` to use Python 2.7.
* `foo`: Name of the function to be called, defined in the `$script`.
* `Callable<()->String>`: Function signature description. The empty brackets indicate that the function doesn't accept any arguments. String after the arrow means that the function returns a string. Example of a function signature with arguments: `Callable<(String, Uint32)->Double>`. [Detailed documentation on the format for describing data types](../types/type_string.md).

As an alternative to using a string describing the function signature, you may also pass the type of the called expression created via [functions for working with data types](../builtins/types.md). This option is usually more convenient if the query uses multiple functions that handle the same composite data types, or if a composite data type can be obtained via [TypeOf](../builtins/types.md#typeof) from a table column or row.

If you omit the function signature description, YQL will extract it either from the annotations or from the beginning of the function's docstring to the first empty string or end of the docstring.

When describing a function with annotations, all arguments and the output value must be annotated. You can't use `*args` and `**kwargs`, and default values can only be `None` while the corresponding argument types must be `Optional`.

### Type annotations

Type annotations in Python 3 should use types from the `yql.typing` module: [primitive types](../types/primitive.md) and, generally, same-name constructor functions of the types from the [text type description](../types/type_string.md), except that square brackets are used instead of angle brackets due to Python restrictions.

To specify the types of `tuple` or `structure` without elements, use the `EmptyTuple` and `EmptyStruct` annotations.

To describe the type of a callable value, use the `Callable` annotation, passing the number of optional arguments, the output value type, and argument descriptions in square brackets. The argument description is either a type or `slice` in `argument-name:type:flags` format. If you don't need to specify the argument name, use an empty string. You can omit the last `slice` component, but if it is specified, it must be a `set` of flags. Currently, the only possible flag value is `AutoMap`. If you need to specify flags or a name for an argument of a particular UDF, specify the `slice` object in the same form in the annotation.

**Example of using annotations:**
```yql
$script = @@#py
from yql.typing import *


def foo(
    x:Optional[Int32],
    y:Struct['a':Int32, 'b':Int32],
    z:slice("name", Int32, {AutoMap})) -> Optional[Int32]:
    """
       foo function description
    """
    return x + y.a + z
@@;

$udf = Python3::foo($script);
SELECT $udf(1, AsStruct(2 AS a, 3 AS b), 10 AS name); -- 13

```

**Example of using docstring:**
```yql
$script = @@#py
def foo():
    """
       ()->String

       foo function description
    """
    return b"bar"
@@;

$udf = Python3::foo($script);
SELECT $udf(); -- bar
```

### Calling a UDF in a query
You can call the function obtained after declaring the signature in the same way as any other built-in YQL function, passing arguments in parentheses. For example, `$f()` or `$udf("test", 123)`.

## Data types

### Primitive (Data type)

[Primitive data types](../types/primitive.md)

### Nullable types
In YQL, table columns (and any values as well) can be either non-nullable or [potentially nullable](../types/optional.md) (in SQL terms, nullable means allowing `NULL`). Functions in YQL, including Python functions, must declare in their signature whether they can handle potentially nullable input values, and whether they need to be able to return a nullable value (`NULL`, or "empty Optional").

When specifying a signature for a Python UDF, nullable types are indicated by appending the `?` suffix; for example, `String?` or `Double?`. For Python, `NULL` values correspond to `None` in the argument or in the returned function value.

If an argument is marked as allowing `NULL` (Optional/Nullable), this **doesn't mean** that it can be omitted when calling the function.

When describing the function signature, optional arguments are specified in square brackets after the required ones. Python will pass `None` as the values of all optional arguments not specified when calling the function.

For example, if a function's arguments are declared as `(String, [Int32?, Bool?])`, it means that the function takes three arguments: one required String argument and two optional Int32 and Bool arguments, respectively.

### Containers

| **Name** | **Signature declaration** | **Signature example** | **Representation in Python** |
| --- | --- | --- | --- |
| List | `List<Type>` | `List<Int32>` | `list-like object` [(learn more)](#python-container-wrappers) |
| Dictionary | `Dict<KeyType,ValueType>` | `Dict<String,Int32>` | `dict-like object` [(learn more)](#python-container-wrappers) |
| Set | `Set<KeyType>` | `Set<String>` | `dict-like object` [(learn more)](#python-container-wrappers), value type — `yql.Void` |
| Tuple | `Tuple<Type1,...,TypeN>` | `Tuple<Int32,Int32>` | `tuple` |
| Structure | `Struct<Name1:Type1,...,NameN:TypeN>` | `Struct<Name:String,Age:Int32>` | Python2 : `object` /  Python3 : `StructSequence` |
| Stream | `Stream<Type>` | `Stream<Int32>` | `generator` |
| Variant on tuple | `Variant<Type1,Type2>` | `Variant<Int32,String>` | `tuple with index and object` |
| Variant on structure | `Variant<Name1:Type1,Name2:Type2>` | `Variant<value:Int32,error:String>` | `tuple with field name and object` |
| Enumeration | `Enum<Name1, Name2>` | `Enum<Foo,Bar>` | `tuple with field name and yql.Void` |

Containers can be nested. For example, `List<Tuple<Int32,Int32>>`.

### Special types

* **Callable**: A callable value that can be executed by passing arguments in parentheses in SQL syntax or with Apply in {% if audience == "internal" %}[s-expressions]({{yql.s-expressions-link}}){% else %}s-expressions{% endif %}. Since the signature of the called expressions must be statically known, all Python UDFs that return callable values must specify their signature as well. For example, `(Double)->(Bool)->Int64`: a UDF takes Double as input and returns a callable value with the ` (Bool)->Int64` signature. You can also use this approach to build longer chains of callable values.
* **Resource**: An opaque pointer to a resource you can pass between user-defined functions. The type of the returned and accepted resource is declared inside a function with a string label. This label is used to match them in advance to ensure that the resources aren't passed between incompatible functions. In a <!--[С++ UDF](cpp.md)-->C++ UDF, you can set a custom resource name, whereas Python UDFs currently only allow a fixed resource name, `Python2`.{% if audience == "internal" %} [Example]({{yql.link}}/Tutorial/yt_22_User_Defined_Aggregation_Functions).{% endif %}
* **Void**: No other type; it is the `NULL` literal type.

## Automatic type conversion when interacting with Python UDFs
As input:

  * `String`,`Yson` to `bytes` (`str` for Python 2).
  * `Utf8`,`Json` to `str` (`unicode` for Python 2).
  * `Void` type object: to `yql.Void` (`import yql` needed).

As output:

  * `long/int` to `Int8`, `Int16`, `Int32`, `Int64`, `Uint8`, `Uint16`, `Uint32`, `Uint64`, `Float`, `Double`.
  * `float` to `Float`, `Double`.
  * `str` (`unicode` in Python 2) to `String`, `Utf8`, `Yson`, `Json`.
  * `bytes` (`str` in Python 2) to `String`, `Yson`.
  * `dict` or object to `Struct<...>`.
  * `tuple` to `List<...>`.
  * Any object to `Bool` according to [Python rules](https://docs.python.org/3/library/stdtypes.html?#truth-value-testing).
  * Generators, iterators, and iterable objects to `Stream<...>`.
  * Iterable objects to lazy `List<...>`.

See the table above for basic container representations.

You can configure automatic YSON unpacking on UDF boundaries using the `_yql_convert_yson` property of the Python function. You should put a tuple with functions for deserialization and serialization into the property.

**Example:**
```yql
$script = @@#py
import cyson

def f(s):
    abc = s.get(b"abc", 0)
    return (abc, s)

f._yql_convert_yson = (cyson.loads, cyson.dumps)
@@;

$udf = Python3::f(Callable<(Yson?)->Tuple<Int64,Yson?>>, $script);

SELECT $udf(CAST(@@{"abc"=1}@@ as Yson));
```

{% note warning "Attention!"%}

A string in Python 3 corresponds to the `Utf8` type in YQL, and the `String` type corresponds to `bytes` in Python 3.

Not following this rule can in many cases lead to unexpected and seemingly erroneous behavior, for example:

```yql
$script = @@#py
def getDict():
    return {'Key': 123}
@@;
$utfDict = Python::getDict(Callable<()-> Dict<Utf8, Int32>>,  $script);
$strDict = Python::getDict(Callable<()-> Dict<String, Int32>>, $script);
SELECT $utfDict()['Key'] AS good, $strDict()['Key'] AS bad, $strDict() AS strange;
```
In Python2, there was actually no difference between a text string and a byte sequence.

{% endnote %}

## Getting a list of structure fields {#python-struct-fields}

Instead of directly accessing structure fields using dot notation, it can sometimes be convenient to loop over all fields and use `getattr` to retrieve their values. You can do this by accessing `__class__.__match_args__` on a structure object.

Example:
```yql
$script = @@#py
def f(s):
    return ",".join(str(getattr(s,name)) for name in s.__class__.__match_args__)
@@;

$f = ($s)->{
    return Python::f(CallableType(0,Utf8,TypeOf($s)),$script)($s);
};

select $f(<|a:"foo"u,b:"bar"u|>); -- "foo,bar"
```

## Specifics of objects for lists and dictionaries passed to functions {#python-container-wrappers}

The lists and dictionaries that you pass to Python UDFs are not actually passed as real Python objects of type list or dict, but rather as special `yql.TList` and `yql.TDict` objects, which are read-only and help avoid copying all data into memory.

Notes on both objects:

* Both objects are read-only, meaning you can't make any changes to them without copying them first, because another part of the current query may use the same object. For example, a neighboring function.
* At the cost of potentially slow copying, you can retrieve a real list object (set, frozenset, and other objects are also possible if necessary) from them by iterating for partial copying or performing some transformations when copying. For dictionaries, you also need to call `iteritems()`. By setting the `_yql_lazy_input` attribute value to False on the Python function itself, you can enable automatic copying of lists and dictionaries to "list" and "dict". This mechanism works recursively for nested containers and also supports other types of containers, including Struct and Tuple.
* To get the number of elements, you can call `len(my_arg)`.
* The below documented method sets may be slightly outdated. You can find out the current set using `dir(my_arg)`.

Notes on `yql.TList`:

* `has_fast_len()` indicates whether you can quickly get the list length. If False is returned, the list is lazy.
* `has_items()` checks whether the list is empty.
* `reversed()` returns a reversed copy of the list.
* `skip(n)` and `take(n)` perform an equivalent function to `[n:]` and `[:n]` slices, respectively. An invalid index (n value) may throw the exception `IndexError`.
* `to_index_dict()` returns a dictionary with element indices as keys, which allows random access. It is disabled on the `yql.TList` object.

** Attention! These specifics are no longer recommended to be used and will be removed in future updates.**
You can use standard Python methods to organize random access to the list:

* list[index]: Access by index.
* list[start:stop:step]: List slice.
* reversed(list): List reversing.

**Example with _yql_lazy_input:**

```yql
$u = Python3::list_func(Callable<(List<Int32>)->Int32>, @@#py
def list_func(lst):
  return lst.count(1)
list_func._yql_lazy_input = False
@@);
SELECT $u(AsList(1,2,3));
```

{% if audience == "internal" %}
## Use of libraries {#how-to-use-libraries}

By default, a subset of libraries from [arcadia/contrib/python]({{source-root}}/contrib/python) and [arcadia/library/python]({{source-root}}/contrib/python) are available in Python3/ArcPython UDFs. The list of libraries is defined in ya.make.incl files for [Python 3 UDFs]({{source-root}}/yql/udfs/common/python/python3/ya.make.incl) and [ArcPython UDFs]({{source-root}}/yql/udfs/common/python/python_arc/ya.make.incl), respectively.

To add a new open-source Python library, go through the standard [procedure of adding it to Arcadia contrib]({{yql.pages.udf.contrib}}) and then submit a review to add the library to the [list of available libraries from Python 3 UDFs]({{source-root}}/yql/udfs/common/python/python3/ya.make.incl) / [ArcPython UDFs]({{source-root}}/yql/udfs/common/python/python_arc/ya.make.incl).

### CustomPython UDF

To use project-specific Python libraries, create your own Python3/Python2 UDF copy with any other PEERDIR set similar to the general one:

1. In ya.make, the [YQL_PYTHON3_UDF]({{source-root}}/yql/udfs/common/python/python3/ya.make?rev=4358725#L1)/[YQL_PYTHON_UDF]({{source-root}}/yql/udfs/common/python/python_arc/ya.make?rev=4358725#L3) macro is used.
2. Replace the [NAME and RESOURCE_NAME parameters]({{source-root}}/yql/udfs/common/python/python3/ya.make?rev=4358725#L6-7) with `CustomPython3`/`CustomPython2`, respectively.
3. Add PEERDIR to the Python libraries.
4. Further use is similar to C++ UDFs<!--[C++ UDF](cpp.md#use-udf-via-attach)-->.
5. When accessing a UDF in a query, use the `CustomPython3::`/`CustomPython2::` prefix instead of `Python3::`/`ArcPython::`.

**Example with CustomPython2**

The [collections_cofe UDF]({{source-root}}/analytics/collections/cofe_lib_udf/ya.make?rev=6215914) project in Arcadia.

```yql
PRAGMA File('libcollections_cofe.so', '{{sandbox-prx}}/979991403');
PRAGMA udf('libcollections_cofe.so');

$script = @@#py
from cofe.projects.collections.logs.parser.constants import (
    PATH_CLICK_CARD,
)

def get_click_path():
    return PATH_CLICK_CARD

@@;

$udf = CustomPython2::get_click_path(Callable<()->String>, $script);

SELECT $udf();
```

{% endif %}

## Please note

* When working on YT, the standard output (stdout) is used for system purposes. Therefore, you must not use it from UDFs.
* All heavy initialization should be done outside frequently called functions, either at the module level or using closures ([see the example below](#separate-init-example)). Otherwise, the operation may not only take an indefinitely long time, but also interfere with its neighbors in a MapReduce cluster.
* You can register multiple functions from a single script by repeating the function name and signature declaration the required number of times.

## Examples
### Hello World
```yql
$script = @@#py
def hello(name):
    return b'Hello, %s!' % name
@@;

$udf = Python3::hello(Callable<(String)->String>, $script);

SELECT $udf("world");
```
### Generators for creating lists
```yql
$script = @@#py
def gen():
    for x in range(1, 10):
        yield x
@@;
$udf = Python::gen(Callable<()->Stream<Int64>>, $script);
SELECT Yql::Collect($udf());
```
Can be used in combination with [PROCESS](../syntax/process.md) for stream table processing{% if audience == "internal" %}: [example]({{yql.link}}/Operations/XKcVKGHljqjgrlMWmpJqI4nvPfwU0Nx-6eMuuWlv3EQ=).{% else %}, for example:
```yql
$udf = Python::process(
Callable<
    (List<Struct<
        age:Int64,
        name:String?
    >>)->Stream<Struct<result:Int64>>
>, @@
def process(rows):
    for row in rows:
        result = row.age ** len(row.name or '')
        yield locals()
@@);

$users = (
    SELECT age, name FROM <table_name>
);

PROCESS $users
USING $udf(TableRows());
```
{% endif %}

### Splitting initialization and function call {#separate-init-example}

```yql
$script = @@#py
def multiply_by_filesize(path):
    with open(path, 'r') as f:
        size = len(f.read())

        def multiply(arg):
            return arg * size  # closure

        return multiply
@@;
$udf = Python3::multiply_by_filesize(
    Callable<(String)->Callable<(Int64)->Int64>>,
    $script
);
$callable = $udf(FilePath("foo.txt")); -- expensive initialization
SELECT $callable(123);                 -- cheap call of already initialized function
```

### Using input data type when declaring a signature
```yql
$func = ($obj) -> {
    $udf = Python3::func(
        CallableType(0, ParseType("String?"), TypeOf($obj)),
        FileContent("my_func.py")
    );
    RETURN $udf($obj);
};

SELECT $func(TableRow()) FROM my_table;
```

### SecureParam
<!--См. также общую [информацию о механизме SecureParam](cpp.md#secureparams).-->

To use SecureParam in Python, access the `_yql_secure_param(token)` attribute of the function whose signature is declared in the query, where `token` (string) is the name of the token whose value you need to obtain. The token name must be passed through the `SecureParam()` wrapper. Otherwise, the value won't be found. If the token isn't found by name, the function will end with an error.
Example of using SecureParam
```yql
$secParLen = @@#py
def MyFunc(token):
    return len(MyFunc._yql_secure_param(token))
@@;
$udf = Python::MyFunc(Callable<(String)->Int>, $secParLen);
SELECT $udf(SecureParam("token:default_yt"));
```

See [Functions for working with data types](../builtins/types.md).
