# Python user-defined functions (UDFs) in YQL

## Introduction

Sometimes it can be difficult to describe data processing tasks using only a declarative language like SQL. You may find it more convenient to use an imperative language as well. For this purpose, YQL supports the creation and use of user-defined functions (UDFs).

## Python UDF

With the Python UDF feature, you can define and use Python functions inside a query.

## How to use

To use a Python UDF in a YQL query, follow these steps:

1. Define your function in a Python script and include this script as a string in your query. This function will be called as a UDF.
2. In your query, declare the function name and its signature (input and output data types).
3. Call the function in the query body and pass the necessary arguments to it.

Each of these steps is explained in detail below.

### Write Python script

You can add your script as:

- Regular string literal. For example, `$script = "def foo(): return 'bar'"`.
- Multi-line string literal (YQL extension). You must enclose this literal in the `@@` characters, which are similar to triple quotes in Python. The `#py` comment enables Python syntax highlighting in the YQL editor:

  ```yql
  $script = @@#py
  def foo():
      return b'bar'
  @@
  ```

- Named file attached to the query. You can include the file content in your query by using the [FileContent](../../../yql/builtins/basic.md#file-content-path) function. For example, `$script = FileContent("foo.py")`.

### Declare function name and signature

Since Python is a dynamically typed language and YQL is statically typed, you need to define the types in advance. The name and signature of the function called from the Python script must be known before executing a YQL query. If the types you declare in the function signature do not match the actual data that the function returns, you get a runtime error.

Example of declaring a function name and signature:
`$f = SystemPython3_8::foo(Callable<()->String>, $script);`. In this example:

- `$f` and `$script`: [Named YQL expressions](../../../yql/syntax/expressions.md#named-nodes).
  - `$f`: Ready-to-use function.
  - `$script`: Python script mentioned in the previous section.
- `SystemPython3_8::`: Fixed prefix for declaring a Python function. `System` means that Python will be taken from the environment at runtime. `3_8` indicates the Python version to be used.
  - As of the time of writing this article, versions `3_8` – `3_12` are available.
- `foo`: Name of the function to be called, defined in the `$script`.
- `Callable<()->String>`: Function signature description. The empty brackets indicate that the function doesn't accept any arguments. `String` after the arrow means that the function returns a string. Example of a function signature with arguments: `Callable<(String, Uint32)->Double>`.

### Use UDF in your query

You can call the function obtained after declaring the signature in the same way as any other built-in YQL function. For example, `$f()` or `$udf("test", 123)`.

## Data types

To find out which data types are allowed in the function signature, see the [YQL data types](../../../yql/types/index.md) section.

### Containers

Containers are converted into Python objects according to the following rules:

| **Name** | **Signature declaration** | **Signature example** | **Representation in Python** |
| --- | --- | --- | --- |
| List | `List<Type>` | `List<Int32>` | `list-like object` [(learn more)](#python-container-wrappers) |
| Dictionary | `Dict<KeyType,ValueType>` | `Dict<String,Int32>` | `dict-like object` [(learn more)](#python-container-wrappers) |
| Tuple | `Tuple<Type1,...,TypeN>` | `Tuple<Int32,Int32>` | `tuple` |
| Structure | `Struct<Name1:Type1,...,NameN:TypeN>` | `Struct<Name:String,Age:Int32>` | `StructSequence` |
| Stream | `Stream<Type>` | `Stream<Int32>` | `generator` |
| Variant on tuple | `Variant<Type1,Type2>` | `Variant<Int32,String>` | `tuple with index and object` |
| Variant on structure | `Variant<Name1:Type1,Name2:Type2>` | `Variant<value:Int32,error:String>` | `tuple with field name and object` |

You can nest containers into each other. For example, `List<Tuple<Int32,Int32>>`.

### Specifics of objects for lists and dictionaries passed to functions {#python-container-wrappers}

When you use `List` and `Dict`, the read-only `yql.TList` and `yql.TDict` objects are passed to the function arguments.

The object's specifics:

- They are read-only. This means that you can't make any changes to these objects without copying them because another part of the current query may use the same object. For example, a neighboring function.
- At the cost of potentially slow copying, you can get a real list object from them by iterating for partial copying. For dictionaries, you also need to call `iteritems()`. By setting the `_yql_lazy_input` attribute value to False on the function itself, you can enable automatic copying of lists and dictionaries to "list" and "dict". This mechanism works recursively for nested containers.
- To get the number of elements, you can call `len(my_arg)`.
- To get the latest set of available methods, call `dir(my_arg)`. [Example](#tlist-methods).

The `yql.TList` methods:

- `has_fast_len()`: Indicates whether you can get the list length quickly. If False is returned, the list is "lazy".
- `has_items()`: Checks whether the list is empty.
- `reversed()`: Returns a reversed copy of the list.
- `skip(n)` and `take(n)`: Similar to `[n:]` and `[:n]` slices, respectively.
- `to_index_dict()`: Returns a dictionary with element numbers as keys. This allows you to access these elements by their indices.

Example with the `_yql_lazy_input` attribute:

```yql
$u = SystemPython3_8::list_func(Callable<(List<Int32>)->Int32>, @@#py
def list_func(lst):
  return lst.count(1)
list_func._yql_lazy_input = False
@@);
SELECT $u(AsList(1,2,3));
```

## Changing the environment {#environment}

### Interaction with Python from the environment

The `SystemPython` UDF is called in a separate operation within a custom script.

The interaction with Python is done via the dynamic `libpython3.N.so` library, which is linked at runtime. If you replace this library in the environment, it will change the Python used and available libraries.

To change the environment from a YQL operation, use the [yt.DockerImage](../../../yql/syntax/pragma.md#ytdockerimage) and [yt.LayerPaths](../../../yql/syntax/pragma.md#ytlayerpaths) pragmas.

They will affect the corresponding user script parameters described in the [Operation options - User script options](../../../user-guide/data-processing/operations/operations-options.md#user_script_options) section.

{% note info "Default environment" %}

The default environment used for jobs is configured by the cluster administrator. It may not include the required Python version.

{% endnote %}

Example of using the TensorFlow library with `pragma yt.DockerImage`:

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

## Specifics

When using UDFs, the standard output (stdout) is used for system purposes. Therefore, you must not use it.

## Examples

### Hello World

```yql
$script = @@#py
def hello(name):
    return b'Hello, %s!' % name
@@;

$udf = SystemPython3_8::hello(Callable<(String)->String>, $script);

SELECT $udf("world"); -- "Hello, world!"
```

### String and number concatenation

```yql
$script = @@#py
def concat(a, b):
    return a.decode("utf-8") + str(b)
@@;
$concat = SystemPython3_8::concat(Callable<(String?, Int64?)->String>, $script);

SELECT $concat(name, age) FROM `//tmp/sample`;
```

### Retrieving the latest list of `yql.TList` methods {#tlist-methods}

```yql
$u = SystemPython3_8::get_dir(Callable<(List<Int32>)->List<String>>, @@#py
def get_dir(lst):
  return dir(lst)
@@);
SELECT $u(AsList());
```

### Inferencing Llama

You need an appropriate environment that you can get using the following Dockerfile:

```dockerfile
FROM ollama/ollama:0.3.9
RUN apt install -y python3.11 libpython3.11 python3-pip
RUN python3.11 -m pip install ollama
```

Use the specified image to call Llama from Python:

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

### I get the error `Module not loaded for script type: SystemPython3_8` or `Module SystemPython3_8 is not registered`

This indicates that the cluster doesn't support this functionality. The cluster must have QueryTracker and YqlAgents with an image version no older than [YTsaurus QueryTracker 0.0.8](https://github.com/ytsaurus/ytsaurus/releases/tag/docker%2Fquery-tracker%2F0.0.8). Ask your cluster administrators to update these components.

### I get the error `libpython3.8.so.1.0: cannot open shared object file: No such file or directory`

This indicates that the environment of the job that is executing the function is missing the dynamic library `libpython3.8.so.1.0`. Make sure that you're using the same Python version that is used in the job environment. [Read more](#environment).

If you're using the [yt.DockerImage](../../../yql/syntax/pragma.md#ytdockerimage) pragma, make sure that the cluster supports this method of changing the environment. [Read more](../../../admin-guide/prepare-spec.md#job-environment).
