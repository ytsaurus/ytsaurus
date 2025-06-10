# Developing UDFs in C++

## Introduction
YQL currently supports user-defined functions (UDFs) in C++ and Python. The information below covers only C++. [Python UDFs](python.md) are covered on a separate page.

YQL offers a set of basic functions, and we recommend viewing the [list](list/index.md) of them.

## Terminology

* **A <span style="color: gray;">callable</span> value** is an entity with a fixed, strictly typed signature that, when called, can return a result based on its arguments.
* **A user-defined function** <span style="color: gray;">(UDF)</span> is a generator of callable values.
* **A <span style="color: gray;">module</span>** is a set of closely related functions with a common prefix. All module functions are distributed together in a single shared library (`.so`). If a module is project-specific, it's advisable to reflect this in its name by adding a prefix.
* **A <span style="color: gray;">registrator</span>** is an interface for adding modules and functions to the general registry.
* **A module <span style="color: gray;">group</span>** is a group of modules belonging to a structural unit or a large project. It serves only for logical separation of source code (see the next section) and is not visible from YQL/SQL.
* **Configuration parameters** are an optional tool for setting up a user-defined function (generator of callable values). You can use configuration parameters to change the behavior of callable values without recompilation.
    * `Type Config`, `User Type`: They are used by the function to define and declare the signature of the callable value; used when building a computation graph. They must be known at compile time, so `Type Config` must be an atom, and `User Type` must be a certain type (for example, obtained using the TypeOf operation).
    * `Run Config`: Function uses it to return the callable value with the previously declared signature. The Run Config value type is specified in the UDF code when declaring the signature of the callable value. It differs from a regular argument in that it is passed at initialization, not at each call.

## Examples
Examples of ready-to-use modules are available at [{{source-root}}/yql/essentials/udfs]({{source-root}}/yql/essentials/udfs).

See the [examples]({{source-root}}/yql/essentials/udfs/examples) subdirectory for very basic examples and other subdirectories (such as [common]({{source-root}}/yql/essentials/udfs/common)) for more realistic ones.

Before you move on to the next section, we recommend opening one or multiple examples to follow along: the section provides only an overview of the code, without complete listings.

## Interfaces (С++)

### TUnboxedValue (udf\_value.h)

Wrapper class for typed data. It provides a set of constructors for creating composite data types from primitive types along with a set of helpers for working with the former.

Numbers, intervals, dates (including dates with a timezone), and short strings up to 14 bytes are stored by value; the `IsEmbedded()` method returns `true` for them. Strings larger than 14 bytes are stored by pointer.

### TUnboxedValuePod(udf\_value.h)

It is a base class for TUnboxedValue. It does not
automatically count references to its value, unlike its subclass, which does it in the constructor and destructor. The key point here is that the TUnboxedValuePod class does not have constructors and destructors. It implements the entire logic of working with
typed data.

### IValueBuilder (udf\_value\_builder.h)

The Run method must return TUnboxedValue. For numeric types, you can use the type constructor, for example, `return TUnboxedValue<ui32>(123);`. To create TUnboxedValue with more complex content, you can pass IValueBuilder to the Run method. It can be used to call one of the `New***` methods to get the `I***ValueBuilder` interface, where *** is a String, Struct, Dict, or another type. Use this to add type-specific elements and then call `Build` to get the filled TUnboxedValue.
You can also create a Struct or Tuple with preallocation to store the contents. To do this, call the corresponding method `New***(const TType* type, TUnboxedValue*& items)` and fill the items.


See [{{yql.udf-ivaluebuilder-example}}]({{yql.udf-ivaluebuilder-example-link}}) for an example with structs.

### TBoxedValue (udf\_value.h)
A typical UDF class inherits from TBoxedValue and, in addition to the constructor, overrides the Run method where the main logic for handling one particular call should be placed. The IValueBuilder interface (more on it below below) and the passed arguments in the form of the TUnboxedValue array are passed to the Run arguments.

In the constructor, you can store a state in private class variables. Thus, a specific object of our TBoxedValue subclass becomes a callable value with potentially unique behavior.

For simple UDFs (without using YQL structures, C++ templates, inheritance, and so on), you can use the `SIMPLE_UDF(udfName, signature)` macro (you can find it in `udf_helpers.h`). `signature` is specified as `returnType(arg1, arg2, ..., argN)`.

Example:
```cpp
SIMPLE_UDF(TAbs, ui64(i64)) {
    i64 input = args[0].Get<i64>();
    ui64 result = static_cast<ui64>(input >= 0 ? input : -input);
    return TUnboxedValue(result);
}
```
The function will be registered as `Abs`, the argument type is `i64`, and the result is `ui64`.

An example where a successful result is not guaranteed:
```cpp
SIMPLE_UDF(TParseDuration, TOptional<ui64>(char*)) {
    TDuration result;
    auto input = args[0].AsStringRef();
    bool success = TDuration::TryParse(input, result);
    return success ? TUnboxedValue::Optional(result.Seconds()) : TUnboxedValue();
}
```
`Optional (type)` ([nullable type](../types/optional.md)) means that a value of the specified type may be missing (for example, if there is an error). In SQL, you can use the `IS [NOT] NULL` expression to check for this type in the result.

### IUdfModule (udf\_registrator.h)

For common modules, you can use the `SIMPLE_MODULE(moduleName, udfs...)` macro (you can find it in `udf_helpers.h`):

* The first argument is the name of the module class; when registering, the `T` prefix and the `Module` suffix will be removed.
* Next is a list of N constituent UDFs; when registering them, only the `T` prefix will be removed.

In the example `SIMPLE_MODULE(TFooModule, TBar, TBaz)`, the `Foo` module with two UDFs (`Bar` and `Baz`) will be available from YQL after registration (more on registration below).

#### Manual module implementation

A typical module class inherits from IUdfModule and must implement two methods:

* `GetAllFunctionNames`: In the passed `IFunctionNamesSink`, you need to call the `Add(name)` method for each UDF included in the module.
* `BuildFunctionTypeInfo`: Using the passed UDF name and Type Config, you need to call on the passed `IFunctionTypeInfoBuilder`:
  * `Args()->Add<ui32>()[->Add(foo)->Add...].Done()`:
    * To specify arguments and their types, for numbers and strings, you can specify the argument type using a template.
    * You can describe a composite type, such as a Struct or Dict, using the same-name `IFunctionTypeInfoBuilder` method, then passing it to the `Add` argument. For example:
```cpp
ui32 a = 0;
ui32 b = 0;
auto foo = builder.Struct()->AddField<char*>("A", &a).AddField<ui32>("B", &b).Build();
builder.Args()->Add(foo);
```
  * When describing types like `builder.List()->...`, you should copy examples of others as carefully as possible: calling a chain of builder methods in a wrong sequence or with wrong arguments often compiles fine, but crashes at runtime with a non-trivial backtrace. Example of such a chain:

```cpp
// udf returns list of lists of char*
auto retType = buider.List()->Item(builder.List()->Item<char*>().Build()).Build(); // Correct
auto retType = buider.List()->Item<char*>().Build(); // Wrong type, coredump with backtrace in casting to string deep in validating
auto retType = buider.List()->Build(); // No type at all, coredump with attempt to dereference nullptr deep in type machinery
builder.Returns(retType);
```
  * The arguments also pass the `ui32 flags` bitmask, which at the time of writing can contain only `(cpp)TFlags::TypesOnly` that, if present, does not require calling `builder.Implementation(new TMyUdf);` for the UDF corresponding to the passed name.


### IRegistrator (udf\_registrator.h)

For simplified module registration in the system, we recommend using a macro:
```cpp
REGISTER_MODULES(
    TFooModule,
    TBarModule<true>,
    TBarModule<false>
)
```
In this case, the module name is taken from the `Name()` method that must return `TStringRef` by value; the `SIMPLE_MODULE` macro creates it automatically.

#### Alternative method for advanced users

You need to declare the `Register` function of the following form, then register the module(s) in the passed interface and export the ABI version:
```cpp
extern "C" YQL_UDF_API void Register(NKikimr::NUdf::IRegistrator& registrator, ui32 flags)
{
     registrator->AddModule("Foo", new NDetail::TFooModule(flags));
}

extern "C" YQL_UDF_API ui32 AbiVersion()
{
    return CurrentAbiVersion();
}
```
You can get the list of flags with `IUdfModule::TFlags`. At the time of writing, only `TypesOnly` is implemented. It is used as an indication that no real computations will be performed yet, because the computation graph is still being built, and only function signatures are required at this stage.

### Exception and error handling

To return fatal exceptions to the user, use the following block:

```cpp
try {
   ...
} catch (const std::exception& e) {
    UdfTerminate(e.what());
}
```

Instead of `e.what()`, you can write a human-readable error description if possible.

{% note warning "Attention" %}

We strongly recommend against completely terminating program execution using `abort`, `exit`, `Y_ABORT_UNLESS`, `Y_ABORT`, and other similar mechanisms from UDF code, because this would limit its scope to environments where a separate process is created for each calculation. The `UdfTerminate` implementation is aware of the current environment and will invoke the error handling mechanism it expects.

{% endnote %}


