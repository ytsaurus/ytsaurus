## Linear types

Most YQL types are immutable, meaning expressions return new values instead of modifying existing ones.

This approach, which is common in functional programming languages, allows for more aggressive optimizations such as eliminating common subexpressions or caching results. However, in some scenarios, this may lead to slower query execution. For example, when changing a value in a list or map, you need to either return its complete copy or use persistent data structures, which also introduces additional overhead.

Linear types offer a different approach: unlike immutable output values, linear type values can't be reused. Similar to a relay baton, they are passed from their point of creation to their point of consumption, where they are converted into regular immutable values.

Linear types were added in version [2025.04](../changelog/2025.04.md).

Linear types are described by a single parameter, type T, and come in two variants: statically verifiable `Linear<T>` and runtime verifiable `DynamicLinear<T>`.

Statically verifiable types are more efficient but are limited in their composition capabilities.

The `T` parameter of a linear type typically uses the `Resource` type. In this case, user-defined functions (UDFs) can pass data between each other with guaranteed protection against reuse in another query, allowing for more efficient implementations.

Linear types are non-serializable, meaning they can't be read from or written to tables and can only be used within expressions.

Functions that accept or return linear types are categorized into three classes:
* Producers: Functions where a linear type appears in the output but not in the parameters.
* Transformers: Functions where a linear type appears both in the parameters and in the output.
* Consumers: Functions where a linear type appears in the parameters but not in the output.

A producer must accept at least one parameter containing a dependent expression because it's possible to generate multiple independent values of linear types from the input.
We recommend creating and consuming linear types within the [`Block`](../builtins/basic.md#block) function, which allows passing an anonymous dependent expression as a `lambda` parameter.

### Validation of static linear types (`Linear<T>`)

* Validation of `linear` types is the final stage of optimization. If the optimizer successfully eliminates reuse of the expression, no error occurs.
* A `linear` type can't be a `lambda` parameter.
* A `linear` type can't be returned by a `lambda`.
* A `linear` type can be used either by itself or within a `Struct/Tuple` field (without nesting). In this case, if possible, the optimizer monitors individual usage of `Struct/Tuple` fields via the field access operator (dot).

To use values of linear types within other container types, such as lists, use a `DynamicLinear` type.

### Validation of dynamic linear types (`DynamicLinear<T>`)

* `FromDynamicLinear` or a user-defined function (UDF) can extract a value of this type, but only once. Attempts to extract the same value again result in a query execution error.
