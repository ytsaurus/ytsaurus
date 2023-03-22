# Special data types

| Type | Description |
----- | -----
| `Callable` | A callable value that can be executed by transmitting arguments in parentheses in YQL's SQL syntax or with the `Apply` function when [s-expressions](/docs/s_expressions) syntax is used. |
| `Resource` | Resource is an opaque pointer to a resource you can pass between the user defined functions (UDF). The type of the returned and accepted resource is declared inside a function using a string label. When passing a resource, YQL checks for label matching to prevent passing of resources between incompatible functions. If the labels mismatch, a type error occurs. |
| `Tagged` | Tagged is the option to assign an application name to any other type. |
| `Generic` | The data type used for data types. |
| `Unit` | A data type for non-computable entities (data sources and recipients, atoms, etc.). |
| `Null` | Void is a singular data type with the only possible null value. It's the type of the `NULL` literal and can be converted to any `Optional` type. |
| `Void` | Void is a singular data type with the only possible `null` value. |
| `EmptyList` | A singular data type with the only possible [] value. It's the type of the `[]` literal and can be converted to any `List` type. |
| `EmptyDict` | A singular data type with the only possible `{}` value. It's a type of the {} literal and can be converted to any `Dict` or `Set` type. |

