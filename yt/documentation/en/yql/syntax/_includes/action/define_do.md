
## DEFINE ACTION {#define-action}

Specifies a named action that is a parameterizable block of multiple top-level expressions.

**Syntax**

1. `DEFINE ACTION`: action definition.
1. [Name of the action](../../expressions.md#named-nodes), as part of which the declared action is further available for the call.
1. The values of parameter names are listed in parentheses.
1. `AS` keyword.
1. List of top-level expressions.
1. `END DEFINE`: The marker of the last expression inside the action.

One or more of the last parameters can be marked with a question mark `?` as optional. If they are omitted during the call, they will be assigned the `NULL` value.

## DO {#do}

Executes an `ACTION` with the specified parameters.

**Syntax**
1. `DO`: Executing an action.
1. The named expression for which the action is defined.
1. The values to be used as parameters are listed in parentheses.

`EMPTY_ACTION`: An action that does nothing.

<!-- In fact, if user file system integration is supported in the product. YQL service over YDB may also be here. -->

{% note info "Note" %}

In large queries, you can declare actions in separate files and connect them to the main query using [EXPORT](../../export_import.md#export) + [IMPORT](../../export_import.md#import) to get multiple logical parts that are easier to navigate than one long text. An important nuance: the `USE my_cluster;` directive in the import query does not affect behavior of actions declared in other files.

{% endnote %}


**Example**

```yql
DEFINE ACTION $hello_world($name, $suffix?) AS
    $name = $name ?? ($suffix ?? "world");
    SELECT "Hello, " || $name || "!";
END DEFINE;

DO EMPTY_ACTION();
DO $hello_world(NULL);
DO $hello_world("John");
DO $hello_world(NULL, "Earth");
```

