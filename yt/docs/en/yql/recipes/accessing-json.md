# Accessing values inside JSON with YQL

YQL provides two main methods for extracting values from JSON:

- Using [**JSON functions from the SQL standard**](../builtins/json.md). This approach is recommended for simple cases and for teams that are familiar with them from other DBMSs.
- Using [**YSON UDFs**](../udf/list/yson.md), built-in functions for [lists](../builtins/list.md), [dicts](../builtins/dict.md), and [lambdas](../syntax/expressions.md#lambda). This approach is more flexible and is tightly integrated with YQL's data type system, making it a better choice for complex cases.

Below are recipes that use the same input JSON to demonstrate how to use each option to check if a key exists, get a specific value, and extract a subtree.

## JSON functions

```yql
$json = @@{
    "friends": [
        {
            "name": "James Holden",
            "age": 35
        },
        {
            "name": "Naomi Nagata",
            "age": 30
        }
    ]
}@@j;

SELECT
    JSON_EXISTS($json, "$.friends[*].name"), -- True
    CAST(JSON_VALUE($json, "$.friends[0].age") AS Int32), -- 35
    JSON_QUERY($json, "$.friends[0]"); -- {"name": "James Holden", "age": 35}
```

`JSON_*` functions expect the `Json` data type as input. In this example, the string literal has the `j` suffix, marking it as `Json`. Tables can store data either in JSON format or as a string representation. To convert data from `String` to `JSON`, use the `CAST` function: for example, `CAST(my_string AS JSON)`.

## Yson UDF

This approach typically combines multiple functions and expressions, so one query can include different strategies.

### Converting the whole JSON into YQL containers

```yql
$json = @@{
    "friends": [
        {
            "name": "James Holden",
            "age": 35
        },
        {
            "name": "Naomi Nagata",
            "age": 30
        }
    ]
}@@j;

$containers = Yson::ConvertTo($json, Struct<friends:List<Struct<name:String?,age:Int32?>>>);
$has_name = ListAny(
    ListMap($containers.friends, ($friend) -> {
        return $friend.name IS NOT NULL;
    })
);
$get_age = $containers.friends[0].age;
$get_first_friend = Yson::SerializeJson(Yson::From($containers.friends[0]));

SELECT
    $has_name, -- True
    $get_age, -- 35
    $get_first_friend; -- {"name": "James Holden", "age": 35}
```

You **don't** have to convert the whole JSON object into a structured combination of containers. Some fields may be omitted if not used, and some subtrees may be left in an unstructured data type like `Json`.

### Working with in-memory representation

```yql
$json = @@{
    "friends": [
        {
            "name": "James Holden",
            "age": 35
        },
        {
            "name": "Naomi Nagata",
            "age": 30
        }
    ]
}@@j;

$has_name = ListAny(
    ListMap(Yson::ConvertToList($json.friends), ($friend) -> {
        return Yson::Contains($friend, "name");
    })
);
$get_age = Yson::ConvertToInt64($json.friends[0].age);
$get_first_friend = Yson::SerializeJson($json.friends[0]);

SELECT
    $has_name, -- True
    $get_age, -- 35
    $get_first_friend; -- {"name": "James Holden", "age": 35}
```

## See also

- [{#T}](modifying-json.md)
