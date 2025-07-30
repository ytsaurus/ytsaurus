# YTsaurus Python Style Guide


## Scope

This describes in brief coding conventions used in the most part of our Python code base. The rules described here apply at least to the following top-level directories:

* `yt/python` — the root directory for YT Python client code and client tools
* `yt/cron` — YT Cron executor and scripts
* `yt/odin` — YT Odin executor and checks
* `yt/yt/tests/integration` - test for yt core


## General

In general we use [PEP 8](https://peps.python.org/pep-0008/) with a slight YT C++ flavour.

The syntax used must be consistent with `Python 3.8+`


## Code Organization


### Imports

Imports should be organized in order from specific to general:
- first the yt libraries
- then the third-party modules
- then the standard modules


### Quotes

We use double quotes for string literals.


### Maximum Line Length

Limit all lines to a maximum of 100 characters.


### Indents

The closing brace/bracket/parenthesis on multiline constructs may be lined up under the first character of the line that starts the multiline construct, as in:

```python
def some_function(
    p1: int,
    p2: str,
) -> int:
    ...

my_dict = {
    "key_1": {
        "sub_key_1": "val_1",
    },
}

result = some_function(
    p1=call_long_named_function_number_one(with_long_parameter_name),
    p2="This is a loooooooooooooooooooooooooooooooooooooooong literal",
)

result = some_function(1, "short literal")
```


### Type annotations

We expects type hints for all public methods and variables.


### Docstring

Public methods should be briefly described with docstring.

