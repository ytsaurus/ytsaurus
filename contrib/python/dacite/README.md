![](https://user-images.githubusercontent.com/1078369/212840759-174c0f2b-d446-4c3a-b97c-67a0b912e7f6.png)

# dacite

[![Build Status](https://travis-ci.org/konradhalas/dacite.svg?branch=master)](https://travis-ci.org/konradhalas/dacite)
[![Coverage Status](https://coveralls.io/repos/github/konradhalas/dacite/badge.svg?branch=master)](https://coveralls.io/github/konradhalas/dacite?branch=master)
[![License](https://img.shields.io/pypi/l/dacite.svg)](https://pypi.python.org/pypi/dacite/)
[![Version](https://img.shields.io/pypi/v/dacite.svg)](https://pypi.python.org/pypi/dacite/)
[![Python versions](https://img.shields.io/pypi/pyversions/dacite.svg)](https://pypi.python.org/pypi/dacite/)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/ambv/black)

This module simplifies creation of data classes ([PEP 557][pep-557])
from dictionaries.

## Installation

To install dacite, simply use `pip`:

```
$ pip install dacite
```

## Requirements

Minimum Python version supported by `dacite` is 3.7.

## Quick start

```python
from dataclasses import dataclass
from dacite import from_dict


@dataclass
class User:
    name: str
    age: int
    is_active: bool


data = {
    'name': 'John',
    'age': 30,
    'is_active': True,
}

user = from_dict(data_class=User, data=data)

assert user == User(name='John', age=30, is_active=True)
```

## Features

Dacite supports following features:

- nested structures
- (basic) type checking
- optional fields (i.e. `typing.Optional`)
- unions
- generics
- forward references
- collections
- custom type hooks
- case conversion

## Motivation

Passing plain dictionaries as a data container between your functions or
methods isn't a good practice. Of course you can always create your
custom class instead, but this solution is an overkill if you only want
to merge a few fields within a single object.

Fortunately Python has a good solution to this problem - data classes.
Thanks to `@dataclass` decorator you can easily create a new custom
type with a list of given fields in a declarative manner. Data classes
support type hints by design.

However, even if you are using data classes, you have to create their
instances somehow. In many such cases, your input is a dictionary - it
can be a payload from a HTTP request or a raw data from a database. If
you want to convert those dictionaries into data classes, `dacite` is
your best friend.

This library was originally created to simplify creation of type hinted
data transfer objects (DTO) which can cross the boundaries in the
application architecture.

It's important to mention that `dacite` is not a data validation library.
There are dozens of awesome data validation projects and it doesn't make
sense to duplicate this functionality within `dacite`. If you want to 
validate your data first, you should combine `dacite` with one of data 
validation library.

Please check [Use Case](#use-case) section for a real-life example.

## Usage

Dacite is based on a single function - `dacite.from_dict`. This function
takes 3 parameters:

- `data_class` - data class type
- `data` - dictionary of input data
- `config` (optional) - configuration of the creation process, instance
of `dacite.Config` class

Configuration is a (data) class with following fields:

- `type_hooks`
- `cast`
- `forward_references`
- `check_types`
- `strict`
- `strict_unions_match`
- `convert_key`

The examples below show all features of `from_dict` function and usage
of all `Config` parameters.

### Nested structures

You can pass a data with nested dictionaries and it will create a proper
result.

```python
@dataclass
class A:
    x: str
    y: int


@dataclass
class B:
    a: A


data = {
    'a': {
        'x': 'test',
        'y': 1,
    }
}

result = from_dict(data_class=B, data=data)

assert result == B(a=A(x='test', y=1))
```

### Optional fields

Whenever your data class has a `Optional` field and you will not provide
input data for this field, it will take the `None` value.

```python
from typing import Optional

@dataclass
class A:
    x: str
    y: Optional[int]


data = {
    'x': 'test',
}

result = from_dict(data_class=A, data=data)

assert result == A(x='test', y=None)
```

### Unions

If your field can accept multiple types, you should use `Union`. Dacite
will try to match data with provided types one by one. If none will
match, it will raise `UnionMatchError` exception.

```python
from typing import Union

@dataclass
class A:
    x: str

@dataclass
class B:
    y: int

@dataclass
class C:
    u: Union[A, B]


data = {
    'u': {
        'y': 1,
    },
}

result = from_dict(data_class=C, data=data)

assert result == C(u=B(y=1))
```

### Collections

Dacite supports fields defined as collections. It works for both - basic
types and data classes.

```python
@dataclass
class A:
    x: str
    y: int


@dataclass
class B:
    a_list: List[A]


data = {
    'a_list': [
        {
            'x': 'test1',
            'y': 1,
        },
        {
            'x': 'test2',
            'y': 2,
        }
    ],
}

result = from_dict(data_class=B, data=data)

assert result == B(a_list=[A(x='test1', y=1), A(x='test2', y=2)])
```

### Generics

Dacite supports generics: (multi-)generic dataclasses, but also dataclasses that inherit from a generic dataclass, or dataclasses that have a generic dataclass field.

```python
T = TypeVar('T')
U = TypeVar('U')

@dataclass
class X:
    a: str


@dataclass
class A(Generic[T, U]):
    x: T
    y: list[U]

data = {
    'x': {
        'a': 'foo',
    },
    'y': [1, 2, 3]
}

result = from_dict(data_class=A[X, int], data=data)

assert result == A(x=X(a='foo'), y=[1,2,3])


@dataclass
class B(A[X, int]):
    z: str

data = {
    'x': {
        'a': 'foo',
    },
    'y': [1, 2, 3],
    'z': 'bar'
}

result = from_dict(data_class=B, data=data)

assert result == B(x=X(a='foo'), y=[1,2,3], z='bar')


@dataclass
class C:
    z: A[X, int]

data = {
    'z': {
        'x': {
            'a': 'foo',
        },
        'y': [1, 2, 3],
    }
}

result = from_dict(data_class=C, data=data)

assert result == C(z=A(x=X(a='foo'), y=[1,2,3]))
```

### Type hooks

You can use `Config.type_hooks` argument if you want to transform the input 
data of a data class field with given type into the new value. You have to 
pass a following mapping: `{Type: callable}`, where `callable` is a 
`Callable[[Any], Any]`.

```python
@dataclass
class A:
    x: str


data = {
    'x': 'TEST',
}

result = from_dict(data_class=A, data=data, config=Config(type_hooks={str: str.lower}))

assert result == A(x='test')
```

If a data class field type is a `Optional[T]` you can pass both - 
`Optional[T]` or just `T` - as a key in `type_hooks`. The same with generic 
collections, e.g. when a field has type `List[T]` you can use `List[T]` to 
transform whole collection or `T` to transform each item. 

### Casting

It's a very common case that you want to create an instance of a field type 
from the input data with just calling your type with the input value. Of 
course you can use `type_hooks={T: T}` to achieve this goal but `cast=[T]` is 
an easier and more expressive way. It also works with base classes - if `T` 
is a base class of type `S`, all fields of type `S` will be also "casted".

```python
from enum import Enum

class E(Enum):
    X = 'x'
    Y = 'y'
    Z = 'z'

@dataclass
class A:
    e: E


data = {
    'e': 'x',
}

result = from_dict(data_class=A, data=data, config=Config(cast=[E]))

# or

result = from_dict(data_class=A, data=data, config=Config(cast=[Enum]))

assert result == A(e=E.X)
```

### Forward References

Definition of forward references can be passed as a `{'name': Type}` mapping to 
`Config.forward_references`. This dict is passed to `typing.get_type_hints()` as the 
`globalns` param when evaluating each field's type.

```python
@dataclass
class X:
    y: "Y"

@dataclass
class Y:
    s: str

data = from_dict(X, {"y": {"s": "text"}}, Config(forward_references={"Y": Y}))
assert data == X(Y("text"))
```

### Type checking

If you want to trade-off type checking for speed, you can disabled type checking by setting `check_types` to `False`.

```python
@dataclass
class A:
    x: str

# won't throw an error even though the type is wrong
from_dict(A, {'x': 4}, config=Config(check_types=False)) 
```

### Strict mode

By default `from_dict` ignores additional keys (not matching data class field) 
in the input data. If you want change this behaviour set `Config.strict` to 
`True`. In case of unexpected key `from_dict` will raise `UnexpectedDataError` 
exception.

### Strict unions match

`Union` allows to define multiple possible types for a given field. By default 
`dacite` is trying to find the first matching type for a provided data and it 
returns instance of this type. It means that it's possible that there are other 
matching types further on the `Union` types list. With `strict_unions_match` 
only a single match is allowed, otherwise `dacite` raises `StrictUnionMatchError`.

## Convert key

You can pass a callable to the `convert_key` configuration parameter to convert camelCase to snake_case.

```python
def to_camel_case(key: str) -> str:
    first_part, *remaining_parts = key.split('_')
    return first_part + ''.join(part.title() for part in remaining_parts)

@dataclass
class Person:
    first_name: str
    last_name: str

data = {
    'firstName': 'John',
    'lastName': 'Doe'
}

result = from_dict(Person, data, Config(convert_key=to_camel_case))

assert result == Person(first_name='John', last_name='Doe') 
```

## Exceptions

Whenever something goes wrong, `from_dict` will raise adequate
exception. There are a few of them:

- `WrongTypeError` - raised when a type of a input value does not match
with a type of a data class field
- `MissingValueError` - raised when you don't provide a value for a
required field
- `UnionMatchError` - raised when provided data does not match any type
of `Union`
- `ForwardReferenceError` - raised when undefined forward reference encountered in
dataclass
- `UnexpectedDataError` - raised when `strict` mode is enabled and the input 
data has not matching keys
- `StrictUnionMatchError` - raised when `strict_unions_match` mode is enabled 
and the input data has ambiguous `Union` match

## Development

First of all - if you want to submit your pull request, thank you very much! 
I really appreciate your support.

Please remember that every new feature, bug fix or improvement should be tested. 
100% code coverage is a must-have. 

We are using a few static code analysis tools to increase the code quality 
(`black`, `mypy`, `pylint`). Please make sure that you are not generating any 
errors/warnings before you submit your PR. You can find current configuration
in `.github/*` directory.

Last but not least, if you want to introduce new feature, please discuss it 
first within an issue.

### How to start

Clone `dacite` repository:

```bash
git clone git@github.com:konradhalas/dacite.git
```

Create and activate virtualenv in the way you like:

```bash
python3 -m venv dacite-env
source dacite-env/bin/activate
```

Install all `dacite` dependencies:

```bash
pip install -e .[dev]
```

And, optionally but recommended, install pre-commit hook for black:

```bash
pre-commit install
```

To run tests you just have to fire:

```bash
pytest
```

### Performance testing

`dacite` is a small library, but its use is potentially very extensive. Thus, it is crucial
to ensure good performance of the library.

We achieve that with the help of `pytest-benchmark` library, and a suite of dedicated performance tests
which can be found in the `tests/performance` directory. The CI process runs these tests automatically,
but they can also be helpful locally, while developing the library.

Whenever you run `pytest` command, a new benchmark report is saved to `/.benchmarks` directory.
You can easily compare these reports by running: `pytest-benchmark compare`, which will load all the runs
and display them in a table, where you can compare the performance of each run.

You can even specify which particular runs you want to compare, e.g. `pytest-benchmark compare 0001 0003 0005`.
 
## Use case

There are many cases when we receive "raw" data (Python dicts) as a input to 
our system. HTTP request payload is a very common use case. In most web 
frameworks we receive request data as a simple dictionary. Instead of 
passing this dict down to your "business" code, it's a good idea to create 
something more "robust".

Following example is a simple `flask` app - it has single `/products` endpoint.
You can use this endpoint to "create" product in your system. Our core 
`create_product` function expects data class as a parameter. Thanks to `dacite` 
we can easily build such data class from `POST` request payload.


```python
from dataclasses import dataclass
from typing import List

from flask import Flask, request, Response

import dacite

app = Flask(__name__)


@dataclass
class ProductVariantData:
    code: str
    description: str = ''
    stock: int = 0


@dataclass
class ProductData:
    name: str
    price: float
    variants: List[ProductVariantData]


def create_product(product_data: ProductData) -> None:
    pass  # your business logic here


@app.route("/products", methods=['POST'])
def products():
    product_data = dacite.from_dict(
        data_class=ProductData,
        data=request.get_json(),
    )
    create_product(product_data=product_data)
    return Response(status=201)

```

What if we want to validate our data (e.g. check if `code` has 6 characters)? 
Such features are out of scope of `dacite` but we can easily combine it with 
one of data validation library. Let's try with 
[marshmallow](https://marshmallow.readthedocs.io).

First of all we have to define our data validation schemas:

```python
from marshmallow import Schema, fields, ValidationError


def validate_code(code):
    if len(code) != 6:
        raise ValidationError('Code must have 6 characters.')


class ProductVariantDataSchema(Schema):
    code = fields.Str(required=True, validate=validate_code)
    description = fields.Str(required=False)
    stock = fields.Int(required=False)


class ProductDataSchema(Schema):
    name = fields.Str(required=True)
    price = fields.Decimal(required=True)
    variants = fields.Nested(ProductVariantDataSchema(many=True))
```

And use them within our endpoint:

```python
@app.route("/products", methods=['POST'])
def products():
    schema = ProductDataSchema()
    result, errors = schema.load(request.get_json())
    if errors:
        return Response(
            response=json.dumps(errors), 
            status=400, 
            mimetype='application/json',
        )
    product_data = dacite.from_dict(
        data_class=ProductData,
        data=result,
    )
    create_product(product_data=product_data)
    return Response(status=201)
```

Still `dacite` helps us to create data class from "raw" dict with validated data.

### Cache

`dacite` uses some LRU caching to improve its performance where possible. To use the caching utility:

```python
from dacite import set_cache_size, get_cache_size, clear_cache

get_cache_size()  # outputs the current LRU max_size, default is 2048
set_cache_size(4096)  # set LRU max_size to 4096
set_cache_size(None)  # set LRU max_size to None
clear_cache()  # Clear the cache
```

The caching is completely transparent from the interface perspective.

## Changelog

Follow `dacite` updates in [CHANGELOG][changelog].

## Authors

Created by [Konrad Hałas][halas-homepage].

[pep-557]: https://www.python.org/dev/peps/pep-0557/
[halas-homepage]: https://konradhalas.pl
[changelog]: https://github.com/konradhalas/dacite/blob/master/CHANGELOG.md
