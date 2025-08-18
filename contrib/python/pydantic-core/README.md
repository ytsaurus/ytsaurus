# pydantic-core --- обновление сборки в arc

Предполагается, что у вас корень аркадии находится в `~/arcadia`, кроме того установлен [rustup](https://rustup.rs/) и 
необходимый rust-toolchain.

Одна из сложностей обновления в том, что нужно собрать бинарь lib_pydantic_core.a, в котором нужно переименовать все символы,
чтобы не было дублей. Для этого используется инструмент [ym2](https://docs.yandex-team.ru/contrib-automation/templates),
исходники которого лежат [здесь](https://a.yandex-team.ru/arcadia/devtools/yamaker/libym2). Впрочем, это --- справочная
информация.

## Обновляем контриб:

(укажите нужную версию [отсюда](https://github.com/pydantic/pydantic-core/releases), помните про двухнедельный карантин)

```bash
cd ~/arcadia/contrib/python/pydantic-core
ya tool yamaker pypi --contrib-path ~/arcadia/contrib/python/pydantic-core -v 2.16.3 
```

Теперь ровно ту же версию (например, 2.16.3) нужно прописать в файле `/.yandex_meta/build.ym` в строчке

```
{% block current_version %}2.16.3{% endblock %}
```

Теперь нужно запустить кросс-сборку бинарей:

```bash
ya tool yamaker ym2 reimport -t ~/arcadia/contrib/python/pydantic-core/.yandex_meta/build.ym
```

Если повезёт, то всё отработает правильно :)


## Ну, теперь осталось залить в аркадию

Свою версию только используйте :) 
Ну или тикет добавьте.

```bash
arc checkout -b bump-pydantic-core-to-v-2.16.3
arc add ~/arcadia/contrib/python/pydantic-core
arc add ~/arcadia/contrib/python/pydantic-core/a -f
arc commit -m "bump-pydantic-core-to-v-2.16.3"
```

Теперь хорошо бы провести проверку на реимпорт:

```bash
ya tool yamaker pypi --contrib-path ~/arcadia/contrib/python/pydantic-core -v 2.16.3 
arc status
```

Изменений быть не должно. Если их нет, то поехали в арк:

```bash
arc status
arc pr create -m "bump-pydantic-core-to-v-2.16.3"
```

## Если не повезло, то билд в аркадии быстро упадёт с ошибкой вида 
`ld.lld: error: undefined symbol: PyInit_13pydantic_core14_pydantic_core`
Ну что же, вы неудачник :)
Сначала посмотрите, что экспортируется во всех ваших бинарях:

```bash
llvm-nm --extern-only --defined-only -A a/aarch64-apple-darwin/release/lib_pydantic_core.a 2>/dev/null | sed -e 's|.* ||' | grep PyInit
llvm-nm --extern-only --defined-only -A a/x86_64-apple-darwin/release/lib_pydantic_core.a 2>/dev/null | sed -e 's|.* ||' | grep PyInit
llvm-nm --extern-only --defined-only -A a/aarch64-unknown-linux-gnu/release/lib_pydantic_core.a 2>/dev/null | sed -e 's|.* ||' | grep PyInit
llvm-nm --extern-only --defined-only -A a/x86_64-unknown-linux-musl/release/lib_pydantic_core.a 2>/dev/null | sed -e 's|.* ||' | grep PyInit
llvm-nm --extern-only --defined-only -A a/x86_64-unknown-linux-gnu/release/lib_pydantic_core.a 2>/dev/null | sed -e 's|.* ||' | grep PyInit
```

Если где-то нет того символа, на нехватку которого есть жалоба, то ищите `redefine-sym` в `/.yandex_meta/build.ym`


## Обновление этой инструкции

Если хочется обновить эту инструкцию, это нужно делать так.

1. Отложить содержимое этой инструкции в другое место
2. Получить "оригинальный" README.md назад: вырезать эту инструкцию
3. Закоммитить `README.md`
5. Внести необходимые изменения в `README.md`
5. Собрать diff:

```bash
arc diff README.md > patches/05-readme-for-updates.patch
```

# pydantic-core

[![CI](https://github.com/pydantic/pydantic-core/workflows/ci/badge.svg?event=push)](https://github.com/pydantic/pydantic-core/actions?query=event%3Apush+branch%3Amain+workflow%3Aci)
[![Coverage](https://codecov.io/gh/pydantic/pydantic-core/branch/main/graph/badge.svg)](https://codecov.io/gh/pydantic/pydantic-core)
[![pypi](https://img.shields.io/pypi/v/pydantic-core.svg)](https://pypi.python.org/pypi/pydantic-core)
[![versions](https://img.shields.io/pypi/pyversions/pydantic-core.svg)](https://github.com/pydantic/pydantic-core)
[![license](https://img.shields.io/github/license/pydantic/pydantic-core.svg)](https://github.com/pydantic/pydantic-core/blob/main/LICENSE)

This package provides the core functionality for [pydantic](https://docs.pydantic.dev) validation and serialization.

Pydantic-core is currently around 17x faster than pydantic V1.
See [`tests/benchmarks/`](./tests/benchmarks/) for details.

## Example of direct usage

_NOTE: You should not need to use pydantic-core directly; instead, use pydantic, which in turn uses pydantic-core._

```py
from pydantic_core import SchemaValidator, ValidationError


v = SchemaValidator(
    {
        'type': 'typed-dict',
        'fields': {
            'name': {
                'type': 'typed-dict-field',
                'schema': {
                    'type': 'str',
                },
            },
            'age': {
                'type': 'typed-dict-field',
                'schema': {
                    'type': 'int',
                    'ge': 18,
                },
            },
            'is_developer': {
                'type': 'typed-dict-field',
                'schema': {
                    'type': 'default',
                    'schema': {'type': 'bool'},
                    'default': True,
                },
            },
        },
    }
)

r1 = v.validate_python({'name': 'Samuel', 'age': 35})
assert r1 == {'name': 'Samuel', 'age': 35, 'is_developer': True}

# pydantic-core can also validate JSON directly
r2 = v.validate_json('{"name": "Samuel", "age": 35}')
assert r1 == r2

try:
    v.validate_python({'name': 'Samuel', 'age': 11})
except ValidationError as e:
    print(e)
    """
    1 validation error for model
    age
      Input should be greater than or equal to 18
      [type=greater_than_equal, context={ge: 18}, input_value=11, input_type=int]
    """
```

## Getting Started

You'll need rust stable [installed](https://rustup.rs/), or rust nightly if you want to generate accurate coverage.

With rust and python 3.9+ installed, compiling pydantic-core should be possible with roughly the following:

```bash
# clone this repo or your fork
git clone git@github.com:pydantic/pydantic-core.git
cd pydantic-core
# create a new virtual env
python3 -m venv env
source env/bin/activate
# install dependencies and install pydantic-core
make install
```

That should be it, the example shown above should now run.

You might find it useful to look at [`python/pydantic_core/_pydantic_core.pyi`](./python/pydantic_core/_pydantic_core.pyi) and
[`python/pydantic_core/core_schema.py`](./python/pydantic_core/core_schema.py) for more information on the python API,
beyond that, [`tests/`](./tests) provide a large number of examples of usage.

If you want to contribute to pydantic-core, you'll want to use some other make commands:
* `make build-dev` to build the package during development
* `make build-prod` to perform an optimised build for benchmarking
* `make test` to run the tests
* `make testcov` to run the tests and generate a coverage report
* `make lint` to run the linter
* `make format` to format python and rust code
* `make` to run `format build-dev lint test`

## Profiling

It's possible to profile the code using the [`flamegraph` utility from `flamegraph-rs`](https://github.com/flamegraph-rs/flamegraph). (Tested on Linux.) You can install this with `cargo install flamegraph`.

Run `make build-profiling` to install a release build with debugging symbols included (needed for profiling).

Once that is built, you can profile pytest benchmarks with (e.g.):

```bash
flamegraph -- pytest tests/benchmarks/test_micro_benchmarks.py -k test_list_of_ints_core_py --benchmark-enable
```
The `flamegraph` command will produce an interactive SVG at `flamegraph.svg`.

## Releasing

1. Bump package version locally. Do not just edit `Cargo.toml` on Github, you need both `Cargo.toml` and `Cargo.lock` to be updated.
2. Make a PR for the version bump and merge it.
3. Go to https://github.com/pydantic/pydantic-core/releases and click "Draft a new release"
4. In the "Choose a tag" dropdown enter the new tag `v<the.new.version>` and select "Create new tag on publish" when the option appears.
5. Enter the release title in the form "v<the.new.version> <YYYY-MM-DD>"
6. Click Generate release notes button
7. Click Publish release
8. Go to https://github.com/pydantic/pydantic-core/actions and ensure that all build for release are done successfully.
9. Go to https://pypi.org/project/pydantic-core/ and ensure that the latest release is published.
10. Done 🎉
