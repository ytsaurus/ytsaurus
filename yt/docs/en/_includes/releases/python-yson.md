## {{product-name}} Python YSON

Current version: {{python-yson-version}}

{% cut "**0.4.9**"%}

**Features:**

- Support ORC format.
- Access thread local variables via noinline functions.
- Support Python 3.13 (avoid using deprecated `PyImport_ImportModuleNoBlock`).

{% endcut %}

{% cut "**0.4.8**"%}

- Add table creation in upload parquet.
- Reduce bindings .so size.

{% endcut %}

{% cut "**0.4.7**"%}

- Add implementation of `upload_parquet`.
- Fix invalid memory access in `YsonStringProxy`.

{% endcut %}