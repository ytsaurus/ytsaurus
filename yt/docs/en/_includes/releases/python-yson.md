## Python YSON bindings


Availabe as a package in [PyPI](https://pypi.org/project/ytsaurus-yson/).




**Releases:**

{% cut "**0.4.10**" %}

**Release date:** 2025-03-05


### Features
   *  Support `dump-parquet` in parallel mode
   *  Support `read_table_structured` in parallel mode
   *  Support retries while upload parquet/orc
   *  Support `dump-orc` in parallel mode
   *  Add `min_batch_row_count` option to dump parquet
  
### Fixes
 * Fix `Unexpected end of stream` error in upload parquet


{% endcut %}


{% cut "**0.4.9**" %}

**Release date:** 2024-08-07


Features:
  - Support ORC format
  - Access thread local variables via noinline functions
  - Support Python 3.13 (avoid using deprecated PyImport_ImportModuleNoBlock)

{% endcut %}


{% cut "**0.4.8**" %}

**Release date:** 2024-04-24


  * Add table creation in upload parquet
  * Reduce bindings .so size


{% endcut %}


{% cut "**0.4.7**" %}

**Release date:** 2024-03-09


- Add implementation of `upload_parquet`
- Fix invalid memory access in YsonStringProxy

{% endcut %}

