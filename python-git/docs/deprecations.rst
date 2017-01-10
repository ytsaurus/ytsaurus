============
Deprecations
============

0.7.x -> 0.8.x
==============

1. `compression_codec`, `replication_factor` parameters will be removed from `run_*` and
   `write_table` functions. Now you can specify arbitrary set of attributes in `create_table_attributes`
   option in config and they will be used for table creation.
2. `file_paths` parameter will be removed from `run_*` functions. Use `yt_files` parameter instead.
3. `write_file_as_one_chunk` will be removed from config. Since 0.17.4 version this option is useless and
   complicates file uploading.
4. Some shortcuts will be removed from config.py file. All configuration can now be done through one
   universal environment variable YT_CONFIG_PATCHES.
5. Default value of option `process_table_index` in `YsonFormat` will be set to None. Now `rows`
   in operation will be rows iterator and rows will not have special `@table_index` field.
   To work with table indices, row indices and other control attributes special decorator `with_context`
   should be used (see `wiki <https://wiki.yandex-team.ru/yt/userdoc/pythonwrapper/#python-func-decorators>`_).
6. `upload_file`, `download_file` will be removed. Use `write_file`, `read_file` instead.
7. Old way to set config options (`yt.config.OPTION_NAME = ...`) usage will be forbidden.
8. Public field `name` of `TablePath` will be removed. `to_yson_type()` method should be used instead. Also `to_name`
   and `to_table` public methods will be removed.
9. `PingableTransaction` will be removed.
10. `api/v2` usage will be forbidden.
11. `timeout` option in `Operation.wait` method will have no effect. Use `time_limit` in operation spec instead.
