try:
    from .yson_lib import (load, loads, dump, dumps, loads_proto, dumps_proto, dump_parquet, upload_parquet, # noqa
                           load_skiff, load_skiff_structured, dump_skiff, dump_skiff_structured,
                           SkiffRecord, SkiffSchema, SkiffTableSwitch, SkiffOtherColumns)
except ImportError:
    from yson_lib import (load, loads, dump, dumps, loads_proto, dumps_proto, dump_parquet, upload_parquet, # noqa
                          load_skiff, load_skiff_structured, dump_skiff, dump_skiff_structured,
                          SkiffRecord, SkiffSchema, SkiffTableSwitch, SkiffOtherColumns)

try:
    from .yson_lib import parse_ypath  # noqa
except ImportError:
    try:
        from yson_lib import parse_ypath  # noqa
    except ImportError:
        pass

try:
    from .version import VERSION, COMMIT  # noqa
    __version__ = VERSION
except ImportError:
    __version__ = VERSION = COMMIT = "unknown"
