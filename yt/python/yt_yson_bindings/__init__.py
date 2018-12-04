try:
    from .yson_lib import load, loads, dump, dumps, loads_proto, dumps_proto, load_skiff, dump_skiff, SkiffRecord, SkiffSchema, SkiffTableSwitch
except ImportError:
    from yson_lib import load, loads, dump, dumps, loads_proto, dumps_proto, load_skiff, dump_skiff, SkiffRecord, SkiffSchema, SkiffTableSwitch

try:
    from .yson_lib import parse_ypath
except ImportError:
    try:
        from yson_lib import parse_ypath
    except ImportError:
        pass

try:
    from .version import VERSION, COMMIT
    __version__ = VERSION
except:
    __version__ = VERSION = COMMIT = "unknown"
