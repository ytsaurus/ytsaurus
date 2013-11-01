
def main():
    # We should use local imports because of replacing __main__ module cause cleaning globals
    import sys
    import itertools
    import imp
    import zipfile

    # Variable names start with "__" to avoid accidental intersection with scope of user function
    __operation_dump = sys.argv[1]
    __modules_archive = sys.argv[2]
    __main_filename = sys.argv[3]
    __main_module_name = sys.argv[4]
    __config_dump_filename = sys.argv[5]

    # Unfortunately we cannot use fixes version of ZipFile
    __zip = zipfile.ZipFile(__modules_archive)
    __zip.extractall("modules")
    __zip.close()

    sys.path = ["./modules"] + sys.path

    sys.modules['__main__'] = imp.load_module(__main_module_name, open(__main_filename, 'rU'), __main_filename, ('', 'rU', imp.PY_SOURCE))

    for name in dir(sys.modules['__main__']):
        globals()[name] = sys.modules['__main__'].__dict__[name]

    from yt.wrapper.pickling import load
    __operation, __attributes, __operation_type, __input_format, __output_format, __keys = load(open(__operation_dump))

    import yt.wrapper.format_config as format_config
    config_dict = load(open(__config_dump_filename))
    for key, value in config_dict.iteritems():
        format_config.__dict__[key] = value

    import yt.yson as yson
    import yt.wrapper as yt
    from yt.wrapper.record import extract_key

    def process_input_table_index(records):
        table_index = None
        for rec in records:
            if "table_index" in rec.attributes:
                table_index = rec.attributes["table_index"]
            if not isinstance(rec, yson.YsonEntity):
                if table_index is not None:
                    rec.attributes["input_table_index"] = table_index
                yield rec

    def process_output_table_index(records):
        table_index = None
        for rec in records:
            new_table_index = rec.get("output_table_index", 0)
            if new_table_index != table_index:
                yield yson.to_yson_type(None, attributes={"table_index": new_table_index})
            table_index = new_table_index
            rec.attributes = {}
            yield rec

    if __attributes.get("is_raw", False):
        __result = itertools.chain(*itertools.imap(__operation, sys.stdin.xreadlines()))
    else:
        if isinstance(__input_format, yt.YsonFormat):
            __records = process_input_table_index(yson.load(sys.stdin, yson_type="list_fragment"))
        else:
            __records = itertools.imap(lambda line: yt.line_to_record(line, __input_format), sys.stdin.xreadlines())

        if __operation_type == "mapper":
            if __attributes.get("is_aggregator", False):
                __result = __operation(__records)
            else:
                __result = itertools.chain.from_iterable(itertools.imap(__operation, __records))
        else:
            __result = itertools.chain.from_iterable(itertools.starmap(__operation, itertools.groupby(__records, lambda rec: extract_key(rec, __keys, __input_format))))
        if isinstance(__input_format, yt.YsonFormat):
            __result = process_output_table_index(__result)
        __result = itertools.imap(lambda rec: yt.record_to_line(rec, __output_format), __result)

    sys.stdout.writelines(__result)

if __name__ == "__main__":
    main()

