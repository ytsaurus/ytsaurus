from __future__ import print_function

import gdb
# import json

def lookup_type(name):
    try:
        return gdb.lookup_type(name)
    except gdb.error:
        return None


def strip_refs_typedefs_and_qualifiers(ty, val):
    while True:
        ty = ty.unqualified().strip_typedefs()
        if ty.code == gdb.TYPE_CODE_REF:
            ty = ty.target()
            val = val.referenced_value()
        else:
            break
    return ty, val

SIGNATURES = {
    "UnversionedValue": "NYT::NTableClient::TUnversionedValue",
    "UnversionedRow": "NYT::NTableClient::TUnversionedRow",
    "UnversionedRowHeader": "NYT::NTableClient::TUnversionedRowHeader",
    "UnversionedOwningRow": "NYT::NTableClient::TUnversionedOwningRow",
    "VersionedValue": "NYT::NTableClient::TVersionedValue",
    "VersionedRow": "NYT::NTableClient::TVersionedRow",
    "VersionedRowHeader": "NYT::NTableClient::TVersionedRowHeader",
    "VersionedOwningRow": "NYT::NTableClient::TVersionedOwningRow",
    "Timestamp": "NYT::NTransactionClient::TTimestamp",
}

def strip_typedefs_and_qualifiers(ty):
    return ty.unqualified().strip_typedefs()

def get_unversioned_value_type():
    global SIGNATURES
    return lookup_type(SIGNATURES["UnversionedValue"])


def get_unversioned_row_type():
    global SIGNATURES
    return lookup_type(SIGNATURES["UnversionedRow"])


def get_unversioned_row_header_type_ptr():
    global SIGNATURES
    header_type = lookup_type(SIGNATURES["UnversionedRowHeader"])
    return None if header_type is None else header_type.pointer()


def get_unversioned_owning_row_type():
    global SIGNATURES
    return lookup_type(SIGNATURES["UnversionedOwningRow"])


def get_versioned_value_type():
    global SIGNATURES
    return lookup_type(SIGNATURES["VersionedValue"])


def get_versioned_row_type():
    global SIGNATURES
    return lookup_type(SIGNATURES["VersionedRow"])


def get_versioned_row_header_type_ptr():
    global SIGNATURES
    header_type = lookup_type(SIGNATURES["VersionedRowHeader"])
    return None if header_type is None else header_type.pointer()


def get_versioned_owning_row_type():
    global SIGNATURES
    return lookup_type(SIGNATURES["VersionedOwningRow"])


def get_timestamp_type():
    global SIGNATURES
    return lookup_type(SIGNATURES["Timestamp"])


def unpack_as_unversioned_value(val):
    value_id = int(val["Id"])
    value_type = val["Type"].cast(gdb.lookup_type("uint8_t"))
    if value_type == 0x02:
        value_type = "null"
        value_data = ""
    elif value_type == 0x00:
        value_type = "min"
        value_data = ""
    elif value_type == 0xef:
        value_type = "max"
        value_data = ""
    elif value_type == 0x03:
        value_type = "i64"
        value_data = str(val["Data"]["Int64"])
    elif value_type == 0x04:
        value_type = "ui64"
        value_data = str(val["Data"]["Uint64"])
    elif value_type == 0x05:
        value_type = "double"
        value_data = str(val["Data"]["Double"])
    elif value_type == 0x06:
        value_type = "boolean"
        value_data = str(val["Data"]["Boolean"])
    elif value_type == 0x10:
        value_type = "string"
        value_data = val["Data"]["String"].string(length=val["Length"])
        value_data = value_data.encode("utf-8")
        # value_data = json.dumps(value_data)  # escaping
    elif value_type == 0x11:
        value_type = "any"
        value_data = val["Data"]["String"].string(length=val["Length"])
        value_data = value_data.encode("utf-8")
        # value_data = json.dumps(value_data)  # escaping
    return value_id, value_type, value_data


def unpack_as_versioned_value(val):
    value_timestamp = long(val["Timestamp"])
    value_id, value_type, value_data = unpack_as_unversioned_value(val)
    return value_timestamp, value_id, value_type, value_data


def format_unversioned_value(val):
    assert strip_typedefs_and_qualifiers(val.type) == get_unversioned_value_type()
    value_id, value_type, value_data = unpack_as_unversioned_value(val)
    if value_data != "":
        value_data = "{" + value_data + "}"
    return "#%-2d  %-7s  %s" % (value_id, value_type, value_data)

def format_lines(lines, compact):
    if not compact:
        return "\n".join(lines)
    else:
        result_with_spaces = " ".join(lines)
        # AA section problem :) let's remove all double-spaces
        result = ""
        for c in result_with_spaces:
            if c != " " or len(result) == 0 or result[-1] != " ":
                result += c
        return result

def format_unversioned_row_with_header(val, val_name, compact):
    assert strip_typedefs_and_qualifiers(val.type) == get_unversioned_row_header_type_ptr()

    row_header = val  # for clarity.

    value_count = int(row_header["Count"])
    value_array = (row_header + 1).cast(get_unversioned_value_type().pointer())

    lines = []
    lines.append("{} &{} {{".format(val_name, row_header.dereference().address))
    for i in range(value_count):
        lines.append("  {}".format(format_unversioned_value(value_array[i])))
    lines.append("}")
    return format_lines(lines, compact)

def format_unversioned_row(val, compact):
    assert strip_typedefs_and_qualifiers(val.type) == get_unversioned_row_type()
    row_header = val["Header_"]
    row_header = row_header.cast(get_unversioned_row_header_type_ptr())
    return format_unversioned_row_with_header(row_header, "TUnversionedRow", compact)


def format_unversioned_owning_row(val, compact):
    assert strip_typedefs_and_qualifiers(val.type) == get_unversioned_owning_row_type()
    row_header = val["RowData_"]["Data_"]
    row_header = row_header.cast(get_unversioned_row_header_type_ptr())
    return format_unversioned_row_with_header(row_header, "TUnversionedOwningRow", compact)


def format_versioned_value(val):
    assert strip_typedefs_and_qualifiers(val.type) == get_versioned_value_type()
    value_timestamp, value_id, value_type, value_data = unpack_as_versioned_value(val)
    if value_data != "":
        value_data = "{" + value_data + "}"
    return "#%-2d  %-7s  @%s %s" % (value_id, value_type, value_timestamp, value_data)

def format_versioned_row_with_header(val, val_name, compact):
    assert strip_typedefs_and_qualifiers(val.type) == get_versioned_row_header_type_ptr()

    row_header = val  # for clarity.

    value_count = row_header["ValueCount"]
    key_count = row_header["KeyCount"]
    write_timestamp_count = row_header["WriteTimestampCount"]
    delete_timestamp_count = row_header["DeleteTimestampCount"]

    write_timestamp_array = (row_header + 1).cast(get_timestamp_type().pointer())
    delete_timestamp_array = (write_timestamp_array + write_timestamp_count).cast(get_timestamp_type().pointer())
    key_array = (delete_timestamp_array + delete_timestamp_count).cast(get_unversioned_value_type().pointer())
    value_array = (key_array + key_count).cast(get_versioned_value_type().pointer())

    lines = []
    lines.append(("{} &{} {{".format(val_name, row_header.dereference().address)))
    s = ", ".join(str(write_timestamp_array[i]) for i in range(write_timestamp_count))
    lines.append("  WriteTimestamps [{}]".format(s))
    s = ", ".join(str(delete_timestamp_array[i]) for i in range(delete_timestamp_count))
    lines.append("  DeleteTimestamps [{}]".format(s))
    lines.append("  Keys [")
    for i in range(key_count):
        lines.append("    {}".format(format_unversioned_value(key_array[i])))
    lines.append("  ]")
    lines.append("  Values [")
    for i in range(value_count):
        lines.append("    {}".format(format_versioned_value(value_array[i])))
    lines.append("  ]")
    lines.append("}")
    return format_lines(lines, compact)


def format_versioned_row(val, compact):
    assert strip_typedefs_and_qualifiers(val.type) == get_versioned_row_type()
    row_header = val["Header_"]
    return format_versioned_row_with_header(row_header, "TVersionedRow", compact)


def format_versioned_owning_row(val, compact):
    assert strip_typedefs_and_qualifiers(val.type) == get_versioned_owning_row_type()
    row_header = val["RowData_"]["Data_"]
    row_header = row_header.cast(get_versioned_row_header_type_ptr())
    return format_versioned_row_with_header(row_header, "TVersionedOwningRow", compact)

def do_format(val, compact):
    ty, val = strip_refs_typedefs_and_qualifiers(val.type, val)
    if ty == get_unversioned_row_type():
        return format_unversioned_row(val, compact)
    elif ty == get_unversioned_owning_row_type():
        return format_unversioned_owning_row(val, compact)
    elif ty == get_unversioned_row_header_type_ptr():
        return format_unversioned_row_with_header(val, "TUnversionedRow", compact)
    elif ty == get_versioned_row_type():
        return format_versioned_row(val, compact)
    elif ty == get_versioned_owning_row_type():
        return format_versioned_owning_row(val, compact)
    elif ty == get_versioned_row_header_type_ptr():
        return format_versioned_row_with_header(val, "TVersionedRow", compact)
    elif ty == get_unversioned_value_type():
        return format_unversioned_value(val, compact)
    elif ty == get_versioned_value_type():
        return format_versioned_value(val, compact)
    else:
        print("Value has type " + str(ty) + " that has no special way of printing")


class YtPrintRow(gdb.Command):
    def __init__(self):
        gdb.Command.__init__(self, "yp", gdb.COMMAND_STACK, gdb.COMPLETE_SYMBOL)

    def invoke(self, arg, _from_tty):
        val = gdb.parse_and_eval(arg)
        print(do_format(val, False))

YtPrintRow()

class YtPrettyPrinter:
    def __init__(self, value):
        self.value = value

    def to_string(self):
        return do_format(self.value, True)

def ya_printer():
    pp = gdb.printing.RegexpCollectionPrettyPrinter("yt_table_client")
    for name, signature in SIGNATURES.items():
        pp.add_printer(name, r"^{}$".format(signature), YtPrettyPrinter)
    return pp


def ya_register(obj):
    gdb.printing.register_pretty_printer(obj, ya_printer())
