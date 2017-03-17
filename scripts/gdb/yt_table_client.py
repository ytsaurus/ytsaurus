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

def strip_typedefs_and_qualifiers(ty):
    return ty.unqualified().strip_typedefs()

def get_unversioned_value_type():
    return lookup_type("NYT::NTableClient::TUnversionedValue")


def get_unversioned_row_type():
    return lookup_type("NYT::NTableClient::TUnversionedRow")


def get_unversioned_row_header_type_ptr():
    header_type = lookup_type("NYT::NTableClient::TUnversionedRowHeader")
    return None if header_type is None else header_type.pointer()


def get_unversioned_owning_row_type():
    return lookup_type("NYT::NTableClient::TUnversionedOwningRow")


def get_versioned_value_type():
    return lookup_type("NYT::NTableClient::TVersionedValue")


def get_versioned_row_type():
    return lookup_type("NYT::NTableClient::TVersionedRow")


def get_versioned_row_header_type_ptr():
    header_type = lookup_type("NYT::NTableClient::TVersionedRowHeader")
    return None if header_type is None else header_type.pointer()


def get_versioned_owning_row_type():
    return lookup_type("NYT::NTableClient::TVersionedOwningRow")


def get_timestamp_type():
    return lookup_type("NYT::NTransactionClient::TTimestamp")


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

def print_unversioned_value(val):
    print format_unversioned_value(val)

def print_unversioned_row_with_header(val, val_name):
    assert strip_typedefs_and_qualifiers(val.type) == get_unversioned_row_header_type_ptr()

    row_header = val  # for clarity.

    value_count = int(row_header["Count"])
    value_array = (row_header + 1).cast(get_unversioned_value_type().pointer())

    print "{} &{} {{".format(val_name, row_header.dereference().address)
    for i in range(value_count):
        print "  {}".format(format_unversioned_value(value_array[i]))
    print "}"


def print_unversioned_row(val):
    assert strip_typedefs_and_qualifiers(val.type) == get_unversioned_row_type()
    row_header = val["Header_"]
    row_header = row_header.cast(get_unversioned_row_header_type_ptr())
    return print_unversioned_row_with_header(row_header, "TUnversionedRow")


def print_unversioned_owning_row(val):
    assert strip_typedefs_and_qualifiers(val.type) == get_unversioned_owning_row_type()
    row_header = val["RowData_"]["Data_"]
    row_header = row_header.cast(get_unversioned_row_header_type_ptr())
    return print_unversioned_row_with_header(row_header, "TUnversionedOwningRow")


def format_versioned_value(val):
    assert strip_typedefs_and_qualifiers(val.type) == get_versioned_value_type()
    value_timestamp, value_id, value_type, value_data = unpack_as_versioned_value(val)
    if value_data != "":
        value_data = "{" + value_data + "}"
    return "#%-2d  %-7s  @%s %s" % (value_id, value_type, value_timestamp, value_data)

def print_versioned_value(val):
    print format_versioned_value(val)

def print_versioned_row_with_header(val, val_name):
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

    print "{} &{} {{".format(val_name, row_header.dereference().address)
    s = ", ".join(str(write_timestamp_array[i]) for i in range(write_timestamp_count))
    print "  WriteTimestamps [{}]".format(s)
    s = ", ".join(str(delete_timestamp_array[i]) for i in range(delete_timestamp_count))
    print "  DeleteTimestamps [{}]".format(s)
    print "  Keys ["
    for i in range(key_count):
        print "    {}".format(format_unversioned_value(key_array[i]))
    print "  ]"
    print "  Values ["
    for i in range(value_count):
        print "    {}".format(format_versioned_value(value_array[i]))
    print "  ]"
    print "}"


def print_versioned_row(val):
    assert strip_typedefs_and_qualifiers(val.type) == get_versioned_row_type()
    row_header = val["Header_"]
    return print_versioned_row_with_header(row_header, "TVersionedRow")


def print_versioned_owning_row(val):
    assert strip_typedefs_and_qualifiers(val.type) == get_versioned_owning_row_type()
    row_header = val["RowData_"]["Data_"]
    row_header = row_header.cast(get_versioned_row_header_type_ptr())
    return print_versioned_row_with_header(row_header, "TVersionedOwningRow")

class YtPrintRow(gdb.Command):
    def __init__(self):
        gdb.Command.__init__(self, "yp", gdb.COMMAND_STACK, gdb.COMPLETE_SYMBOL)

    def invoke(self, arg, _from_tty):
        val = gdb.parse_and_eval(arg)
        ty, val = strip_refs_typedefs_and_qualifiers(val.type, val)
        if ty == get_unversioned_row_type():
            print_unversioned_row(val)
        elif ty == get_unversioned_owning_row_type():
            print_unversioned_owning_row(val)
        elif ty == get_unversioned_row_header_type_ptr():
            print_unversioned_row_with_header(val, "TUnversionedRow")
        elif ty == get_versioned_row_type():
            print_versioned_row(val)
        elif ty == get_versioned_owning_row_type():
            print_versioned_owning_row(val)
        elif ty == get_versioned_row_header_type_ptr():
            print_versioned_row_with_header(val, "TVersionedRow")
        elif ty == get_unversioned_value_type():
            print_unversioned_value(val)
        elif ty == get_versioned_value_type():
            print_versioned_value(val)
        else:
            print "Value has type " + str(ty) + " that has no special way of printing"

YtPrintRow()
