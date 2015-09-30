import gdb
# import json


def get_unversioned_value_type():
    return gdb.lookup_type("NYT::NTableClient::TUnversionedValue")


def get_unversioned_row_type():
    return gdb.lookup_type("NYT::NTableClient::TUnversionedRow")


def get_unversioned_row_header_type():
    return gdb.lookup_type("NYT::NTableClient::TUnversionedRowHeader")


def get_unversioned_owning_row_type():
    return gdb.lookup_type("NYT::NTableClient::TUnversionedOwningRow")


def get_versioned_value_type():
    return gdb.lookup_type("NYT::NTableClient::TVersionedValue")


def get_versioned_row_type():
    return gdb.lookup_type("NYT::NTableClient::TVersionedRow")


def get_versioned_row_header_type():
    return gdb.lookup_type("NYT::NTableClient::TVersionedRowHeader")


def get_versioned_owning_row_type():
    return gdb.lookup_type("NYT::NTableClient::TVersionedOwningRow")


def get_timestamp_type():
    return gdb.lookup_type("NYT::NTransactionClient::TTimestamp")


def unpack_as_unversioned_value(val):
    value_id = int(val["Id"])
    value_type = val["Type"].cast(gdb.lookup_type("uint16_t"))
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
    assert val.type == get_unversioned_value_type()
    value_id, value_type, value_data = unpack_as_unversioned_value(val)
    if value_data != "":
        value_data = "{" + value_data + "}"
    return "#%-2d  %-7s  %s" % (value_id, value_type, value_data)


def print_unversioned_row_with_header(val, val_name):
    assert val.type == get_unversioned_row_header_type().pointer()

    row_header = val  # for clarity.

    value_count = int(row_header["Count"])
    value_array = (row_header + 1).cast(get_unversioned_value_type().pointer())

    print "{} &{} {{".format(val_name, row_header.dereference().address)
    for i in range(value_count):
        print "  {}".format(format_unversioned_value(value_array[i]))
    print "}"


def print_unversioned_row(val):
    assert val.type.strip_typedefs() == get_unversioned_row_type()
    row_header = val["Header_"]
    return print_unversioned_row_with_header(row_header, "TUnversionedRow")


def print_unversioned_owning_row(val):
    assert val.type.strip_typedefs() == get_unversioned_owning_row_type()
    row_header = val["RowData_"]["Data_"]
    row_header = row_header.cast(get_unversioned_row_header_type().pointer())
    return print_unversioned_row_with_header(row_header, "TUnversionedOwningRow")


def format_versioned_value(val):
    assert val.type == get_versioned_value_type()
    value_timestamp, value_id, value_type, value_data = unpack_as_versioned_value(val)
    if value_data != "":
        value_data = "{" + value_data + "}"
    return "#%-2d  %-7s  @%s %s" % (value_id, value_type, value_timestamp, value_data)


def print_versioned_row_with_header(val, val_name):
    assert val.type == get_versioned_row_header_type().pointer()

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
    assert val.type.strip_typedefs() == get_versioned_row_type()
    row_header = val["Header_"]
    return print_versioned_row_with_header(row_header, "TVersionedRow")


def print_versioned_owning_row(val):
    assert val.type.strip_typedefs() == get_versioned_owning_row_type()
    row_header = val["RowData_"]["Data_"]
    row_header = row_header.cast(get_versioned_row_header_type().pointer())
    return print_versioned_row_with_header(row_header, "TVersionedOwningRow")


class YtPrintRow(gdb.Command):
    def __init__(self):
        gdb.Command.__init__(self, "yt_print_row", gdb.COMMAND_STACK, gdb.COMPLETE_SYMBOL)

    def invoke(self, arg, _from_tty):
        val = gdb.parse_and_eval(arg)
        ty = val.type.strip_typedefs()
        if ty is None:
            print ":("
        elif ty == get_unversioned_row_type():
            print_unversioned_row(val)
        elif ty == get_unversioned_owning_row_type():
            print_unversioned_owning_row(val)
        elif ty == get_versioned_row_type():
            print_versioned_row(val)
        elif ty == get_versioned_owning_row_type():
            print_versioned_owning_row(val)

YtPrintRow()
