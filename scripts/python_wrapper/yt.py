from common import YtError
from record import Record, record_to_line, line_to_record
from format import DsvFormat, YamrFormat, YsonFormat
from table import Table
from tree_commands import set, get, list, get_attribute, set_attribute, list_attributes, exists, remove
from table_commands import create_table, write_table, read_table, remove_table, \
                           copy_table, move_table, sort_table, records_count, is_sorted, \
                           create_temp_table, merge_tables, run_map, run_reduce

from operation_commands import get_operation_state, abort_operation, WaitStrategy, AsyncStrategy

if __name__ == "__main__":
    """ Some tests """
    def to_list(iter):
        return [x for x in iter]

    table = "//home/ignat/temp"
    other_table = "//home/ignat/temp2"

    counter = 0
    for line in read_table('//statbox/access-log/"2012-07-04"', format=DsvFormat()):
        print line
        print
        counter += 1
        if counter == 2: break

    set("//home/files", "{}")

    print "LIST", to_list(list("//home"))

    print "GET"
    get(table)

    print "REMOVE"
    remove(table)
    print "EXISTS", exists(table)
    print "CREATE"
    create_table(table)
    print "EXISTS", exists(table)
    print "WRITE"
    write_table(table, map(record_to_line,
        [Record("x", "y", "z"),
         Record("key", "subkey", "value")]))
    print "READ"
    print to_list(read_table(table))
    print "COPY"
    copy_table(table, other_table)
    print to_list(read_table(other_table))
    print "SORT"
    sort_table(table)
    print to_list(read_table(table))

    print "MAP"
    run_map("PYTHONPATH=. python my_op.py",
            table, other_table, files=["tests/my_op.py", "tests/helpers.py"])
    print to_list(read_table(other_table))

    print "READ RANGE"
    sort_table(other_table)
    print to_list(read_table(other_table + "[(key0):(x1)]"))
    print "REDUCE"
    run_reduce("./cpp_bin", other_table, table, files=["tests/cpp_bin"])
    print to_list(read_table(table))

