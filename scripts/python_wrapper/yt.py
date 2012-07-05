from common import add_quotes, parse_bool, bool_to_string, flatten, require, YtError
from record import Record

import os
import sys
import random
import string
import types
import simplejson as json
from httplib2 import Http
from urllib import urlencode
from itertools import imap, izip, ifilter
from time import sleep

WAIT_TIMEOUT = 2.0
DEFAULT_PROXY = "proxy.yt.yandex.net"
#DEFAULT_PROXY = "n01-0400g.yt.yandex.net"

class Buffer(object):
    """ Reads line iterator by chunks """
    def __init__(self, lines_iterator, has_eoln=True):
        self._lines_iterator = lines_iterator
        self._empty = False
        self._has_eoln = has_eoln

    def get(self, bytes = 1000000):
        sep = "" if self._has_eoln else "\n"
        if isinstance(self._lines_iterator, types.ListType):
            self._empty = True
            return sep.join(self._lines_iterator)
        read_bytes = 0
        result = []
        while read_bytes < bytes:
            try:
                line = self._lines_iterator.next()
            except StopIteration:
                self._empty = True
                return sep.join(result)
            read_bytes += len(line)
            result.append(line)
        return sep.join(result)

    def empty(self):
        return self._empty


# TODO(ignat): Add custom field separator
class Format(object):
    pass

class DsvFormat(Format):
    def __init__(self):
        pass

    def to_mime_type(self):
        return "text/tab-separated-values"

    def to_json(self):
        return "dsv"

class YamrFormat(Format):
    def __init__(self, has_subkey, lenval):
        self.has_subkey = has_subkey
        self.lenval = lenval

    def to_mime_type(self):
        return "application/x-yamr%s-%s" % \
            ("-subkey" if self.has_subkey else "",
             "lenval" if self.lenval else "delimited")

    def to_json(self):
        return {"$value": "yamr",
                "$attributes":
                    {"has_subkey": bool_to_string(self.has_subkey),
                     "lenval": bool_to_string(self.lenval)}}

Format.default = YamrFormat(has_subkey=True, lenval=False)
   

""" Methods for records convertion """
def record_to_line(rec, eoln=True, format=None):
    if format is None: format = Format.default
    
    if isinstance(format, YamrFormat):
        require(not format.lenval, YtError("Lenval convertion is not supported now."))
        if format.has_subkey:
            fields = [rec.key, rec.subkey, rec.value]
        else:
            fields = [rec.key, rec.value]
        body = "\t".join(fields)
    else:
        body = "\t".join("=".join(map(str, item)) for item in rec.iteritems())
    return "%s%s" % (body, "\n" if eoln else "")

def line_to_record(line, format=None):
    if format is None: format = Format.default
    
    if isinstance(format, YamrFormat):
        return Record(*line.strip("\n").split("\t", 1 + (1 if format.has_subkey else 0)))
    else:
        return dict(field.split("=") for field in line.strip("\n").split("\t"))


def make_request(http_type, type, params,
                 data=None, format=None, verbose=False, proxy=None, check_errors=True):
    """ Make request to yt proxy.
        http_type may be equal to GET, POST or PUT
        type may be equal to  get, read, write, create ... """

    # Prepare request url.
    if proxy is None:
        proxy = DEFAULT_PROXY
    url = "http://{0}/api/{1}".format(proxy, type)
    if params is not None:
        url += "?" + urlencode(params)
    
    # Prepare headers
    if format is None:
        mime_type = "application/json"
    else:
        mime_type = format.to_mime_type()
    headers = {"User-Agent": "maps yt library",
               "content-type": mime_type,
               "accept": mime_type}

    if verbose:
        print >>sys.stderr, "Request url:", url
        print >>sys.stderr, "Headers:", headers
        if http_type != "PUT" and data is not None:
            print >>sys.stderr, data
    
    # Response is tuple(header, body)
    # TODO(ignat): may use one Http object for all request is more fast and appropriate 
    response = Http().request(
        url,
        http_type,
        headers=headers,
        body=data)
    if verbose:
        print >>sys.stderr, "Response header", response[0]
        print >>sys.stderr, "Response body", response[1]

    result = response[1].strip()
    if response[0]["content-type"] == "application/json":
        if result:
            result = json.loads(result)
        else:
            result = None
        
        if check_errors and isinstance(result, dict) and "error" in result:
            raise YtError(
                "Response to request {0} with headers {1} contains error: {2}".
                format(url, headers, result["error"]))
    return result


""" Common cypress methods """
def get(path):
    return make_request("GET", "get", dict(path=path))

def set(path, value):
    return make_request("PUT", "set", dict(path=path), value)

def list(path, check_existance=True, quoted=True):
    if check_existance and not exists(path):
        return []
    result = make_request("GET", "list", {"path": path})
    if quoted:
        result = imap(add_quotes, result)
    return result

def dirname(path):
    return path.rsplit("/", 1)[0]

def basename(path):
    return path.rsplit("/", 1)[1]

def exists(path):
    # TODO(ignat): use here not already existed function 'exists' from http
    names = path.strip().split("/")[1:]
    check_path = ""
    for current_name, check_name in izip(names, names[1:]):
        check_path += "/" + current_name
        if check_name.strip("\"") not in list(check_path, check_existance=False, quoted=False):
            return False
    return True

def remove(path):
    if exists(path):
        return make_request("POST", "remove", {"path": path})
    return None

def get_attribute(path, attribute):
    return get("%s/@%s" % (path, attribute))

""" Common table methods """
def create_table(path, make_it_empty=True):
    create = True
    if exists(path):
        require(get_attribute(path, "type") == "table",
                YtError("You try create table by existed path that differs from table"))
        if make_it_empty:
            remove(path)
            create = True
        else:
            create = False
    if create:
        make_request("POST", "create", {"path": path, "type": "table"})
    return path

def write_table(table, lines, format=None, append=False):
    if format is None: format = Format.default
    create_table(table, not append)
    buffer = Buffer(lines)
    while not buffer.empty():
        make_request("PUT", "write", {"path": table}, buffer.get(), format=format)
    return table

def read_table(table, format=None):
    def add_eoln(str):
        return str + "\n"
    if format is None: format = Format.default
    response = make_request("GET", "read", {"path": table}, format=format)
    return imap(add_eoln, ifilter(None, response.strip().split("\n")))

def remove_table(table):
    if exists(table) and get_attribute(table, "type") == "table":
        remove(table)

# It will be maked by user
#def read_range(table, lower, upper):
#    response = make_request("GET", "read", {"path": table + "[(%s):(%s)]" % (lower, upper)}, format="dsv")
#    return imap(yt_to_record, (line for line in response.split("\n") if line))

def copy_table(source_table, destination_table, append=False):
    source_table = flatten(source_table)
    require(destination_table not in source_table,
            YtError("Destination should differ from source tables in copy operation"))
    create_table(destination_table, make_it_empty=not append)
    mode = "sorted" if all(map(is_sorted, source_table)) else "ordered"
    params = json.dumps(
        {"spec":
            {"input_table_paths": source_table,
             "output_table_path": destination_table,
             "mode": mode}})
    operation = add_quotes(make_request("POST", "merge", None, params))
    wait_operation(operation)

def move_table(source_table, destination_table, append=False):
    copy_table(source_table, destination_table, append)
    remove(source_table)

def records_count(table):
    require(exists(table), YtError("Table %s doesn't exist" % table))
    return get_attribute(table, "row_count")

def is_sorted(table):
    require(exists(table), YtError("Table %s doesn't exist" % table))
    return parse_bool(get_attribute(table, "sorted"))

def sort_table(table, key_columns=None):
    if key_columns is None:
        key_columns= ["key", "subkey"]
    temp_table = create_temp_table(dirname(table), basename(table))
    params = json.dumps(
        {"spec":
            {"input_table_paths": [table],
             "output_table_path": temp_table,
             "key_columns": key_columns}})
    operation = add_quotes(make_request("POST", "sort", None, params))
    wait_operation(operation)
    move_table(temp_table, table)

def create_temp_table(path, prefix=None):
    require(exists(path), YtError("You cannot create table in unexisting path"))
    LENGTH = 10
    char_set = string.ascii_lowercase + string.ascii_uppercase + string.digits
    while True:
        name = "%s/%s%s" % (path, prefix, "".join(random.sample(char_set, LENGTH)))
        if not exists(name):
            create_table(name)
            return name


""" Operation is methods """
def get_operation_state(operation):
    require(exists("//sys/operations/" + operation),
            YtError("Operation %s doesn't exist" % operation))
    return get("//sys/operations/%s/@state" % operation)

def wait_operation(operation, check_failed=True, timeout=None):
    if timeout is None:
        timeout = WAIT_TIMEOUT
    while True:
        state = get_operation_state(operation)
        if state in ["completed", "aborted", "failed"]:
            require(not check_failed or (state != "failed" and state != "aborted"),
                    YtError("Operation %s completed unsuccessfully" % operation))
            return state
        sleep(timeout)

def abort_operation(operation):
    if get_operation_state(operation) not in ["completed", "aborted", "failed"]:
        make_request("POST", "abort_op", dict(operation_id=operation))

""" File methods """
def download_file(path):
    return make_request("GET", "download", {"path": path})

def upload_file(file, destination=None, replace=False):
    require(os.path.isfile(file),
            YtError("Upload: %s should be file" % file))
    set_executable = os.access(file, os.X_OK)
    if destination is None:
        destination = os.path.join("//home/files", add_quotes(os.path.basename(file)))
    if replace or not exists(destination):
        if exists(destination):
            remove(destination)
        # TODO(ignat): open(file).read() holds file in RAM. It is bad for huge files. Add buffering here.
        operation = make_request("PUT", "upload", dict(path=destination), open(file).read())
        if set_executable:
            set(destination + "/@executable", add_quotes("true"))
    return destination


""" Operation methods """
def get_stderr(operation):
    jobs = list("//sys/operations/%s/jobs" % operation)
    stderr_paths = ("//sys/operations/{0}/jobs/{1}/stderr".format(operation, job) for job in jobs)
    return "\n\n".join(download_file(path) for path in stderr_paths if exists(path))

def run_operation(binary, source_table, destination_table, files, replace_files, format, append, check_result, op_type):
    source_table = flatten(source_table)
    destination_table = flatten(destination_table)
    # TODO(ignat): support multiple output tables and different strategies of append
    require(len(destination_table) == 1, YtError("Multiple output tables are not supported yet"))
    if format is None:
        format = Format.default

    if files is None:
        files = []
    files = flatten(files)

    file_paths = []
    for file in files:
       file_paths.append(upload_file(file, replace=True))

    create_table(destination_table[0], not append)

    op_key = {
        "map": "mapper",
        "reduce": "reducer"}

    operation_descr = \
                {"command": binary,
                 "format": format.to_json(),
                 "file_paths": file_paths}
    # TODO(ignat): move it to method parameters
    if op_type == "reducer":
        operation_descr.update({"key_columns": "key"})
    
    params = json.dumps(
        {"spec":
            {"input_table_paths": source_table,
             "output_table_paths": destination_table,
             op_key[op_type]: operation_descr}})
    operation = add_quotes(make_request("POST", op_type, None, params))
    
    state = wait_operation(operation, check_failed=False)
    operation_result = make_request(
        "GET", "get", {"path": "//sys/operations/%s/@result" % operation}, None, check_errors=False)
    stderr = get_stderr(operation)
    # TODO(ignat): add parsing of jobs results
    if check_result and state in ["aborted", "failed"]:
        print >>sys.stderr, "Operation %s failed!" % operation
        print >>sys.stderr, "Result:", operation_result
        print >>sys.stderr, "Stderr:", stderr
        exit(1)
    return destination_table, operation_result, stderr

def run_map(binary, source_table, destination_table, files=None, replace_files=True, format=None, append=False, check_result=True):
    run_operation(binary, source_table, destination_table,
                  files=files, replace_files=replace_files, format=format, append=append, check_result=check_result, op_type="map")

def run_reduce(binary, source_table, destination_table, files=None, replace_files=True, format=None, append=False, check_result=True):
    run_operation(binary, source_table, destination_table,
                  files=files, replace_files=replace_files, format=format, append=append, check_result=check_result, op_type="reduce")


if __name__ == "__main__":
    def to_list(iter):
        return [x for x in iter]


    """ Some tests """
    table = "//home/ignat/temp"
    other_table = "//home/ignat/temp2"

    set("//home/files", "{}")

    print "LIST", list("//home")

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
            table, other_table, files=["test/my_op.py", "test/helpers.py"])
    print to_list(read_table(other_table))

    print "READ RANGE"
    sort_table(other_table)
    print to_list(read_table(other_table + "[(key0):(x1)]"))
    print "REDUCE"
    run_reduce("./cpp_bin", other_table, table, files=["test/cpp_bin"])
    print to_list(read_table(table))

