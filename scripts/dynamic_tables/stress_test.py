#!/usr/bin/python

import yt.wrapper as yt
import yt.yson as yson
import yt_driver_bindings
from yt.common import YtError
from yt.wrapper.client import Yt
from yt.wrapper.native_driver import make_request
import argparse
import random
from time import sleep
import sys
import traceback
import itertools

LOCAL_FILES=["driver.conf"]

class TInt64():
    def random(self):
        return yson.YsonInt64(random.randint(-2**63, 2**63 - 1))
    def str(self):
        return "int64"
    def comparable(self):
        return True
    def aggregatable(self):
        #return ["sum", "min", "max"]
        return ["min", "max"]
    def aggregate(self, function, lhs, rhs):
        if function == "sum":
            r = lhs + rhs
            if r < -2**63:
                r += 2**64
            elif r > 2**63 - 1:
                r -= 2**64
            return r
        elif function == "max":
            return max(lhs, rhs)
        elif function == "min":
            return min(lhs, rhs)

class TUnt64():
    def random(self):
        return yson.YsonUint64(random.randint(0, 2**64 - 1))
    def str(self):
        return "uint64"
    def comparable(self):
        return True
    def aggregatable(self):
        #return ["sum", "min", "max"]
        return ["min", "max"]
    def aggregate(self, function, lhs, rhs):
        if function == "sum":
            return (lhs + rhs) % 2**64
        elif function == "max":
            return max(lhs, rhs)
        elif function == "min":
            return min(lhs, rhs)

class TBoolean():
    def random(self):
        return yson.YsonBoolean(random.randint(0,1))
    def str(self):
        return "boolean"
    def comparable(self):
        return True
    def aggregatable(self):
        return None

class TDouble():
    def random(self):
        return yson.YsonDouble(random.uniform(-2**100,2**100))
    def str(self):
        return "double"
    def comparable(self):
        return True
    def aggregatable(self):
        return None

class TString():
    def random(self):
        def generate_string():
            length = random.randint(1,100)
            s = ''.join((chr(x) for x in (random.randint(0x41, 0x58) for i in xrange(length))))
            return "start_{0}_end".format(s)
        return yson.YsonString(generate_string())
    def str(self):
        return "string"
    def comparable(self):
        return True
    def aggregatable(self):
        return ["min", "max"]
    def aggregate(self, function, lhs, rhs):
        if function == "max":
            return max(lhs, rhs)
        elif function == "min":
            return min(lhs, rhs)

class TAny():
    def random(self):
        return [{},{}]
    def str(self):
        return "any"
    def comparable(self):
        return False
    def aggregatable(self):
        return None

types = [TInt64(), TUnt64(), TBoolean(), TString(), TAny()]
key_types = [t for t in types if t.comparable()]
types_map = {t.str(): t for t in types}

class Column():
    def __init__(self, ttype, name, sort_order=None, aggregate=None):
        self.type = ttype
        self.name = name
        self.sort_order = sort_order
        self.aggregate = aggregate
    def yson(self):
        y = {"name": self.name, "type": self.type.str()}
        if self.sort_order:
            y["sort_order"] = self.sort_order
        if self.aggregate:
            y["aggregate"] = self.aggregate
        return y
    def generate_value(self):
        return self.type.random()
    def do_aggregate(self, lhs, rhs):
        if lhs == None:
            return rhs
        elif rhs == None:
            return lhs
        else:
            return self.type.aggregate(self.aggregate, lhs, rhs)

class Schema():
    def __init__(self):
        self.appearance_probability = 0.99
        self.aggregate_probability = 0.5
        key_column_count = random.randint(3,5)
        data_column_count = random.randint(5,10)
        key_columns = [random.choice(key_types) for i in xrange(key_column_count)]
        data_columns = [random.choice(types) for i in xrange(data_column_count)]
        key_names = ["k%s" % str(i) for i in range(len(key_columns))]
        data_names = ["v%s" % str(i) for i in range(len(data_columns))]
        self.key_columns = [Column(t, n, "ascending") for (t,n) in zip(key_columns, key_names)]
        def aggr(t):
            def random_aggr(l):
                return l[random.randint(0, len(l) - 1)]
            return random_aggr(t.aggregatable()) if t.aggregatable() and random.random() < self.aggregate_probability else None
        self.data_columns = [Column(t, n, None, aggr(t)) for (t,n) in zip(data_columns, data_names)]
        self.columns = self.key_columns + self.data_columns
    def from_yson(self, yson):
        self.key_columns = []
        self.data_columns = []
        for c in yson:
            column = Column(types_map[c["type"]], c["name"], c.get("sort_order", None), c.get("aggregate", None))
            if "sort_order" in c and c["sort_order"] == "ascending":
                self.key_columns.append(column)
            else:
                self.data_columns.append(column)
        self.columns = self.key_columns + self.data_columns
    def get_key_column_names(self):
        return [c.name for c in self.key_columns]
    def get_data_column_names(self):
        return [c.name for c in self.data_columns]
    def get_column_names(self):
        return [c.name for c in self.columns]
    def get_key_columns(self):
        return self.key_columns
    def get_data_columns(self):
        return self.data_columns
    def create_pivot_keys(self, tablet_count):
        self.pivot_keys = self.generate_pivot_keys(tablet_count)
        return self.pivot_keys
    def get_pivot_keys(self):
        return self.pivot_keys
    def yson(self):
        return [c.yson() for c in self.columns]
    def flaten_key(self, map_key):
        list_key = []
        for c in self.key_columns:
            list_key.append(map_key.get(c.name, None))
        return list_key
    def generate_key(self):
        return self.generate_row_from_schema(self.key_columns)
    def generate_data(self):
        return self.generate_row_from_schema(self.data_columns)
    def generate_row(self):
        return self.generate_row_from_schema(self.columns)
    def generate_row_from_schema(self, columns):
        while True:
            result = {c.name: c.generate_value() for c in columns if random.random() < self.appearance_probability}
            if len(result) > 0:
                return result
    def generate_pivot_key(self):
        return [c.generate_value() if random.random() < self.appearance_probability else None for c in self.key_columns]
    def generate_pivot_keys(self, tablet_count):
        if tablet_count <= 1:
            return [[]]
        pivots = sorted([self.generate_pivot_key() for i in xrange(tablet_count - 1)])
        unique_pivots = [pivots[0]]
        for pivot in pivots[1:]:
            if pivot != unique_pivots[-1]:
                unique_pivots.append(pivot)
        return [[]] + unique_pivots


class SchemafulMapper(object):
    def __init__(self, schema, table):
        #self.sleep_interval = args.sleep_interval
        #self.max_retry_count = args.max_retry_count
        self.sleep_interval = 120
        self.max_retry_count = 10
        self.schema = schema
        self.table = table

    def make_request(self, command, params, data, client):
        errors = []
        attempt = 0
        while attempt < self.max_retry_count:
            attempt += 1
            try:
                return make_request(command, params, data=data, client=client)
            except YtError as error:
                errors.append((attempt, str(error)))
                sleep(random.randint(1, self.sleep_interval))
        errors = ["try: %s\nerror:%s\n" % (attempt, err) for attempt, err in errors]
        errors = [e +  "\n\n===================================================================\n\n" for e in errors]
        stderr = "".join(errors)
        print >> sys.stderr, stderr
        #print >> sys.stderr, data
        raise Exception(" ".join(("Failed to execute command (%s attempts):" % attempt, command, str(params))))

    def create_client(self):
        return Yt(config={"backend": "native", "driver_config_path": "driver.conf"})

    def prepare(self, value):
        if not isinstance(value, list):
            value = [value]
        return yson.dumps(value, yson_type="list_fragment", yson_format="text")

def compare_keys(a, b):
    def compare_values(a,b):
        if a == None and b == None: return 0
        if a == None: return -1
        if b == None: return 1
        if a == b: return 0
        return -1 if a < b else 1
    for i in xrange(min(len(a), len(b))):
        res = compare_values(a[i], b[i])
        if res != 0: return res
    if len(a) == len(b): return 0
    return -1 if len(a) < len(b) else 1

def create_keys(schema, dst, count, job_count):
    print "Generate random keys"
    class Mapper():
        def __init__(self, schema):
            self.schema = schema
        def __call__(self, record):
            pivot_keys = schema.get_pivot_keys()
            count = [0] * len(pivot_keys)
            total_count = 0
            fail_count = 0
            max_bucket_size = (record["count"] + len(count) - 1) / len(count)
            while total_count < record["count"] and fail_count < 2 * record["count"]:
                key = self.schema.generate_key()
                flat_key = schema.flaten_key(key)
                index = self.search_index(flat_key, pivot_keys)
                if count[index] < max_bucket_size:
                    count[index] += 1
                    total_count += 1
                    yield key
                else:
                    fail_count += 1
        def search_index(self, key, pivot_keys):
            s = 0
            t = len(pivot_keys)
            while t - s > 1:
                m = (s + t) / 2
                if compare_keys(key, pivot_keys[m]) >= 0: s = m
                else: t = m
            return s

    key_columns = schema.get_key_column_names()
    tmp = yt.create_temp_table()
    rows = [{"count": (count + job_count - 1) / job_count} for i in xrange(job_count)]
    yt.write_table(tmp, rows, raw=False)
    yt.run_map(Mapper(schema), tmp, dst, spec={"job_count": job_count, "max_failed_job_count": 100})
    yt.run_sort(dst, sort_by=key_columns)
    def reducer(key, records):
        yield next(records)
    yt.run_reduce(reducer, dst, dst, reduce_by=key_columns)
    yt.run_sort(dst, sort_by=key_columns)
    yt.remove(tmp)

def wait_until(path, state):
    while not all(x["state"] == state for x in yt.get(path + "/@tablets")):
        sleep(1)
def mount_table(path):
    sys.stdout.write("Mounting table %s... " % (path))
    yt.mount_table(path)
    wait_until(path, "mounted")
    print "done"
def unmount_table(path):
    sys.stdout.write("Unmounting table %s... " % (path))
    yt.unmount_table(path)
    wait_until(path, "unmounted")
    print "done"
def freeze_table(path):
    sys.stdout.write("Freezing table %s... " % (path))
    yt.freeze_table(path)
    wait_until(path, "frozen")
    print "done"
def unfreeze_table(path):
    sys.stdout.write("Unfreezing table %s... " % (path))
    yt.unfreeze_table(path)
    wait_until(path, "mounted")
    print "done"

def create_dynamic_table(table, schema, attributes, tablet_count):
    print "Create dynamic table %s" % table
    attributes["dynamic"] = True
    attributes["schema"] = schema.yson()
    yt.create_table(table, attributes=attributes)
    owner = yt.get(table + "/@owner")
    yt.set(table + "/@acl", [{"permissions": ["mount"], "action": "allow", "subjects": [owner]}])
    yt.reshard_table(table, schema.create_pivot_keys(tablet_count))
    mount_table(table)

def create_dynamic_table_from_data(data_table, table, schema, attributes, tablet_count):
    print "Create dynamic table %s from %s" % (table, data_table)
    yschema = yson.YsonList(schema.yson())
    yschema.attributes["strict"] = True
    yschema.attributes["unique_keys"] = True
    # TODO: chunk_writer config here
    attributes["schema"] = yschema
    yt.create_table(table, attributes=attributes)
    yt.run_merge(data_table, table, mode="ordered", spec={"job_io": {"table_writer": {"block_size": 256 * 2**10}}})
    yt.alter_table(table, dynamic=True)
    owner = yt.get(table + "/@owner")
    yt.set(table + "/@acl", [{"permissions": ["mount"], "action": "allow", "subjects": [owner]}])
    yt.reshard_table(table, schema.get_pivot_keys())
    yt.set(table + "/@enable_tablet_balancer", False)
    mount_table(table)

def reshard_table(table, schema, tablet_count):
    unmount_table(table)
    yt.reshard_table(table, schema.create_pivot_keys(tablet_count + random.randint(-3,3)))
    mount_table(table)

@yt.aggregator
class DataCreationMapper(SchemafulMapper):
    def __init__(self, schema, table):
        super(DataCreationMapper, self).__init__(schema, table)
    def __call__(self, records):
        for record in records:
            for k in record.keys():
                if k[0] == '@':
                    record.pop(k)
            data = self.schema.generate_data()
            record.update(data)
            yield record

def create_random_data(schema, key_table, iter_table, iteration, job_count):
    print "Generate random data, iteration %s" % iteration
    yt.run_map(
        DataCreationMapper(schema, iter_table),
        key_table,
        iter_table,
        spec={"job_count": job_count, "max_failed_job_count": 100},
        format=yt.YsonFormat(),
        local_files=LOCAL_FILES)
    yt.run_sort(iter_table, sort_by=schema.get_key_column_names())

@yt.aggregator
class WriterMapper(SchemafulMapper):
    def __init__(self, schema, table, aggregate, update):
        super(WriterMapper, self).__init__(schema, table)
        self.aggregate = aggregate
        self.update = update
    def __call__(self, records):
        rows = []
        for record in records:
            for k in record.keys():
                if k[0] == '@':
                    record.pop(k)
            rows.append(record)

        client = self.create_client()
        params = {
            "path": self.table,
            "input_format": "yson",
            "aggregate": self.aggregate,
            "update": self.update,
        }
        self.make_request("insert_rows", params, self.prepare(rows), client)

        if False:
            yield None

def write_data(schema, iter_table, table, iteration, aggregate, update, job_count):
    print "Write data, iteration %s" % iteration
    tmp_table = yt.create_temp_table()
    yt.run_map(
        WriterMapper(schema, table, aggregate, update),
        iter_table,
        tmp_table,
        spec={"job_count": job_count, "max_failed_job_count": 100},
        local_files=LOCAL_FILES)
    yt.remove(tmp_table)

class AggregateReducer:
    def __init__(self, schema, aggregate, update):
        self.schema = schema
        self.aggregate = aggregate
        self.update = update
        self.aggregates = {}
        if aggregate:
            for c in schema.get_data_columns():
                if c.aggregate:
                    self.aggregates[c.name] = c
    def __call__(self, key, records):
        records = list(records)
        records = sorted(records, key=lambda(x): x["@table_index"])
        record = dict(key)
        for c in self.schema.get_data_column_names():
            record[c] = None
        for r in records:
            for c in self.schema.get_data_column_names():
                if c not in r.keys() and self.update == False:
                    r[c] = None
                if c in r.keys():
                    if c in self.aggregates.keys():
                        record[c] = self.aggregates[c].do_aggregate(record[c], r[c])
                    elif c in self.schema.get_column_names():
                        record[c] = r[c]
        yield record

def aggregate_data(schema, data_table, iter_table, new_data_table, aggregate, update, job_count):
    print "Aggregate data"
    key_columns = schema.get_key_column_names()
    yt.run_reduce(
        AggregateReducer(schema, aggregate, update),
        [data_table, iter_table],
        new_data_table,
        reduce_by=key_columns,
        format=yt.YsonFormat(control_attributes_mode="row_fields"),
        local_files=LOCAL_FILES)
    yt.run_sort(new_data_table, sort_by=key_columns)

def equal(columns, x, y):
    if (x == None) + (y == None) > 0:
        return (x == None) == (y == None)
    for c in columns:
        if ((c in x) != (c in y)) or ((c in x) and (x[c] != y[c])):
            return False
    return True

@yt.aggregator
class VerifierMapper(SchemafulMapper):
    def __init__(self, schema, table):
        super(VerifierMapper, self).__init__(schema, table)
    def __call__(self, records):
        client = self.create_client()
        params = {
            "path": self.table,
            "input_format": "yson",
            "output_format": "yson",
            "keep_missing_rows": True,
        }

        records = list(records)
        keys = []
        for record in records:
            for k in record.keys():
                if k[0] == '@':
                    record.pop(k)
            key = {}
            for k in self.schema.get_key_column_names():
                key[k] = record[k]
            keys.append(key)

        data = self.make_request("lookup_rows", params, self.prepare(keys), client)
        results = yson.loads(data, yson_type="list_fragment")

        for i in xrange(len(keys)):
            record = records[i]
            result = next(results, None)
            if not equal(self.schema.get_column_names(), result, record):
                yield {"expected": record, "actual": result}

def verify(schema, data_table, table, result_table, job_count):
    print "Verify data"
    yt.set_attribute(result_table, "stack", traceback.format_stack())
    yt.run_map(
        VerifierMapper(schema, table),
        data_table,
        result_table,
        spec={"job_count": job_count, "max_failed_job_count": 100},
        local_files=LOCAL_FILES)
    rows = yt.read_table(result_table, raw=False)
    if next(rows, None) != None:
        raise Exception("Verification failed")
    print "Everything OK"

class VerifierReducer(SchemafulMapper):
    def __init__(self, schema):
        self.schema = schema
    def __call__(self, key, records):
        rows = [[], []]
        for r in records:
            rows[r["@table_index"]].append(r)
        if len(rows[0]) > 1 or len(rows[1]) > 1:
            for i in [0, 1]:
                rows[i] = {"variant%s" % j : rows[i][j] for j in len(rows[i])}
            yield {"expected": rows[0], "actual": rows[1]}
        else:
            for i in [0, 1]:
                rows[i] = rows[i][0] if len(rows[i]) == 1 else None
        if not equal(self.schema.get_column_names(), rows[0], rows[1]):
            yield {"expected": rows[0], "actual": rows[1]}

def verify_output(schema, data_table, dump_table, result_table):
    print "Verify output"
    key_columns = schema.get_key_column_names()

    yt.run_sort(dump_table, sort_by=key_columns)
    yt.set_attribute(result_table, "stack", traceback.format_stack())
    yt.run_reduce(
        VerifierReducer(schema),
        [data_table, dump_table],
        result_table,
        reduce_by=key_columns,
        format=yt.YsonFormat(control_attributes_mode="row_fields"),
        local_files=LOCAL_FILES)

    rows = yt.read_table(result_table, raw=False)
    if next(rows, None) != None:
        raise Exception("Verification failed")
    yt.remove(dump_table)
    print "Everything OK"

@yt.reduce_aggregator
class SelectReducer(SchemafulMapper):
    def __init__(self, schema, table, key_columns):
        super(SelectReducer, self).__init__(schema, table)
        self.key_columns = key_columns
    def __call__(self, row_groups):
        client = self.create_client()

        keys = []
        for key, rows in row_groups:
            row = next(rows)
            keylist = ["null" if row.get(k, None) == None else yson.dumps(row[k]) for k in self.key_columns]
            keys.append("(%s)" % (",".join(keylist)))

        query = "* from [%s] where (%s) in (%s)" % (self.table, ",".join(self.key_columns), ",".join(keys))
        params = {
            "query": query,
            "output_format": "yson",
        }
        data = self.make_request("select_rows", params, "", client=client)
        rows = yson.loads(data, yson_type="list_fragment")
        for row in rows:
            yield row

def verify_select(schema, data_table, table, dump_table, result_table, job_count, key_columns):
    def check_bool(schema, key_columns):
        yschema = schema.yson()
        for k in key_columns:
            for c in yschema:
                if c["name"] == k and c["type"] != "boolean":
                    return True
        return False
    print "Verify select for key %s" % (key_columns)
    if not check_bool(schema, key_columns):
        print "Disabled since all keys are boolean"
        return
    yt.run_reduce(
        SelectReducer(schema, table, key_columns),
        data_table,
        dump_table,
        reduce_by=key_columns,
        spec={"job_count": job_count, "max_failed_job_count": 100},
        local_files=LOCAL_FILES)
    verify_output(schema, data_table, dump_table, result_table)

def wait_interruptable_op(op):
    period = 10
    op.wait()
    #while True:
    #    sleep(period)
    #    state = op.get_state()
    #    if state.is_running():
    #        print "running. will interrupt jobs"
    #        jobs = yt.get("//sys/scheduler/orchid/scheduler/operations/{0}/running_jobs".format(op.id))
    #        for job, value in jobs.iteritems():
    #            if random.random() >= 0.1 or len(jobs) < 10 or value["job_type"] not in ["map", "reduce"]:
    #                continue
    #            try: yt.abort_job(job, interrupt_timeout=10000)
    #            except: pass
    #    else:
    #        print "Operation is not in running state after {} seconds, wait without interrupts".format(period)
    #        op.wait()
    #        break

def verify_merge(schema, data_table, table, dump_table, result_table, mode):
    print "Run %s merge" % (mode)
    yt.run_merge(table, dump_table, mode=mode)
    verify_output(schema, data_table, dump_table, result_table)

def verify_map(schema, data_table, table, dump_table, result_table, ordered):
    print "Run %s map" % ("ordered" if ordered else "unordered")
    def mapper(record):
        yield record
    op = yt.run_map(mapper, table, dump_table, ordered=ordered, sync=False)
    wait_interruptable_op(op)
    verify_output(schema, data_table, dump_table, result_table)

def verify_sort(schema, data_table, table, dump_table, result_table, sort_by):
    print "Run sort by %s" % (sort_by)
    yt.run_sort(table, dump_table, sort_by=sort_by)
    verify_output(schema, data_table, dump_table, result_table)

def verify_reduce(schema, data_table, table, dump_table, result_table, reduce_by):
    print "Run reduce by %s" % (reduce_by)
    def reducer(key, records):
        for r in records:
            yield r
    op = yt.run_reduce(reducer, table, dump_table, reduce_by=reduce_by, sync=False)
    wait_interruptable_op(op)
    verify_output(schema, data_table, dump_table, result_table)

def verify_map_reduce(schema, data_table, table, dump_table, result_table, reduce_by):
    print "Run map reduce by %s" % (reduce_by)
    def mapper(record):
        yield record
    def reducer(key, records):
        for r in records:
            yield r
    op = yt.run_map_reduce(mapper, reducer, table, dump_table, reduce_by=reduce_by, sync=False)
    wait_interruptable_op(op)
    verify_output(schema, data_table, dump_table, result_table)

class VerifierEqualReducer(SchemafulMapper):
    def __init__(self, columns):
        self.columns = columns
    def __call__(self, key, records):
        rows = [[], []]
        for r in records:
            rows[r["@table_index"]].append(r)
        if len(rows[0]) != len(rows[1]):
            yield{"row": rows[0][0], "expected": len(rows[0]), "actual": len(rows[1])}
        else:
            rows = [sorted(r) for r in rows]
            for i in xrange(len(rows[0])):
                if not equal(self.columns, rows[0][i], rows[1][i]):
                    yield {"expected": rows[0][i], "actual": rows[1][i]}

def verify_equal(columns, expected_table, actual_table, result_table):
    print "Verify output"

    yt.run_sort(expected_table, sort_by=columns)
    yt.run_sort(actual_table, sort_by=columns)
    yt.set_attribute(result_table, "stack", traceback.format_stack())
    yt.run_reduce(
        VerifierEqualReducer(columns),
        [expected_table, actual_table],
        result_table,
        reduce_by=columns,
        format=yt.YsonFormat(control_attributes_mode="row_fields"),
        local_files=LOCAL_FILES)

    rows = yt.read_table(result_table, raw=False)
    if next(rows, None) != None:
        raise Exception("Verification failed")
    yt.remove(expected_table)
    yt.remove(actual_table)
    print "Everything OK"

def generate_selector(schema, table):
    #FXIME(savrus): remove when ypath supports Null
    def good_key(key):
        for k in key:
            if k == None: return False
        return True
    keys = yt.get_attribute(table, "pivot_keys")
    keys = [key for key in keys if good_key(key)]
    keys = random.sample(keys, 1)
    while len(keys) < 2:
        key = schema.generate_pivot_key()
        if good_key(key):
            keys.append(key)
    keys = sorted(keys)
    skeys = [",".join([yson.dumps(k) for k in key]) for key in keys]

    columns = [c.name for c in schema.columns if c.type.str() != "any" and random.random() > 0.5]
    scols = ",".join(columns)

    # FIXME(savrus) enable for 19.2
    #selector="{{{0}}}[({1}):({2})]".format(scols, skeys[0], skeys[1])
    selector="{{{0}}}".format(scols)
    return selector, columns

def verify_extract_merge(schema, data_table, table, dump_table, result_table):
    selector, columns = generate_selector(schema, table)
    print "Run unordered merge with selector {0}".format(selector)

    dump_table_dynamic = dump_table + ".dynamic"
    dump_table_static = dump_table + ".static"

    yt.run_merge(table + selector, dump_table_dynamic, mode="unordered")
    yt.run_merge(data_table + selector, dump_table_static, mode="unordered")
    verify_equal(columns, dump_table_static, dump_table_dynamic, result_table)

def verify_extract_map(schema, data_table, table, dump_table, result_table):
    selector, columns = generate_selector(schema, table)
    print "Run unordered map with selector {0}".format(selector)

    dump_table_dynamic = dump_table + ".dynamic"
    dump_table_static = dump_table + ".static"

    def mapper(record):
        yield record
    op = yt.run_map(mapper, table + selector, dump_table_dynamic, ordered=False, sync=False)
    wait_interruptable_op(op)

    yt.run_merge(data_table + selector, dump_table_static, mode="unordered")
    verify_equal(columns, dump_table_static, dump_table_dynamic, result_table)

def verify_mapreduce(schema, data_table, table, dump_table, result_table):
    key_columns = schema.get_key_column_names()

    verify_merge(schema, data_table, table, dump_table, result_table, "unordered")
    verify_merge(schema, data_table, table, dump_table, result_table, "ordered")
    verify_merge(schema, data_table, table, dump_table, result_table, "sorted")
    verify_sort(schema, data_table, table, dump_table, result_table, key_columns)
    verify_sort(schema, data_table, table, dump_table, result_table, list(reversed(key_columns)))
    verify_map(schema, data_table, table, dump_table, result_table, False)
    verify_map(schema, data_table, table, dump_table, result_table, True)
    verify_reduce(schema, data_table, table, dump_table, result_table, key_columns)
    verify_map_reduce(schema, data_table, table, dump_table, result_table, key_columns)

    if len(key_columns) > 1:
        verify_sort(schema, data_table, table, dump_table, result_table, key_columns[:-1])
        verify_reduce(schema, data_table, table, dump_table, result_table, key_columns[:-1])
        verify_map_reduce(schema, data_table, table, dump_table, result_table, key_columns[:-1])

    verify_extract_merge(schema, data_table, table, dump_table, result_table)
    verify_extract_map(schema, data_table, table, dump_table, result_table)

def remove_existing(paths, force):
    for path in paths:
        if yt.exists(path):
            if force:
                yt.remove(path)
            else:
                raise Exception("Table %s already exists. Use --force" % path)

def single_iteration(schema, table, key_table, data_table, dump_table, result_table, iterno, args):
    job_count = args.job_count
    force = args.force
    mapreduce = not args.nomapreduce

    aggregate_probability = 0.9
    update_probability = 0.5
    aggregate = random.random() < aggregate_probability
    update = random.random() < update_probability

    new_data_table = data_table + ".new"
    iter_table = table + ".iter.(%s-%s-%s)" % (iterno, aggregate, update)
    remove_existing([iter_table], force)

    create_random_data(schema, key_table, iter_table, iterno, job_count)
    write_data(schema, iter_table, table, iterno, aggregate, update, job_count)
    aggregate_data(schema, data_table, iter_table, new_data_table, aggregate, update, job_count)
    verify(schema, new_data_table, table, result_table, job_count)

    key_columns = schema.get_key_column_names()
    verify_select(schema, new_data_table, table, dump_table, result_table, job_count, key_columns)
    verify_select(schema, new_data_table, table, dump_table, result_table, job_count, key_columns[:-1])

    freeze_table(table)
    unfreeze_table(table)

    if mapreduce:
        verify_mapreduce(schema, new_data_table, table, dump_table, result_table)

    yt.move(new_data_table, data_table, force=True)

    #TODO delete a few rows here.

    return iter_table

def do_single_execution(table, schema, attributes, args):
    tablet_count = args.tablet_count
    key_count = args.key_count
    iterations = args.iterations
    job_count = args.job_count
    force = args.force
    keep = args.keep
    mapreduce = not args.nomapreduce

    key_table = table + ".keys"
    data_table = table + ".data"
    result_table = table + ".result"
    dump_table = table + ".dump"
    remove_existing([table, key_table, data_table, result_table, dump_table], force)
    yt.create_table(data_table)
    yt.create_table(result_table)

    schema.create_pivot_keys(tablet_count)
    create_keys(schema, key_table, key_count, job_count)
    create_random_data(schema, key_table, data_table, 0, job_count)
    create_dynamic_table_from_data(data_table, table, schema, attributes, tablet_count)
    empty_table = yt.create_temp_table(attributes={"schema": schema.yson()})
    aggregate_data(schema, empty_table, data_table, data_table, False, True, job_count)
    yt.remove(empty_table)
    verify(schema, data_table, table, result_table, job_count)

    key_columns = schema.get_key_column_names()
    verify_select(schema, data_table, table, dump_table, result_table, job_count, key_columns)
    verify_select(schema, data_table, table, dump_table, result_table, job_count, key_columns[:-1])

    if mapreduce:
        verify_mapreduce(schema, data_table, table, dump_table, result_table)

    iter_tables = []
    for i in xrange(1, iterations + 1):
        iter_table = single_iteration(schema, table, key_table, data_table, dump_table, result_table, i, args)
        iter_tables.append(iter_table)
    if not keep:
        for path in [table, key_table, data_table, result_table] + iter_tables:
            yt.remove(path)

def single_execution(table, schema, attributes, args):
    #schema.from_yson(yt.get_attribute(table, "schema"))
    try:
         do_single_execution(table, schema, attributes, args)
    except Exception as ex:
        sys.stdout.write(traceback.format_exc())
        print "Test %s failed\n" % (table)

def variate_modes(table, args):
    schema = Schema()
    externals = [False] if args.noexternal else [True, False]

    for external, optimize_for in itertools.product(externals, ["scan", "lookup"]):
        single_execution(table + ".ext" + str(external) + "." + optimize_for + ".none", schema, {"external": external, "optimize_for": optimize_for}, args)
        single_execution(table + ".ext" + str(external) + "." + optimize_for + ".compressed", schema, {"external": external, "optimize_for": optimize_for, "in_memory_mode": "compressed"}, args)
        single_execution(table + ".ext" + str(external) + "." + optimize_for + ".uncompressed", schema, {"external": external, "optimize_for": optimize_for, "in_memory_mode": "uncompressed"}, args)
        single_execution(table + ".ext" + str(external) + "." + optimize_for + ".uncompressed.lookuptable", schema, {"external": external, "optimize_for": optimize_for, "in_memory_mode": "uncompressed", "enable_lookup_hash_table": True}, args)

def run_test(args):
    #module_filter = lambda module: hasattr(module, "__file__") and \
    #                               not module.__file__.endswith(".so") and \
    #                               "yt_yson_bindings" not in getattr(module, "__name__", "") and \
    #                               "yt_driver_bindings" not in getattr(module, "__name__", "")
    #yt.config["pickling"]["module_filter"] = module_filter

    for i in range(args.generations):
        variate_modes(args.table + "." + str(i), args)

def main():
    parser = argparse.ArgumentParser(description="Map-Reduce table manipulator.")
    parser.add_argument("--force", action="store_true", default=False, help="Overwrite destination table if it exists")
    parser.add_argument("--keep", action="store_true", default=False, help="Keep tables anyway")
    parser.add_argument("--table", type=str, help="Table path", required=True)
    parser.add_argument("--proxy", type=yt.config.set_proxy, help="YT proxy")
    parser.add_argument("--key_count", type=int, default=1000, help="Nuber of keys in dynamic table")
    parser.add_argument("--job_count", type=int, default=10, help="Nuber of jobs")
    parser.add_argument("--tablet_count", type=int, default=10, help="Nuber of tablets")
    parser.add_argument("--iterations", type=int, default=2, help="Nuber of iterations")
    parser.add_argument("--generations", type=int, default=100, help="Number of generations")
    parser.add_argument("--nomapreduce", action="store_true", default=False, help="Test map-reduce over dynamic tables")
    parser.add_argument("--noexternal", action="store_true", default=False, help="Do not create external tables")
    args = parser.parse_args()

    run_test(args)

if __name__ == "__main__":
    main()
