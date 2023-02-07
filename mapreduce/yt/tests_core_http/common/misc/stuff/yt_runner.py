import json

import pytest

from mapreduce.yt.python.yt_stuff import YtStuff, YtConfig

from yql_ports import get_yql_port
from yql_utils import prepare_program, log

import time

YT_TGZ = 'yt.tgz'  # comes to cwd
YT_PREFIX = '//'
YT_TOKEN = None
YT_SERVER_HOST = 'localhost'

START_LOCAL_YT = (YT_SERVER_HOST == 'localhost')
YT_SERVER_PORT = None if START_LOCAL_YT else 80

KSV_ROW_SPEC = '''
{
    "Type"=["StructType";[
        ["key";["DataType";"String"]];
        ["subkey";["DataType";"String"]];
        ["value";["DataType";"String"]]
    ]]
}
'''


class YT(YtStuff):

    def __init__(self, *args, **kwargs):
        super(YT, self).__init__(*args, **kwargs)
        self.yt_proxy_port = get_yql_port('yt')
        self.table_prefix = YT_PREFIX

    def mkdir(self, path, recursive=None):
        self.yt_wrapper.mkdir(path=path, recursive=recursive)

    def remove_table(self, name):
        self.yt_wrapper.remove(name, force=True)

    def list_tables(self, path='/', absolute=False):
        return self.yt_wrapper.list(path=path, absolute=absolute)

    def drop_all(self, path='/'):
        if YT_SERVER_HOST != 'localhost':
            log('Will not drop all on ' + YT_SERVER_HOST)
            return
        tables = self.yt_wrapper.list(path)
        if path != '/':
            tables = map(lambda table: '%s/%s' % (path, table), tables)
        log(tables)
        for table in tables:
            if path != '/':
                self.remove_table(table)
            else:
                if table not in ('home', 'sys', 'tmp'):
                    self.remove_table(table)

    def write_table(self, name, content, attrs=None, format='yson'):
        full_name = self.table_prefix + name
        self.remove_table(full_name)

        import yt.yson
        mattrs = dict()
        if attrs:
            mattrs = yt.yson.loads(attrs)

        if 'type' in mattrs:
            type_attr = mattrs['type']
            del mattrs['type']
            if type_attr == 'document':
                yql_type = mattrs['_yql_type']
                if yql_type == 'view':
                    self.yt_wrapper.create(type_attr, path=full_name, attributes=mattrs or None, recursive=True)
                    self.yt_wrapper.set(full_name, content)
                    return
                else:
                    raise Exception('Unsupported yql type: %s' % yql_type)
            else:
                raise Exception('Unsupported type: %s' % type_attr)

        schema_attrs = {
            'schema', '_yql_row_spec', '_read_schema',
            '_yql_key_meta', '_yql_subkey_meta', '_yql_value_meta',
            'infer_schema', '_format'
        }
        if not (schema_attrs & set(mattrs.iterkeys())):
            mattrs['_yql_row_spec'] = yt.yson.loads(KSV_ROW_SPEC)

        if 'infer_schema' in mattrs:
            mattrs.pop('infer_schema')
        is_dynamic = mattrs.pop('_yql_dynamic', False)

        self.yt_wrapper.create_table(full_name, attributes=mattrs or None, recursive=True)

        sorted_by = None
        if len(mattrs):
            log(mattrs)
            sorted_by = self.get_sort_by(mattrs)

        if len(content) > 0:
            log('=====')
            log(content)
            log('=====')
            if sorted_by:
                log('sorted_by' + str(sorted_by))
            self.yt_wrapper.write_table(
                self.yt_wrapper.TablePath(full_name, sorted_by=sorted_by) if sorted_by else full_name,
                content,
                raw=True,
                format=self.yt_wrapper.YsonFormat()
            )

        if is_dynamic:
            self.yt_wrapper.alter_table(full_name, dynamic=True)
            self.yt_wrapper.mount_table(full_name)
            while not all(x['state'] == 'mounted' for x in self.yt_wrapper.get_attribute(full_name, 'tablets')):
                time.sleep(0.1)

    def write_metaattrs(self, name, metaattrs):
        for (k, v) in metaattrs.items():
            self.yt_wrapper.set(self.table_prefix + name + '/@' + k, v)

    def get_metaattrs(self, name):
        return self.yt_wrapper.get(self.table_prefix + name + '/@')

    def get_important_metaattrs(self, name):
        if self.yt_wrapper.exists(name):
            attrs = self.get_metaattrs(name)
            important_attrs = [
                'optimize_for',
                'user_attribute_keys',
                '_yql_row_spec',
                'schema',
                'sorted',
            ]
            return json.dumps({k: str(v) for k, v in attrs.items() if k in important_attrs}, indent=4)
        return None

    def get_sort_by(self, metaattrs):
        if '_yql_row_spec' in metaattrs:
            spec = metaattrs['_yql_row_spec']
            if 'SortedBy' in spec:
                return spec['SortedBy']
        if 'schema' in metaattrs:
            spec = metaattrs['schema']
            sortedBy = [col['name'] for col in spec if col.get('sort_order') == 'ascending']
            if sortedBy:
                return sortedBy
        return None

    def read_table(self, name):
        full_name = self.table_prefix + name
        if self.yt_wrapper.exists(full_name):
            res = self.yt_wrapper.read_table(full_name)
            import yt.yson
            return ''.join(yt.yson.dumps(r) + ';\n' for r in res)
        else:
            return ''

    def get_user_transactions(self):
        service_transaction_patterns = [
            'Scheduler lock',
            'Lease for node',
            'sys/scheduler/event_log',
            'Lock for changelog store',
            'Prerequisite for cell',
            'Upload to //sys/tablet_cells'
        ]
        if YT_SERVER_HOST == 'localhost':
            try:
                transactions = self.yt_wrapper.list('//sys/transactions', attributes=['title'])
                log([tx.attributes for tx in transactions])
                res = []
                for tx in transactions:
                    title = tx.attributes.get('title', '')
                    if not any(p in title for p in service_transaction_patterns):
                        res.append(tx)
                return [(tx, tx.attributes) for tx in res]
            except Exception as e:
                log('While processing transactions: ' + str(e))
            return []
        else:
            log('Will not ask remote YT about transactions')
            return []

    def remove_user_transactions(self):
        for tx in self.get_user_transactions():
            self.yt_wrapper.abort_transaction(tx[0])

    def prepare_program(self, program, program_file, res_dir, lang='sql'):
        program, program_file = prepare_program(program, program_file, res_dir)
        if self.table_prefix != YT_PREFIX:
            self.mkdir(path=self.table_prefix.rstrip('/'), recursive=True)
        return program, program_file


@pytest.fixture(scope='module')
def yt(request):
    # it will be better to create YT object from YtStuff object in init,
    # so in this case you will be able to write 'instance = YT(yt_stuff)',
    # but for now you can use only yt_config and yt_stuff fixtures simultaneously
    try:
        yt_config = request.getfixturevalue("yt_config")
    except BaseException:
        yt_config = YtConfig()
    try:
        yt_stuff = request.getfixturevalue("yt_stuff")
    except BaseException:
        yt_stuff = None

    instance = YT(yt_config)
    if yt_stuff:
        # if local yt was already started (using yt_stuff) we should keep the same yt_proxy_port, yt_id and yt_work_dir
        instance.yt_proxy_port = yt_stuff.yt_proxy_port
        instance.yt_id = yt_stuff.yt_id
        instance.yt_work_dir = yt_stuff.yt_work_dir
    else:
        instance.start_local_yt()
        request.addfinalizer(instance.stop_local_yt)

    return instance
