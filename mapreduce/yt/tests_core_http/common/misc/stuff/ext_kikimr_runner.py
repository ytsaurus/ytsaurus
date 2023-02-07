import os
import re
import hashlib

import pytest
import yatest.common

import yql_utils
from libs.harness.kikimr_runner import KiKiMR
from libs.harness.kikimr_client import kikimr_client_factory
from libs.harness.kikimr_config import KikimrConfigGenerator
from libs.common.types import Erasure
from libs.dsl.ddl import CreateDirCmd, FlatTxId

from client.python.run_kikimr import MSTATUS_OK

import kikimr.core.protos.flat_scheme_op_pb2 as flat_scheme_op_pb2
import kikimr.core.protos.msgbus_pb2 as msgbus
from google.protobuf.text_format import Merge


DELIM = ';'

KSV_ATTRS = '''
Name: "%s"
Columns {
  Name: "key"
  Type: "String"
  Id: 1
}
Columns {
  Name: "subkey"
  Type: "String"
  Id: 2
}
Columns {
  Name: "value"
  Type: "String"
  Id: 3
}
KeyColumnNames: "key"
KeyColumnNames: "subkey"
'''


class MRInterfaceWrapper(object):
    def __init__(self, kikimr):
        self.kikimr = kikimr
        self.client = kikimr_client_factory(server='localhost', port=self.kikimr.nodes[1].port)
        self.client.scheme_root_key = self.kikimr.scheme_root_key
        self.table_prefix = ''

    def replace_prefix(self, s):
        md5 = hashlib.md5(yatest.common.context.test_name).hexdigest()
        return s.replace(
            'yql_test_root',
            'yql_test_root_' + md5
        )

    def get_path(self):
        return self.replace_prefix(os.path.join(self.client.scheme_root_key.path_as_string, 'yql_test_root'))

    def write_table(self, name, content, attrs, format='yson'):
        if format != 'yson':
            pytest.skip('Unsupported format ' + format)
        if not self.kikimr.nodes[1].check_run():
            pytest.skip('Kikimr crashed, see teardown')

        csv = yql_utils.yson_to_csv(content, with_header=True)

        path = self.get_path()
        response = self.client.ddl_exec(
            CreateDirCmd(
                path
            )
        )
        if response.Status != MSTATUS_OK:
            if 'Already exists' not in str(response.ErrorReason):
                raise RuntimeError(str(response.ErrorReason))

        table_descr = flat_scheme_op_pb2.TTableDescription()
        attrs = attrs or KSV_ATTRS % name
        try:
            Merge(attrs, table_descr)
        except Exception:
            pytest.skip('can\'t set metaattrs')
        lines = csv.splitlines()
        header = lines[0]
        columns = header.split(DELIM)
        keys = set()
        needs_id = False

        for line in lines[1:]:
            values = line.split(DELIM)
            row_keys = []
            for column_name, value in zip(columns, values):
                if column_name in table_descr.KeyColumnNames:
                    row_keys.append(value)
            row_keys = tuple(row_keys)
            if row_keys in keys:
                needs_id = True
                break
            else:
                keys.add(row_keys)

        request = msgbus.TSchemeOperation()
        request.Transaction.ModifyScheme.OperationType = flat_scheme_op_pb2.ESchemeOpCreateTable
        request.Transaction.ModifyScheme.WorkingDir = path
        request.Transaction.ModifyScheme.CreateTable.MergeFrom(table_descr)
        if needs_id:
            id_column = request.Transaction.ModifyScheme.CreateTable.Columns.add()
            id_column.Name = 'id_'
            id_column.Type = 'Uint64'
            request.Transaction.ModifyScheme.CreateTable.KeyColumnNames.append('id_')

        txid = FlatTxId.from_response(
            self.client.message_bus._send_request_sync(request)
        )
        self.client.ddl_exec_status(txid)

        count = 0
        update_expr = ''
        if needs_id:
            columns.append('id_')
        types = {c.Name: c.Type for c in request.Transaction.ModifyScheme.CreateTable.Columns}

        id_ = 0
        for line in lines[1:]:
            update_row_key_expr = ''
            update_row_value_expr = ''
            values = line.split(DELIM)
            if needs_id:
                values.append(str(id_))
                id_ += 1

            for column_name, value in zip(columns, values):
                if column_name in request.Transaction.ModifyScheme.CreateTable.KeyColumnNames:
                    update_row_key_expr += '''
                        '('%s (%s '"%s"))
                    ''' % (column_name, types[column_name], (value if value.strip() else ''))
                else:
                    if value:
                        update_row_value_expr += '''
                            '('%s (%s '"%s"))
                        ''' % (column_name, types[column_name], value)

            update_row_key_expr = '''
            (let key '(%s))
            ''' % update_row_key_expr

            update_row_value_expr = '''
            (let value '(%s))
            ''' % update_row_value_expr

            update_expr += update_row_key_expr + update_row_value_expr + '''
            (let r%d (UpdateRow '"%s" key value))
            ''' % (count, path + '/' + name)
            count += 1
        update_program = '''(
            %s
            (let pgmReturn (AsList %s))
            (return pgmReturn)
        )''' % (
            update_expr,
            ' '.join('r%d' % i for i in range(count))
        )
        self.client.minikql_exec(update_program)

    def write_metaattrs(self, name, metaattrs):
        pass

    def get_important_metaattrs(self):
        return None

    def read_table(self, name, csv=True):
        type_name = ''
        path = self.get_path()
        request = msgbus.TSchemeDescribe()
        table_path = os.path.join(path, name)
        request.Path = table_path
        response = self.client.message_bus._send_request_sync(request)

        key = response.PathDescription.Table.KeyColumnNames[0]
        for c in response.PathDescription.Table.Columns:
            if c.Name == key:
                type_name = c.Type

        read_program = '''(
            (let table '%s)
            (let range '('IncTo '('%s (Nothing (OptionalType (DataType '%s))) (Void))))
            (let select '(%s))
            (let options '())
            (let res (SelectRange table range select options))
            (let reslist (Member res 'List))
            (return (AsList
                (SetResult 'list reslist)
            ))
        )''' % (
            table_path,
            key,
            type_name,
            ' '.join('\'' + c.Name for c in response.PathDescription.Table.Columns)
        )

        res = self.client.minikql_exec(read_program, parse=True).response.ExecutionEngineEvaluatedResponse
        if csv:
            _csv = ''
            table = res.Value.Struct[0].Optional
            for row in table.List:
                _csv += DELIM.join(val.Optional.Bytes for val in row.Struct) + '\n'
            return _csv.strip()
        else:
            return res

    def remove_table(self, name):
        pass

    def list_tables(self):
        return []

    def drop_all(self, path='/'):
        pass

    def prepare_program(self, program, program_file, res_dir, lang='sql'):
        program, program_file = yql_utils.prepare_program(program, program_file, res_dir)
        program = self.replace_prefix(program)
        program = re.compile(re.escape('plato'), re.IGNORECASE).sub('local_kikimr', program)
        if lang == 'sql':
            for w in (
                'kikimr can not',
                'concat',
                'filepath',
                'filecontent',
                'refselect',
                'insert',
                'ReturnBrokenInt',
                'parsefile',
                'yql::unwrap',
                'python::',
                'streaming::',
                'scriptudf',
                'window'
            ):
                if w in program.lower():
                    pytest.skip('kikimr does not support ' + w)

            path = self.get_path()[1:]  # remove first '/'
            program = ('''PRAGMA kikimr.UnwrapReadTableValues = "true";PRAGMA TablePathPrefix("local_kikimr", "%s");
''' % path) + program
            program = program.replace('-- kikimr only:', '')
        with open(program_file, 'w') as f:
            f.write(program)
        return program, program_file


class ExtKiKiMR(KiKiMR):
    def _run_node(self, node_n=None, extra_opts=None):

        if extra_opts is None:
            extra_opts = []

        extra_opts += ['--udfs-dir=' + yql_utils.get_udfs_path()]

        with open(os.path.join(self.config_path, 'kqp.txt'), 'w') as f:
            f.write('Enable: true')
        with open(os.path.join(self.config_path, 'log.txt'), 'a') as f:
            f.write('''
                Entry {
                    Component: "KQP_PROXY"
                    Level: 7
                }
                Entry {
                    Component: "KQP_WORKER"
                    Level: 7
                }
            ''')

        super(ExtKiKiMR, self)._run_node(
            node_n,
            extra_opts=extra_opts,
        )


@pytest.fixture(scope='module')
def kikimr(request):
    configurator = KikimrConfigGenerator(erasure=Erasure.NONE, nodes=1)
    ext_kikimr = ExtKiKiMR(
        client_type='commandline',
        cluster_name='ext_kikimr',
        configurator=configurator,
    )
    ext_kikimr.start()
    request.addfinalizer(ext_kikimr.stop)
    return MRInterfaceWrapper(ext_kikimr)
