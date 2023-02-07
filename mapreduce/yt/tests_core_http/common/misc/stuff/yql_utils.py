import hashlib
import os
import sys
import re
import tempfile
import shutil
import subprocess

from collections import namedtuple
from functools import partial
import codecs
import decimal


import pytest
import yatest.common

import logging
import getpass

logger = logging.getLogger(__name__)

KSV_ATTR = '''{_yql_row_spec={
    Type=[StructType;
        [[key;[DataType;String]];
        [subkey;[DataType;String]];
        [value;[DataType;String]]]]}}'''


def get_param(name, default=None):
    name = 'YQL_' + name.upper()
    return os.environ.get(name) or yatest.common.get_param(name, default)


def find_file(path):
    arcadia_root = '.'
    while '.arcadia.root' not in os.listdir(arcadia_root):
        arcadia_root = os.path.join(arcadia_root, '..')
    res = os.path.abspath(os.path.join(arcadia_root, path))
    assert os.path.exists(res)
    return res


output_path_cache = {}


def yql_output_path(*args, **kwargs):
    if not get_param('LOCAL_BENCH_XX'):
        return yatest.common.output_path(*args, **kwargs)

    else:
        if args and args in output_path_cache:
            return output_path_cache[args]
        res = os.path.join(tempfile.mkdtemp(prefix='yql_tmp_'), *args)
        if args:
            output_path_cache[args] = res
        return res


def yql_binary_path(*args, **kwargs):
    if not get_param('LOCAL_BENCH_XX'):
        return yatest.common.binary_path(*args, **kwargs)

    else:
        return find_file(args[0])


def yql_source_path(*args, **kwargs):
    if not get_param('LOCAL_BENCH_XX'):
        return yatest.common.source_path(*args, **kwargs)
    else:
        return find_file(args[0])


def yql_work_path():
    return os.path.abspath('.')


if get_param('LOCAL_BENCH_XX'):
    yatest.common.source_path = find_file
    yatest.common.binary_path = find_file
    yatest.common.output_path = yql_output_path
    yatest.common.runtime.work_path = yql_work_path


YQLExecResult = namedtuple('YQLExecResult', (
    'std_out',
    'std_err',
    'results',
    'results_file',
    'opt',
    'opt_file',
    'plan',
    'plan_file',
    'program',
    'execution_result'
))


Table = namedtuple('Table', (
    'name',
    'full_name',
    'content',
    'file',
    'yqlrun_file',
    'attr',
    'format',
    'exists'
))


def new_table(full_name, file_path=None, yqlrun_file=None, content=None, res_dir=None,
              attr=None, format_name='yson', def_attr=None, should_exist=False):

    assert '.' in full_name, 'expected name like cedar.Input'
    name = '.'.join(full_name.split('.')[1:])

    if res_dir is None:
        res_dir = get_yql_dir('table_')

    exists = True
    if content is None:
        # try read content from files
        src_file = file_path or yqlrun_file
        if src_file is None:
            # nonexistent table, will be output for query
            content = ''
            exists = False
        else:
            if os.path.exists(src_file):
                with open(src_file) as f:
                    content = f.read()
            else:
                content = ''
                exists = False

    file_path = os.path.join(res_dir, name + '.txt')
    new_yqlrun_file = os.path.join(res_dir, name + '.yqlrun.txt')

    if exists:
        with open(file_path, 'w') as f:
            f.write(content)

        # copy or create yqlrun_file in proper dir
        if yqlrun_file is not None:
            shutil.copy2(yqlrun_file, new_yqlrun_file)
        else:
            with open(new_yqlrun_file, 'w') as f:
                f.write(content)
    else:
        assert not should_exist, locals()

    if attr is None:
        # try read from file
        attr_file = None
        if os.path.exists(file_path + '.attr'):
            attr_file = file_path + '.attr'
        elif yqlrun_file is not None and os.path.exists(yqlrun_file + '.attr'):
            attr_file = yqlrun_file + '.attr'

        if attr_file is not None:
            with open(attr_file) as f:
                attr = f.read()

    if attr is None:
        attr = def_attr

    if attr is not None:
        # probably we get it, now write attr file to proper place
        attr_file = new_yqlrun_file + '.attr'
        with open(attr_file, 'w') as f:
            f.write(attr)

    return Table(
        name,
        full_name,
        content,
        file_path,
        new_yqlrun_file,
        attr,
        format_name,
        exists
    )


def get_yql_dir(prefix):
    yql_dir = yql_output_path('yql')
    if not os.path.isdir(yql_dir):
        os.mkdir(yql_dir)
    res_dir = tempfile.mkdtemp(prefix=prefix, dir=yql_dir)
    os.chmod(res_dir, 0o755)
    return res_dir


def get_cmd_for_files(arg, files):
    cmd = ' '.join(
        arg + ' ' + name + '@' + files[name]
        for name in files
    )
    cmd += ' '
    return cmd


def read_res_file(file_path):
    if os.path.exists(file_path):
        with open(file_path) as descr:
            res = descr.read().strip()
            if res == '':
                log_res = '<EMPTY>'
            else:
                log_res = res
    else:
        res = ''
        log_res = '<NOTHING>'
    return res, log_res


volatile_attrs = {'DataSize', 'ModifyTime'}

current_user = getpass.getuser()


def replace_vals(y):
    from yt.yson.yson_types import YsonBoolean, YsonEntity
    if isinstance(y, YsonBoolean) or isinstance(y, bool):
        return 'true' if y else 'false'
    if isinstance(y, YsonEntity) or y is None:
        return None
    if isinstance(y, list):
        return [replace_vals(i) for i in y]
    if isinstance(y, dict):
        return {replace_vals(k): replace_vals(v) for k, v in y.items() if k not in volatile_attrs}
    s = str(y) if not isinstance(y, unicode) else y.encode('utf-8', errors='xmlcharrefreplace')
    return s.replace('tmp/' + current_user + '/', 'tmp/')


def patch_yson_vals(y, patcher):
    if isinstance(y, list):
        return [patch_yson_vals(i, patcher) for i in y]
    if isinstance(y, dict):
        return {patch_yson_vals(k, patcher): patch_yson_vals(v, patcher) for k, v in y.items()}
    if isinstance(y, str):
        return patcher(y)
    return y


floatRe = re.compile(r'^-?\d*\.\d+$')


def fix_double(x):
    if floatRe.match(x) and len(x.replace('.', '').replace('-', '')) > 10:
        # Emulate the same double precision as C++ code has
        decimal.getcontext().rounding = decimal.ROUND_HALF_DOWN
        decimal.getcontext().prec = 10
        return str(decimal.Decimal(0) + decimal.Decimal(x)).rstrip('0')
    return x


def prepare_program(program, program_file, yql_dir):
    assert not (program is None and program_file is None), 'Needs program or program_file'

    if program is None:
        with codecs.open(program_file, encoding='utf-8') as program_file_descr:
            program = program_file_descr.read()

    program_file = os.path.join(yql_dir, 'program.yql')
    with codecs.open(program_file, 'w', encoding='utf-8') as program_file_descr:
        program_file_descr.write(program)

    return program, program_file


def get_program_cfg(suite, case, DATA_PATH):
    ret = []
    config = os.path.join(DATA_PATH, suite if suite else '', case + '.cfg')
    if not os.path.exists(config):
        config = os.path.join(DATA_PATH, suite if suite else '', 'default.cfg')

    if os.path.exists(config):
        for line in open(config, 'r'):
            if line.strip():
                ret.append(tuple(line.split()))
    else:
        in_filename = case + '.in'
        in_path = os.path.join(DATA_PATH, in_filename)
        default_filename = 'default.in'
        default_path = os.path.join(DATA_PATH, default_filename)
        for filepath in [in_path, in_filename, default_path, default_filename]:
            if os.path.exists(filepath):
                try:
                    shutil.copy2(filepath, in_path)
                except shutil.Error:
                    pass
                ret.append(('in', 'yamr.plato.Input', in_path))
                break

    if not is_os_supported(ret):
        pytest.skip('%s not supported here' % sys.platform)

    return ret


def find_user_file(suite, path, DATA_PATH):
    source_path = os.path.join(DATA_PATH, suite, path)
    if os.path.exists(source_path):
        return source_path
    else:
        try:
            return yql_binary_path(path)
        except Exception:
            raise Exception('Can not find file ' + path)


def get_input_tables(suite, cfg, DATA_PATH, def_attr=None):
    in_tables = []
    for item in cfg:
        if item[0] in ('in', 'out'):
            io, table_name, file_name = item
            if io == 'in':
                in_tables.append(new_table(
                    full_name=table_name.replace('yamr.', '').replace('yt.', ''),
                    yqlrun_file=os.path.join(DATA_PATH, suite if suite else '', file_name),
                    def_attr=def_attr,
                    should_exist=True
                ))
    return in_tables


def get_supported_providers(cfg):
    providers = 'yt_file', 'yt', 'kikimr'
    for item in cfg:
        if item[0] == 'providers':
            providers = [i.strip() for i in ''.join(item[1:]).split(',')]
    return providers


def is_os_supported(cfg):
    for item in cfg:
        if item[0] == 'os':
            return any(sys.platform.startswith(_os) for _os in item[1].split(','))
    return True


def execute(
        klass=None,
        program=None,
        program_file=None,
        files=None,
        urls=None,
        run_sql=False,
        verbose=False,
        check_error=True,
        input_tables=None,
        output_tables=None,
):
    '''
    Executes YQL/SQL

    :param klass: KiKiMRForYQL if  instance (default: YQLRun)
    :param program: string with YQL or SQL program
    :param program_file: file with YQL or SQL program (optional, if :param program: is None)
    :param files: dict like {'name': '/path'} with extra files
    :param urls: dict like {'name': utr} with extra files utls
    :param run_sql: execute sql instead of yql
    :param verbose: log all results and diagnostics
    :param check_error: fail on non-zero exit code
    :param input_tables: list of Table (will be written if not exist)
    :param output_tables: list of Table (will be returned)
    :return: YQLExecResult
    '''

    if input_tables is None:
        input_tables = []
    else:
        assert isinstance(input_tables, list)
    if output_tables is None:
        output_tables = []

    klass.write_tables(input_tables + output_tables)

    res = klass.yql_exec(
        program=program,
        program_file=program_file,
        files=files,
        urls=urls,
        run_sql=run_sql,
        verbose=verbose,
        check_error=check_error,
        tables=(output_tables + input_tables)
    )

    return res, klass.get_tables(output_tables)


execute_sql = partial(execute, run_sql=True)


def log(s):
    logger.debug(s)


@pytest.fixture(scope='module')
def tmpdir_module(request):
    return tempfile.mkdtemp(prefix='kikimr_test_')


def escape_backslash(s):
    return s.replace('\\', '\\\\')


default_mount_point_config_content = '''
MountPoints {
    RootAlias: '/examples'
    MountPoint: '%s'
}
MountPoints {
    RootAlias: '/lib'
    MountPoint: '%s'
    Library: true
}
''' % (
    escape_backslash(yql_source_path('yql/mount/examples')),
    escape_backslash(yql_source_path('yql/mount/lib')),
)


def get_mount_config_file(content=None):
    config = yql_output_path('mount.cfg')
    if not os.path.exists(config):
        with open(config, 'w') as f:
            f.write(content or default_mount_point_config_content)
    return config


def run_command(program, cmd, tmpdir_module=None, stdin=None,
                async=False, check_exit_code=True, env=None, stdout=None):
    if tmpdir_module is None:
        tmpdir_module = tempfile.mkdtemp()

    stdin_stream = None
    if isinstance(stdin, basestring):
        with tempfile.NamedTemporaryFile(
            prefix='stdin_',
            dir=tmpdir_module,
            delete=False
        ) as stdin_file:
            stdin_file.write(stdin)
        stdin_stream = open(stdin_file.name)
    elif isinstance(stdin, file):
        stdin_stream = stdin
    elif stdin is not None:
        assert 0, 'Strange stdin ' + repr(stdin)

    if isinstance(cmd, basestring):
        cmd = cmd.split()
    else:
        cmd = [str(c) for c in cmd]
    log(' '.join('\'%s\'' % c if ' ' in c else c for c in cmd))
    cmd = [program] + cmd

    if async:
        stderr_stream = subprocess.PIPE
        stdout_stream = subprocess.PIPE
    else:
        stderr_stream = None
        stdout_stream = None

    if stdout:
        stdout_stream = stdout

    res = yatest.common.execute(
        cmd,
        cwd=tmpdir_module,
        stdin=stdin_stream,
        stdout=stdout_stream,
        stderr=stderr_stream,
        check_exit_code=check_exit_code,
        env=env,
        wait=(not async)
    )
    if not async:
        if res.std_err:
            log(res.std_err)
        if res.std_out:
            log(res.std_out)
    return res


def yson_to_csv(yson_content, columns=None, with_header=True, strict=False):
    import yt.yson
    if columns:
        headers = sorted(columns)
    else:
        headers = set()
        for item in yt.yson.loads(yson_content, yson_type='list_fragment'):
            headers.update(item.iterkeys())
        headers = sorted(headers)
    csv_content = []
    if with_header:
        csv_content.append(';'.join(headers))
    for item in yt.yson.loads(yson_content, yson_type='list_fragment'):
        if strict and sorted(item.iterkeys()) != headers:
            return None
        csv_content.append(';'.join([str(item[h]).encode('string_escape') if h in item else '' for h in headers]))
    return '\n'.join(csv_content)


def get_udfs_path():
    udfs_build_path = yatest.common.build_path('yql/udfs')
    robot_udfs_build_path = yatest.common.build_path('robot/rthub/yql/udfs')

    try:
        udfs_bin_path = yatest.common.binary_path('yql/udfs')
    except Exception:
        udfs_bin_path = None

    try:
        udfs_project_path = yql_binary_path('yql/ci/udfs_deps/yql/udfs')
    except Exception:
        udfs_project_path = None

    merged_udfs_path = yql_output_path('yql_udfs')
    if not os.path.isdir(merged_udfs_path):
        os.mkdir(merged_udfs_path)

    log('process search UDF in: %s, %s, %s' % (udfs_project_path, udfs_bin_path, udfs_build_path))
    for _udfs_path in udfs_project_path, udfs_bin_path, udfs_build_path, robot_udfs_build_path:
        if _udfs_path:
            for dirpath, dnames, fnames in os.walk(_udfs_path):
                for f in fnames:
                    if f.endswith('.so'):
                        f = os.path.join(dirpath, f)
                        if not os.path.exists(f) and os.path.lexists(f):  # seems like broken symlink
                            try:
                                os.unlink(f)
                            except OSError:
                                pass
                        link_name = os.path.join(merged_udfs_path, os.path.basename(f))
                        if not os.path.exists(link_name):
                            os.symlink(f, link_name)
                            log('Added UDF: ' + f)
    return merged_udfs_path


def get_test_prefix():
    return 'yql_tmp_' + hashlib.md5(yatest.common.context.test_name).hexdigest()
