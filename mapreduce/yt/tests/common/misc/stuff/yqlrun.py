import os
import pytest
import shutil

import yatest.common

import yql_utils


class YQLRun(object):

    def __init__(self, udfs_dir=None, prov='yamr', use_sql2yql=True):
        self.yqlrun_binary = yql_utils.yql_binary_path('yql/tools/yqlrun/yqlrun')

        try:
            self.sql2yql_binary = yql_utils.yql_binary_path('yql/tools/sql2yql/sql2yql')
        except BaseException:
            self.sql2yql_binary = None

        try:
            self.udf_resolver_binary = yql_utils.yql_binary_path('yql/tools/udf_resolver/udf_resolver')
        except Exception:
            self.udf_resolver_binary = None

        if udfs_dir is None:
            self.udfs_path = yql_utils.get_udfs_path()
        else:
            self.udfs_path = udfs_dir
        res_dir = yql_utils.get_yql_dir(prefix='yqlrun_')
        self.res_dir = res_dir
        self.tables = {}
        self.prov = prov
        self.use_sql2yql = use_sql2yql

    def yql_exec(self, program=None, program_file=None, files=None, urls=None,
                 run_sql=False, verbose=False, check_error=True, tables=None):
        res_dir = self.res_dir

        def res_file_path(name): return os.path.join(res_dir, name)
        opt_file = res_file_path('opt.yql')
        results_file = res_file_path('results.txt')
        plan_file = res_file_path('plan.txt')

        udfs_dir = self.udfs_path
        prov = self.prov

        program, program_file = yql_utils.prepare_program(program, program_file, res_dir)

        if run_sql and self.use_sql2yql:
            orig_sql = program_file + '.orig_sql'
            shutil.copy2(program_file, orig_sql)
            cmd = ' '.join([
                self.sql2yql_binary,
                orig_sql,
                '--yql',
                '--output=' + program_file
            ])
            yatest.common.process.execute(cmd.split(), cwd=res_dir)

        with open(program_file) as f:
            yql_program = f.read()
        if self.prov == 'yt':
            yql_program = yql_program.replace('yamr', 'yt')
        elif self.prov == 'yamr':
            pass  # TODO yql_program = yql_program.replace('yt', 'yamr')
        with open(program_file, 'w') as f:
            f.write(yql_program)

        cmd = self.yqlrun_binary + ' ' \
            '--run ' \
            '-L ' \
            '--program=%(program_file)s ' \
            '--expr-file=%(opt_file)s ' \
            '--result-file=%(results_file)s ' \
            '--plan-file=%(plan_file)s ' \
            '--udfs-dir=%(udfs_dir)s ' \
            '--keep-temp ' \
            '--gateways=%(prov)s ' \
            '--tmp-dir=%(res_dir)s ' % locals()
        cmd += '--mounts=' + yql_utils.get_mount_config_file() + ' '
        if files:
            for f in files:
                if not files[f].startswith('/'):
                    path_to_copy = os.path.join(
                        yatest.common.work_path(),
                        files[f]
                    )
                    shutil.copy2(path_to_copy, res_dir)
                else:
                    shutil.copy2(files[f], res_dir)
                files[f] = os.path.basename(files[f])
            cmd += yql_utils.get_cmd_for_files('--file', files)

        if tables:
            for table in tables:
                self.tables[table.full_name] = table
            for name in self.tables:
                cmd += '--table=%s.%s@%s ' % (self.prov, name, self.tables[name].yqlrun_file)

        if yql_utils.get_param('UDF_RESOLVER'):
            cmd += '--udf-resolver=' + self.udf_resolver_binary + ' '

        if run_sql and not self.use_sql2yql:
            cmd += '--sql '

            yql_utils.log(cmd)
        if verbose:
            yql_utils.log('prov is ' + self.prov)
        proc_result = yatest.common.process.execute(cmd.strip().split(), check_exit_code=False, cwd=res_dir)
        if proc_result.exit_code != 0 and check_error:
            assert 0, \
                'Command\n%(command)s\n finished with exit code %(code)d, stderr:\n\n%(err)s' % {
                    'command': cmd,
                    'code': proc_result.exit_code,
                    'err': proc_result.std_err,
                }

        if os.path.exists(results_file) and os.stat(results_file).st_size == 0:
            os.unlink(results_file)  # kikimr yql-exec compatibility

        results, log_results = yql_utils.read_res_file(results_file)
        plan, log_plan = yql_utils.read_res_file(plan_file)
        opt, log_opt = yql_utils.read_res_file(opt_file)

        if verbose:
            yql_utils.log('PROGRAM:')
            yql_utils.log(program)
            yql_utils.log('OPT:')
            yql_utils.log(log_opt)
            yql_utils.log('PLAN:')
            yql_utils.log(log_plan)
            yql_utils.log('RESULTS:')
            yql_utils.log(log_results)

        return yql_utils.YQLExecResult(
            proc_result.std_out,
            proc_result.std_err,
            results,
            results_file,
            opt,
            opt_file,
            plan,
            plan_file,
            program,
            proc_result
        )

    def write_tables(self, tables):
        for table in tables:
            if table.format != 'yson':
                pytest.skip('yqlrun can not work with table in %s format' % table.format)

    def get_tables(self, tables):
        res = {}
        for table in tables:
            # recreate table after yql program was executed
            res[table.full_name] = yql_utils.new_table(
                table.full_name,
                yqlrun_file=self.tables[table.full_name].yqlrun_file,
                res_dir=self.res_dir
            )

            yql_utils.log('YQLRun table ' + table.full_name)
            yql_utils.log(res[table.full_name].content)

        return res
