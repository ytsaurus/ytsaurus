import yatest
import copy
import os
import tarfile

YT_ABI = '19_3'


class TestYtNode(object):
    @classmethod
    def setup_class(cls):
        cls.node_path = yatest.common.binary_path('yt/{0}/yt/nodejs/targets/bin/ytnode'.format(YT_ABI))
        cls.test_dir_path = yatest.common.source_path('yt/{0}/yt/nodejs/tests'.format(YT_ABI))
        sandbox_resource_dir = yatest.common.build_path('yt/{0}/yt/node_modules'.format(YT_ABI))
        with tarfile.open(os.path.join(sandbox_resource_dir, 'resource.tar.gz')) as tar:
            tar.extractall(path=sandbox_resource_dir)
        cls.node_modules = os.path.join(sandbox_resource_dir, 'node_modules')
        cls.mocha_path = os.path.join(cls.node_modules, '.bin/mocha')

    def prepare_cmd_line(self):
        tests = [
            os.path.join(self.test_dir_path, test_name)
            for test_name in os.listdir(self.test_dir_path)
            if test_name.startswith('test_') and test_name.endswith('.js')]
        return [
            self.node_path, self.mocha_path,
            '--reporter', 'spec',
            '--require', os.path.join(self.test_dir_path, 'common.js'),
            '--expose-gc'] + tests

    def prepare_env(self):
        test_env = copy.deepcopy(os.environ)
        test_env.update({
            'NODE_PATH': self.node_modules,
        })
        return test_env

    def test_ytnode(self):
        cmd_line = self.prepare_cmd_line()
        test_env = self.prepare_env()
        res = yatest.common.execute(cmd_line, env=test_env)
        assert not res.exit_code
