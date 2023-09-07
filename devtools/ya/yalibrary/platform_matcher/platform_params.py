import copy
import json
import logging

import test.const
from .matcher import stringize_platform

logger = logging.getLogger(__name__)


def stringize_toolchain(tc):
    platform = []
    platform.append(stringize_platform(tc['platform']['target']).lower())
    if tc.get('build_type'):
        platform.append(tc['build_type'])
    if tc.get('run_tests', False):
        platform.append('tests')
    if 'test_size_filters' in tc:
        for size in sorted(tc['test_size_filters']):
            platform.append('test-size={}'.format(size))
    if 'test_type_filters' in tc:
        test_type_filters = copy.copy(tc['test_type_filters'])
        regular_test_types = set(test.const.REGULAR_TEST_TYPES)
        if regular_test_types & set(test_type_filters) == regular_test_types:
            platform.append('regular-tests')
            for test_type in regular_test_types:
                test_type_filters.remove(test_type)
        for test_type in sorted(test_type_filters):
            platform.append('test-type={}'.format(test_type))
    flags = tc.get('flags', {})
    for k in sorted(flags):
        platform.append('{}={}'.format(k, flags[k]))

    return ','.join(platform)


def make_platform_params(pl):
    def _make_platform_param(p):
        if p in ('tests',) or p.startswith('test-size=') or p.startswith('test-type='):
            return ['--target-platform-{}'.format(p)]
        elif p == 'regular-tests':
            return ['--target-platform-regular-tests']
        elif p in ('release', 'debug', 'relwithdebinfo', 'minsizerel'):
            return ['--target-platform-build-type', p]
        else:
            return ['--target-platform-flag', p]

    tokens = pl.split(',')
    res = ['--target-platform', str(tokens[0])]
    for pp in tokens[1:]:
        res += _make_platform_param(pp)
    return res


def make_yamake_options(platforms=None, build_vars=None, host_platform_flags=None):
    result = []

    if build_vars:
        result += ['-D{var}'.format(var=var) for var in build_vars]
    if host_platform_flags:
        result += sum([['--host-platform-flag', var] for var in host_platform_flags], [])
    if platforms:
        result += sum([make_platform_params(pl) for pl in platforms], [])

    return result


def transform_toolchain(alias, target_platforms, toolchain_transforms):

    # XXX: remove after DEVTOOLS-6216

    platforms_params = {}
    for target_platform in target_platforms:
        platforms_params[target_platform] = make_platform_params(target_platform)

    for toolchain, name in toolchain_transforms.items():
        if name == alias:
            for target_platform, platform_params in platforms_params.items():
                platform_name = target_platform.split(',')[0]
                if toolchain.startswith(platform_name):

                    params_to_replace = {}

                    for param in platform_params:
                        if "-" in param and param != platform_name:
                            params_to_replace[param.replace("-", "_")] = param

                    for replace_param, orig_param in params_to_replace.items():
                        toolchain = toolchain.replace(orig_param, replace_param)

                    replaced_toolchain_params = make_platform_params(
                        platform_name + toolchain.replace(platform_name, "", 1).replace("-", ",")
                    )

                    toolchain_params = []

                    for param in replaced_toolchain_params:
                        if param in params_to_replace:
                            toolchain_params.append(params_to_replace[param])
                        else:
                            toolchain_params.append(param)

                    extra_params = set(toolchain_params).difference(set(platform_params))
                    missed_params = set(platform_params).difference(set(toolchain_params))

                    if '--target-platform-build-type' in extra_params and 'release' in extra_params and '--target-platform-build-type' not in platform_params:
                        extra_params.remove('--target-platform-build-type')
                        extra_params.remove('release')

                    if 'musl' in extra_params and 'MUSL=yes' in missed_params:
                        extra_params.remove('musl')
                        missed_params.remove('MUSL=yes')

                    if 'race' in extra_params and 'RACE=yes' in missed_params:
                        extra_params.remove('race')
                        missed_params.remove('RACE=yes')

                    if 'msan' in extra_params and 'SANITIZER_TYPE=memory' in missed_params:
                        extra_params.remove('msan')
                        missed_params.remove('SANITIZER_TYPE=memory')

                    if 'asan' in extra_params and 'SANITIZER_TYPE=address' in missed_params:
                        extra_params.remove('asan')
                        missed_params.remove('SANITIZER_TYPE=address')

                    if 'tsan' in extra_params and 'SANITIZER_TYPE=thread' in missed_params:
                        extra_params.remove('tsan')
                        missed_params.remove('SANITIZER_TYPE=thread')

                    for missed_param in missed_params.copy():
                        for extra_param in extra_params.copy():
                            if extra_param == missed_param + '=yes':
                                extra_params.remove(extra_param)
                                missed_params.remove(missed_param)

                    if '--target-platform-regular-tests' in missed_params:
                        missed_params.remove('--target-platform-regular-tests')

                    if 'FAKEID=sandboxing' in extra_params:
                        extra_params.remove('FAKEID=sandboxing')

                    if not extra_params and all(param.startswith('--target-platform-test') for param in missed_params):
                        logger.debug('Will use {} platform for {} alias'.format(target_platform, alias))
                        return platform_params


def get_target_platform_alias(toolchain_string, toolchain_transforms):
    toolchain_chunks = []
    for chunk in toolchain_string.split(','):
        if not chunk.startswith(('test', 'regular-tests')):
            toolchain_chunks.append(chunk)

    if toolchain_chunks[1] not in ('release', 'debug', 'relwithdebinfo', 'minsizerel'):
        toolchain_chunks.insert(1, 'release')

    for chunk in list(toolchain_chunks):
        if chunk.startswith('SANITIZER_TYPE'):
            _, v = chunk.split('=', 1)
            toolchain_chunks.remove(chunk)
            toolchain_chunks.append(v[0] + 'san')
        elif chunk.startswith('MUSL'):
            toolchain_chunks.remove(chunk)
            toolchain_chunks.append('musl')
        elif chunk.startswith('SANDBOXING'):
            toolchain_chunks.append('FAKEID=sandboxing')
        elif chunk.startswith('USE_FPGA'):
            toolchain_chunks.remove(chunk)
            toolchain_chunks.append('USE_FPGA=yes')
        elif chunk.startswith('TENSORFLOW_WITH_CUDA'):
            toolchain_chunks.remove(chunk)
            toolchain_chunks.append('TENSORFLOW_WITH_CUDA=yes')
        elif chunk.startswith('TIDY'):
            toolchain_chunks.remove(chunk)
            toolchain_chunks.append('TIDY=yes')

    toolchain_chunks = toolchain_chunks[: 2] + list(sorted(set(toolchain_chunks[2:])))
    alias_key = '-'.join(toolchain_chunks)
    logging.debug('Searching alias for toolchain: %s, and alias key: %s', toolchain_string, alias_key)
    alias = toolchain_transforms.get(alias_key)
    if alias:
        logging.debug('Found alias name: %s', alias)
    else:
        logging.debug('Cannot find alias in transforms_list: %s', json.dumps(toolchain_transforms, indent=4))
    return alias
