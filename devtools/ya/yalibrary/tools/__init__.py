# coding=utf-8
import copy
import json
import logging
import os
import six

import core.config
import yalibrary.fetcher
import yalibrary.platform_matcher as pm
from yalibrary.toolscache import toolscache_version
from exts import func
import exts.path2
import devtools.libs.yaplatform.python.platform_map as platform_map

logger = logging.getLogger(__name__)

_TOOLCHAIN_SEPARATOR = ','


class ToolNotFoundException(Exception):
    mute = True


class ToolResolveException(Exception):
    mute = True


class UnsupportedPlatform(Exception):
    mute = True


class UnsupportedToolchain(Exception):
    mute = True


class _Bottle(object):
    def __init__(self, toolchain_name, bottle_name, formula, executable, for_platform, force_refetch):
        self.__formula = formula
        self.__bottle_name = bottle_name
        self.__executable = executable
        self.__formula = formula
        if self.__executable and not isinstance(self.__executable, dict):
            binname = self.__executable
        else:
            binname = None
        self.__fetcher = yalibrary.fetcher.get_tool_chain_fetcher(core.config.tool_root(toolscache_version()), toolchain_name, bottle_name, self.__formula, for_platform, binname, force_refetch)

    def resolve(self, cache=True):
        return self.__fetcher.fetch_if_need(cache=cache).where

    def get_resource_id_from_cache(self):
        self.resolve()

        return self.__fetcher.resource_id_from_cache()

    def sid(self):
        return self.__formula.get("sandbox_id")

    def __getitem__(self, name):
        if not self.__executable:
            return self.resolve()
        if isinstance(self.__executable, dict):
            # это tar архив с потенциально несколькими бинарниками
            if name not in self.__executable:
                raise Exception('Cannot find ' + name)
            suffix = self.__executable[name]
            path = self.resolve()
            return exts.path2.normpath(os.path.join(path, *suffix))
        else:
            # этот файл - сам является бинарником, это не архив!
            path = self.resolve()
            return exts.path2.normpath(os.path.join(path, self.__executable))


class _Bottler(object):
    def get(self, toolchain_name, bottle_name, for_platform, force_refetch):
        visited = set()
        bottles = core.config.config()['bottles']
        while True:
            if bottle_name in visited:
                raise Exception('loop detected')
            visited.add(bottle_name)
            value = bottles[bottle_name]
            if isinstance(value, dict):
                return _Bottle(toolchain_name, bottle_name, value['formula'], value.get('executable'), for_platform, force_refetch)
            else:
                bottle_name = value


@func.lazy
def _bottler():
    return _Bottler()


def _bottle(toolchain_name, bottle_name, for_platform=None, force_refetch=False):
    return _bottler().get(toolchain_name, bottle_name, for_platform, force_refetch=force_refetch)


class _ToolInfo(object):
    def __init__(self, name, visible, description):
        self.name = name
        self.visible = visible
        self.description = description


def tools():
    for name, value in sorted(six.iteritems(core.config.config()['tools'])):
        yield _ToolInfo(name, value.get('visible', True), value.get('description'))


class _ToolChain(object):
    def __init__(self, extra_toolchain):
        extra_tc = [x for x in extra_toolchain.split(_TOOLCHAIN_SEPARATOR) if x] if extra_toolchain else []
        native_with_target_tc = []
        native_tc = []
        other_tc = []
        for tc, value in six.iteritems(core.config.config()['toolchain']):
            for pl in value['platforms']:
                if pl.get('default', False):
                    current_os = yalibrary.platform_matcher.current_platform()['os']
                    if pl['host']['os'] == current_os:
                        if pl.get('target', {}).get('os', current_os) == current_os:
                            native_with_target_tc.append(tc)
                        else:
                            native_tc.append(tc)
                    else:
                        other_tc.append(tc)
        self.order = extra_tc + native_with_target_tc + native_tc + [tc for tc in other_tc if tc not in native_tc]

    def __find_toolchain(self, tool_name):
        logger.debug('Using old-style toolchain for: %s', tool_name)
        for tc in self.order:
            if tc in core.config.config()['toolchain']:
                toolchain = core.config.config()['toolchain'][tc]
                if tool_name in toolchain['tools']:
                    location = toolchain['tools'][tool_name]
                    if 'system' in location and location['system']:
                        return tc, toolchain, location
                    elif 'bottle' in location:
                        return tc, toolchain, location
                    else:
                        raise ToolNotFoundException('Bottle not defined for tool: ' + tool_name)
            else:
                raise ToolNotFoundException('Cannot find toolchain: ' + tc)
        raise ToolNotFoundException('Cannot find tool: ' + tool_name)

    def find(self, tool_name, with_params=False, for_platform=None, cache=True, force_refetch=False):
        tc, toolchain, location = self.__find_toolchain(tool_name)
        if 'system' in location and location['system']:
            executable = location.get('executable')
            tool_params = copy.deepcopy(toolchain.get('params', {}))
            if not with_params:
                return executable
            else:
                return executable, tool_params

        executable_name = location.get('executable')
        cur_bottle = _bottle(tc, location['bottle'], for_platform, force_refetch=force_refetch)
        if not cache:
            cur_bottle.resolve(cache=cache)
        executable = cur_bottle[executable_name]  # if executable_name is None it's Ok
        if not with_params:
            return executable
        else:
            tool_params = copy.deepcopy(toolchain.get('params', {}))
            tool_params['toolchain_root_path'] = exts.path2.normpath(cur_bottle.resolve())
            if executable_name is not None:
                tool_params['toolchain_name'] = executable_name.upper()
            tool_params['toolchain'] = tc
            return executable, tool_params

    def task_id(self, tool_name):
        tc, toolchain, location = self.__find_toolchain(tool_name)
        return _bottle(tc, location['bottle']).sid()

    def resource_id_from_cache(self, tool_name, for_platform):
        tc, toolchain, location = self.__find_toolchain(tool_name)
        return str(_bottle(tc, location['bottle'], for_platform).get_resource_id_from_cache())

    def param(self, tool_name, param):
        params = self.__find_toolchain(tool_name)[1].get('params', {})
        return params.get(param)

    def system_libs(self, tool_name, for_platform):
        platf = for_platform.upper() if for_platform else pm.current_platform()['os']

        params = self.__find_toolchain(tool_name)[1].get('params', {})
        sys_libs = params.get('sys_lib', {}).get(platf, [])
        tool_root = params.get('match_root')
        resolved_sys_libs = []
        if tool_root is not None:
            tool_name_var = "$({0})".format(tool_root.upper())
            resolved_sys_libs = [path.replace(tool_name_var, self.toolchain_root(tool_name, for_platform)) for path in sys_libs]
        return ' '.join(resolved_sys_libs)

    def environ(self, tool_name):
        tc, toolchain, location = self.__find_toolchain(tool_name)
        params = toolchain.get('params', {})
        environ = toolchain.get('env', {})
        if not environ:
            return {}
        cur_bottle = _bottle(tc, location['bottle'])
        if cur_bottle:
            path = exts.path2.normpath(cur_bottle.resolve())
            executable_name = params.get('match_root')
            if executable_name is not None:
                tool_name_var = "$({0})".format(executable_name.upper())
                for var in environ.keys():
                    environ[var] = [x.replace(tool_name_var, path) for x in environ[var]]
        return environ

    def toolchain_root(self, tool_name, for_platform):
        tc, toolchain, location = self.__find_toolchain(tool_name)
        return _bottle(tc, location['bottle'], for_platform).resolve()


def tool(name, toolchain_extra=None, with_params=False, for_platform=None, target_platform=None, cache=True, force_refetch=False):
    if target_platform:
        if toolchain_extra:
            raise ToolResolveException("toolchain and target platform should not be specified together")
        toolchain_extra = resolve_tool_by_host_os(name, pm.current_os(), target_platform)['name']
    toolchain = _ToolChain(toolchain_extra)
    return toolchain.find(name, with_params, for_platform, cache=cache, force_refetch=force_refetch)


def param(name, toolchain_extra, param):
    return _ToolChain(toolchain_extra).param(name, param)


def environ(name, toolchain_extra=None):
    return _ToolChain(toolchain_extra).environ(name)


def resource_id(name, toolchain_extra, for_platform):
    return _ToolChain(toolchain_extra).resource_id_from_cache(name, for_platform)


def task_id(name, toolchain_extra=None):
    return _ToolChain(toolchain_extra).task_id(name)


def toolchain_root(name, toolchain_extra, for_platform):
    return _ToolChain(toolchain_extra).toolchain_root(name, for_platform)
    # return ToolChain(toolchain_extra).toolchain_root(name, for_platform)


def toolchain_sys_libs(name, toolchain_extra, for_platform):
    return _ToolChain(toolchain_extra).system_libs(name, for_platform)


def iter_tools(name, tn_filter=None):
    tc = core.config.config()['toolchain']
    bt = core.config.config()['bottles']

    for tn, descr in tc.items():
        if tn_filter is not None and not tn_filter(tn, descr):
            continue

        trn = tn
        tn = descr.get('name', tn)
        tools = descr.get('tools', {})

        if name in tools:
            tool = tools[name]
            bottle_name = tool.get('bottle', None)

            formula = bt[bottle_name]['formula'] if bottle_name else None

            def iter_platforms():
                for pl in descr.get('platforms', []):

                    def load_toolchain(platf_type, default_value=None):
                        platform = pl.get(platf_type, None)
                        if not platform:
                            if default_value:
                                return copy.deepcopy(default_value)
                            else:
                                raise UnsupportedToolchain('%s platform should be always specified. %s', platf_type, pl)
                        else:
                            res_os = platform.get('os', None)
                            if not res_os:
                                raise UnsupportedToolchain('OS should be defined. %s', platform)
                            return {'os': res_os, 'arch': platform.get('arch', 'x86_64'), 'toolchain': tn, 'visible_name': tn}

                    host = load_toolchain('host')
                    target = load_toolchain('target', host)

                    yield {'host': host, 'target': target}

                    is_default = pl.get('default', False)

                    if is_default:
                        h_copy = copy.deepcopy(host)
                        t_copy = copy.deepcopy(target)

                        h_copy['toolchain'] = 'default'
                        t_copy['toolchain'] = 'default'
                        yield {'host': h_copy, 'target': t_copy}

            for p in iter_platforms():
                pp = descr.get('params', {})

                if 'sys_lib' in pp:
                    pp = pp.copy()
                    pp['sys_lib'] = pp['sys_lib'].get(p['target']['os'], [])

                res = {'platform': p, 'env': descr.get('env', {}), 'params': pp, 'formula': formula, 'name': trn, 'bottle_name': bottle_name}
                root = res.get('params', {}).get('match_root', None)

                if root:
                    if formula and res.get('params', {}).get('use_bundle', False):
                        formula = yalibrary.fetcher.tool_chain_fetcher.get_formula_value(formula)
                        ts = six.ensure_str(platform_map.mapping_var_name_from_json(root, json.dumps(formula)))
                    else:
                        ts = pm.stringize_platform(p['target'], sep='_')

                    def subst(x):
                        if isinstance(x, dict):
                            return dict((subst(k), subst(v)) for k, v in x.items())

                        if isinstance(x, list):

                            return [subst(v) for v in x]
                        if isinstance(x, six.string_types):
                            if x == root:
                                return ts

                            return x.replace('$(' + root + ')', '$(' + ts + ')')

                        return x

                    for key in ('env', 'params'):
                        res[key] = subst(res[key])

                yield res


def get_tool(name, toolchain_key):
    for tool in iter_tools(name, lambda key, descr: key == toolchain_key):
        return tool

    raise UnsupportedToolchain('No toolchain found: %s' % toolchain_key)


def get_tool_for_ide(name, ide):
    for tool in iter_tools(name, lambda key, descr: descr.get('params', {}).get('for_ide') == ide):
        return tool

    raise UnsupportedToolchain('No toolchain found for ide: %s' % ide)


def resolve_tool(name, host, target):

    def filter_host(tool_name, tool_host):
        avail = set()
        ok = False

        for tool in iter_tools(tool_name):
            host_str = pm.stringize_platform(tool['platform']['host'])
            avail.add(host_str)
            if host_str == tool_host:
                ok = True
                yield tool

        if not ok:
            raise UnsupportedPlatform('Unsupported host platforms %s for tool %s, use one of %s' % (tool_host, tool_name, ', '.join(sorted(avail))))

    return _resolve_tool(name, host, target, filter_host)


def resolve_tool_by_host_os(name, host_os, target):

    def filter_host(tool_name, tool_host_os):
        avail = set()
        ok = False

        for tool in iter_tools(tool_name):
            # print tool['platform']['target']
            host_os_str = tool['platform']['host']['os']
            avail.add(host_os_str)
            if host_os_str == tool_host_os:
                ok = True
                yield tool

        if not ok:
            raise UnsupportedPlatform('Unsupported host os %s for tool %s, use one of %s' % (tool_host_os, tool_name, ', '.join(sorted(avail))))

    return _resolve_tool(name, host_os, target, filter_host)


def _resolve_tool(name, host, target, filter_host):

    def filter_target():
        avail = set()
        ok = False

        for tool in filter_host(name, host):
            target_str = pm.stringize_platform(tool['platform']['target'])
            avail.add(target_str)

            if target_str == target:
                ok = True

                yield tool

        if not ok:
            raise UnsupportedPlatform('Unsupported target platform %s for tool %s, use one of %s' % (target, name, ', '.join(sorted(avail))))

    for t in filter_target():
        return t
