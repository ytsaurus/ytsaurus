import logging
import os
import requests
import six
import sys
import time
from collections import namedtuple
from copy import deepcopy

import exts.yjson as json
from exts import fs, func
from yalibrary import guards
from yalibrary import platform_matcher
from yalibrary import toolscache

from core import config

from .common import (
    BINARY,
    parse_resource_uri,
    stringify_memoize,
    ProgressPrinter,
)
from .cache_helper import (
    install_symlink,
    installed,
    safe_resource_lock,
    update_resource,
)
from .resource_fetcher import (
    fetch_resource_if_need,
)


logger = logging.getLogger(__name__)


_CACHE_FILE_NAME = ".platform.cache"
_SB_REST_API_TIMEOUT = 30


class ResourceNotFound(RuntimeError):
    mute = True


@stringify_memoize
def get_tool_chain_fetcher(root, toolchain_name, bottle_name, formula, for_platform=None, binname=None, force_refetch=False):
    if for_platform is None:
        for_platform = platform_matcher.my_platform()
    formula = get_formula_value(formula)
    platform_replacements = formula.get("platform_replacements")
    if "by_platform" in formula:
        return _ToolChainByPlatformFetcher(root, toolchain_name, platform_replacements, formula["by_platform"], for_platform, binname, force_refetch)
    elif "latest_matched" in formula:
        return _ToolChainLatestMatchedResourceFetcher(root, toolchain_name, platform_replacements, formula["latest_matched"], for_platform, binname, force_refetch, bottle_name)
    elif "sandbox_id" in formula:
        return _ToolChainSandboxFetcher(root, toolchain_name, platform_replacements, formula['match'], formula["sandbox_id"], for_platform, binname, force_refetch)
    else:
        raise Exception("Unsupported formula: {}".format(formula))


def resolve_resource_id(root, toolchain_name, bottle_name, formula, platform):
    return get_tool_chain_fetcher(root, toolchain_name, bottle_name, formula, platform).resource_id_from_cache()


def get_formula_value(formula):
    '''Return the value directly or read from a file if the value is a file name'''
    if isinstance(formula, six.string_types):
        return config.config_from_arc_rel_path(formula)
    else:
        return formula


_ToolChainInfo = namedtuple('_ToolChainInfo', 'where')


class _ToolChainFetcherBase(object):
    def fetch_if_need(self):
        raise NotImplementedError()

    def resource_id_from_cache(self):
        raise NotImplementedError()


class _ToolChainFetcherImplBase(_ToolChainFetcherBase):
    def __init__(self, root, toolchain_name, platform_replacements, for_platform, binname, force_refetch):
        self._root = root
        self._where = None
        self._toolchain_name = toolchain_name
        self._platform_replacements = platform_replacements
        self._platform = for_platform
        self._binname = binname
        self._binname_kwargs = dict(install_params=(BINARY, True), binname=binname) if binname else {}
        self._force_refetch = force_refetch

    def resource_id_from_cache(self):
        resource = self._get_matched_resource()
        return self._get_resource_uri(resource)

    @stringify_memoize(cache_kwarg='cache')
    def fetch_if_need(self):
        if self._force_refetch or not self._check():
            logger.debug("{0}: try to fetch by {1} for '{2}'".format(self._toolchain_name, self._details(), self._platform))
            self._where = self._fetch()
            logger.debug("{0}: successfully fetched into {1}".format(self._toolchain_name, self._where))
        return _ToolChainInfo(self._where)

    def _check(self):
        if self._where is not None:
            toolscache.notify_tool_cache(self._where)
            return installed(self._where)
        return False

    def _get_matched_resource(self):
        by_platform = self._by_platform
        best_match = platform_matcher.match_platform(self._platform, by_platform, self._platform_replacements)
        if best_match is not None:
            logger.debug("{0}: will use '{1}' platform".format(self._toolchain_name, best_match))
            return by_platform[best_match]
        else:
            raise platform_matcher.PlatformNotSupportedException('{}: platform ({}) not matched to any of {}'.format(self._toolchain_name, self._platform, by_platform.keys()))

    def _resource_path(self, resource_id):
        return os.path.join(self._root, str(resource_id))

    def _fetch(self):
        resource = self._get_matched_resource()
        return fetch_resource_if_need(None, self._root, self._get_resource_uri(resource), force_refetch=self._force_refetch, **self._binname_kwargs)

    def _details(self):
        raise NotImplementedError()

    @func.lazy_property
    def _by_platform(self):
        return self._make_by_platform()

    def _make_by_platform(self):
        raise NotImplementedError()


class _ToolChainSandboxFetcher(_ToolChainFetcherImplBase):
    def __init__(self, root, name, platform_replacements, match, sid, for_platform, binname, force_refetch):
        super(_ToolChainSandboxFetcher, self).__init__(root, name, platform_replacements, for_platform, binname, force_refetch)
        self.__task_ids = _to_list(sid)
        self.__match = match.lower()
        self.__platform_cache_needs_refreshing = False

    def resource_id_from_cache(self):
        resource = self._get_matched_resource()
        self._install_cache(resource)
        self._install_symlink(resource, platform_cache_only=True)

        return self._get_resource_uri(resource)

    def _load_by_platform_from_platform_cache(self):
        logger.debug('{}: lookup platform mapping in tools cache'.format(self._toolchain_name))
        task_id_set = set(self.__task_ids)
        for task_id in self.__task_ids:
            cache_path = os.path.join(self._resource_path(task_id), _CACHE_FILE_NAME)
            if os.path.exists(cache_path):
                self.__platform_cache_needs_refreshing = True
                with open(cache_path) as platform_cache:
                    try:
                        by_platform = json.load(platform_cache)
                    except ValueError:
                        logger.exception('Cannot parse {}'.format(cache_path))
                        continue
                    found_task_ids = set((r['task']['id'] for r in by_platform.values()))
                    # Is by platform cache contains all required task_ids
                    if found_task_ids >= task_id_set:
                        self.__platform_cache_needs_refreshing = False
                        logger.debug('{}: platform mapping is loaded from {}'.format(self._toolchain_name, cache_path))
                        return {k: r for k, r in by_platform.items() if r['task']['id'] in task_id_set}
        logger.debug('{}: platform mapping is not found in tools cache for {}'.format(self._toolchain_name, self._details()))
        return None

    def _load_by_platform_from_sandbox(self):
        logger.debug('{}: load platform mapping from sandbox for {}'.format(self._toolchain_name, self._details()))
        resources = _list_all_resources(state='READY', task_id=self.__task_ids)
        by_platform = {}
        for resource in resources:
            if resource['type'] in ['TASK_LOGS', 'PLATFORM_MAPPING', 'BUILD_LOGS']:
                continue
            if self._match_description(resource):
                attrs = resource.get('attributes', {})
                platform = None
                if 'platform' in attrs:
                    platform = attrs['platform']
                else:
                    platform = 'any'
                by_platform[platform] = resource
        return by_platform

    def _make_by_platform(self):
        if config.has_mapping():
            return self._make_by_platform_from_mapping_config()

        by_platform = self._load_by_platform_from_platform_cache()
        if by_platform:
            # Legacy platform cache may contain not filtered resources. Filter them again
            by_platform = {p: r for p, r in by_platform.items() if self._match_description(r)}
        else:
            by_platform = self._load_by_platform_from_sandbox()
        dump_file = os.environ.get("YA_DUMP_RESOURCES_FILE")
        if dump_file:
            _dump_resources_file(dump_file, by_platform.values())
        return by_platform

    def _make_by_platform_from_mapping_config(self):
        by_platform = {}
        mapping_tasks = config.mapping()["tasks"]
        for task_id in self.__task_ids:
            task_id = str(task_id)
            if task_id in mapping_tasks:
                for platform in mapping_tasks[task_id]:
                    by_platform[platform] = {
                        'id': mapping_tasks[task_id][platform],
                        'task': {
                            'id': task_id,
                        }
                    }
        return by_platform

    def _match_description(self, resource):
        def filter(x):
            return self.__match in x.lower()

        return filter(resource['description']) or filter(resource.get('attributes', {}).get('description', ''))

    def _get_resource_uri(self, resource):
        return "sbr:{}".format(resource["id"])

    def _get_resource_id(self, resource):
        return resource['id']

    def _fetch(self):
        resource = self._get_matched_resource()
        fetcher, progress_callback = _get_fetcher(self._toolchain_name)
        where = fetch_resource_if_need(fetcher, self._root, self._get_resource_uri(resource), progress_callback, force_refetch=self._force_refetch, **self._binname_kwargs)
        self._install_cache(resource)
        self._install_symlink(resource)
        return where

    def _details(self):
        return "{} task-id".format(self.__task_ids)

    def _install_cache(self, resource):
        """
        Tools downloaded during local execution may lack CACHE_FILE_NAME
        """
        resource_path = self._resource_path(self._get_resource_id(resource))
        cache_path = os.path.join(resource_path, _CACHE_FILE_NAME)
        if not os.path.exists(cache_path) or self.__platform_cache_needs_refreshing:
            update_resource(resource_path, lambda: self._write_platform_cache(resource_path))

    def _install_symlink(self, resource, platform_cache_only=False):
        resource_id = self._get_resource_id(resource)
        resource_path = self._resource_path(resource_id)
        link_path = self._resource_path(resource["task"]["id"])
        # Permit some stale symlinks and directories with platform cache only
        # to avoid polling in tool cache.
        if not platform_cache_only:
            toolscache.notify_tool_cache(link_path)
        return install_symlink(resource_path, link_path)

    def _write_platform_cache(self, resource_path):
        by_platform = self._by_platform
        logger.debug('{}: install {} to {}'.format(self._toolchain_name, _CACHE_FILE_NAME, resource_path))
        temp_cache = os.path.join(resource_path, _CACHE_FILE_NAME + '.tmp')
        cache_file = os.path.join(resource_path, _CACHE_FILE_NAME)
        with open(temp_cache, mode="w") as f:
            json.dump(by_platform, f, indent=4)
        fs.replace(temp_cache, cache_file)


class _ToolChainByPlatformFetcher(_ToolChainFetcherImplBase):
    def __init__(self, root, name, platform_replacements, by_platform, for_platform, binname, force_refetch):
        super(_ToolChainByPlatformFetcher, self).__init__(root, name, platform_replacements, for_platform, binname, force_refetch)
        self.__by_platform = by_platform

    # Dumped once
    @stringify_memoize
    def _dump_resources_to_file(self, dump_file):
        platform_by_resource_id = {}
        for platform, resource_desc in self.__by_platform.items():
            parsed_uri = parse_resource_uri(self._get_resource_uri(resource_desc))
            if parsed_uri.resource_type == 'sbr':
                platform_by_resource_id[int(parsed_uri.resource_id)] = platform
        if platform_by_resource_id:
            resources = _list_all_resources(id=list(platform_by_resource_id.keys()))
            for resource in resources:
                attrs = resource['attributes']
                # Add platform attribute if it's missing
                if 'platform' not in attrs:
                    attrs['platform'] = platform_by_resource_id[resource['id']]
            _dump_resources_file(dump_file, resources)

    def _make_by_platform(self):
        dump_file = os.environ.get('YA_DUMP_RESOURCES_FILE')
        if dump_file:
            self._dump_resources_to_file(dump_file)
        return self.__by_platform

    def _get_resource_uri(self, resource_desc):
        return resource_desc['uri']

    def _details(self):
        return self.__by_platform

    def _fetch(self):
        resource_desc = self._get_matched_resource()
        fetcher, progress_callback = _get_fetcher(self._toolchain_name)
        where = fetch_resource_if_need(fetcher, self._root, self._get_resource_uri(resource_desc), progress_callback, force_refetch=self._force_refetch, **self._binname_kwargs)
        return where


class _ToolChainLatestMatchedResourceFetcher(_ToolChainFetcherImplBase):
    _DEFAULT_UPDATE_INTERVAL = 24 * 3600
    _DISABLE_AUTO_UPDATE_FILE_NAME = '.disable.auto.update'
    _updated_toolchains = set()

    def __init__(self, root, toolchain_name, platform_replacements, params, for_platform, binname, force_refetch, bottle_name):
        super(_ToolChainLatestMatchedResourceFetcher, self).__init__(root, toolchain_name, platform_replacements, for_platform, binname, force_refetch)
        self.__params = params
        self.__bottle_name = bottle_name
        self.__update_interval = params.get('update_interval', self._DEFAULT_UPDATE_INTERVAL)
        self.__force_update = bool(os.environ.get('YA_TOOL_FORCE_UPDATE'))
        if params.get('ignore_platform', False):
            self.__platforms = []
        else:
            platforms = [for_platform] + platform_matcher.get_platform_replacements(for_platform, self._platform_replacements)
            self.__platforms = self._add_default_arch(platforms)

    def _add_default_arch(self, platforms):
        default_arch = 'x86_64'
        result = []
        for p in platforms:
            if '-' not in p:
                result += [p, '{}-{}'.format(p, default_arch)]
            else:
                result.append(p)
        return result

    def resource_id_from_cache(self):
        resource_id = self._get_matched_resource_id()
        return 'sbr:{}'.format(resource_id)

    def _get_symlink_path(self):
        return os.path.join(self._root, '-'.join([self.__bottle_name, self._platform]))

    def _get_info_path(self):
        return self._get_symlink_path() + '.info'

    def _write_info(self, info):
        info_path = self._get_info_path()
        tmp = info_path + '.tmp'
        with open(tmp, 'w') as f:
            json.dump(info, f)
        fs.replace(tmp, info_path)
        logger.debug('{}: write info "{}" to {}'.format(self._toolchain_name, info, info_path))

    def _read_info(self):
        info_path = self._get_info_path()
        with open(info_path) as f:
            info = json.load(f)
        logger.debug('{}: info "{}" was read from {}'.format(self._toolchain_name, info, info_path))
        return info

    def _get_matched_resource_id(self):
        if config.has_mapping():
            return self._get_resource_id_from_mapping_config()

        resource_id = self._get_resource_id_from_info_file()
        if resource_id is None:
            link_path = self._get_symlink_path()
            # Use symlink path for lock file as install_symlink() does
            with safe_resource_lock(link_path):
                resource = self._find_resource()
                resource_id = resource['id']
                info = {
                    'update_time': int(time.time()),
                    'resource_id': resource_id,
                }
                self._write_info(info)
        return resource_id

    def _get_resource_id_from_mapping_config(self):
        mapping = config.mapping()
        bottle = mapping.get('bottles')
        if bottle is None:
            raise Exception("'bottles' section is missing in mapping config file")
        platforms = self.__platforms or ['any']
        for platform in platforms:
            if platform in bottle:
                return bottle[platform]
        raise ResourceNotFound('{}: no resource is found in bottle mapping: {}'.format(self._toolchain_name, bottle))

    def _get_resource_id_from_info_file(self):
        info_path = self._get_info_path()
        update_time_threshold = time.time() - self.__update_interval
        if os.path.exists(info_path):
            info = self._read_info()
            update_time = info['update_time']
            if self.__force_update and self._toolchain_name not in _ToolChainLatestMatchedResourceFetcher._updated_toolchains:
                logger.debug('{}: force update'.format(self._toolchain_name))
                _ToolChainLatestMatchedResourceFetcher._updated_toolchains.add(self._toolchain_name)
            elif update_time > update_time_threshold or self.__is_auto_update_disabled:
                resource_id = info['resource_id']
                logger.debug('{}: use cached toolchain with resource id: {}'.format(self._toolchain_name, resource_id))
                return resource_id
            else:
                logger.debug('{}: it is time to check toolchain for update'.format(self._toolchain_name))
        elif self.__is_auto_update_disabled:
            raise Exception('File {} is mandatory if auto update is disabled'.format(info_path))
        return None

    def _find_resource(self):
        resources = []
        failed_queries = []
        for sb_api_query in self._get_sb_api_queries():
            logger.debug('%s: lookup resources with query: %s', self._toolchain_name, sb_api_query)
            resources = _list_resources(**sb_api_query)
            if resources:
                assert len(resources) == 1
                resource = resources[0]
                logger.debug('%s: found resource: %s', self._toolchain_name, resource)
                return resource
            failed_queries.append(json.dumps(sb_api_query))
        raise ResourceNotFound('{}: no resource is found. Sandbox queries made:\n{}'.format(self._toolchain_name, ',\n'.join(failed_queries)))

    def _get_sb_api_queries(self, platforms=None, limit=1):
        from sandbox.common.types.resource import State as ResourceState

        sb_api_query = {
            'type': self.__params['query']['resource_type'],
            'owner': self.__params['query']['owner'],
            'state': ResourceState.READY,
            'order': '-id',
            'limit': limit,
        }
        if 'attributes' in self.__params['query']:
            sb_api_query['attrs'] = self.__params['query']['attributes'].copy()
        if platforms is None:
            platforms = self.__platforms
        if platforms:
            for platform in self.__platforms:
                sb_api_query.setdefault('attrs', {})['platform'] = platform
                yield deepcopy(sb_api_query)
        else:
            yield sb_api_query

    def _get_resource_uri(self, resource_id):
        return "sbr:{}".format(resource_id)

    def _details(self):
        return "params: {}".format(self.__params)

    def _fetch(self):
        dump_file = os.environ.get("YA_DUMP_RESOURCES_FILE")
        if dump_file:
            self._dump_resources_to_file(dump_file)
        resource_id = self._get_matched_resource_id()
        fetcher, progress_callback = _get_fetcher(self._toolchain_name)
        fetch_resource_if_need(fetcher, self._root, self._get_resource_uri(resource_id), progress_callback, force_refetch=self._force_refetch, **self._binname_kwargs)
        return self._install_symlink(resource_id)

    def _install_symlink(self, resource_id):
        resource_path = self._resource_path(resource_id)
        link_path = self._get_symlink_path()
        toolscache.notify_tool_cache(link_path)
        return install_symlink(resource_path, link_path)

    @func.lazy_property
    def __is_auto_update_disabled(self):
        return os.path.exists(os.path.join(self._root, self._DISABLE_AUTO_UPDATE_FILE_NAME))

    # Dumped once
    @stringify_memoize
    def _dump_resources_to_file(self, dump_file):
        found_platforms = set()
        resources = []
        for sb_api_query in self._get_sb_api_queries(platforms=[], limit=30):
            logger.debug('{}: lookup resources with query: {}'.format(self._toolchain_name, sb_api_query))
            for resource in _list_resources(**sb_api_query):
                platform = resource['attributes'].get('platform', 'any')
                if platform not in found_platforms:
                    found_platforms.add(platform)
                    resources.append(resource)
        _dump_resources_file(dump_file, resources, self.__bottle_name)


def _to_list(val):
    if val is None:
        return []

    if isinstance(val, list):
        return val

    return [val]


def _get_fetcher(name):
    try:
        import app_ctx

        def display_progress(percent=None):
            if app_ctx.state.check_cancel_state():
                app_ctx.display.emit_status('Downloading [[imp]]{}[[rst]] - [[imp]]{:.1f}%[[rst]]'.format(name, percent))

        progress_printer = ProgressPrinter(
            progress_callback=display_progress,
            finish_callback=lambda: app_ctx.display.emit_status('')
        )
        return app_ctx.fetcher, progress_printer
    except (ImportError, AttributeError):
        from yalibrary.yandex.sandbox import fetcher

        progress_printer = ProgressPrinter(
            progress_callback=lambda x: sys.stderr.write('.'),
            finish_callback=lambda: sys.stderr.write('\n')
        )
        return fetcher.SandboxFetcher(), progress_printer


def _list_all_resources(**kwargs):
    from yalibrary.yandex.sandbox import SandboxClient
    return _do_sandbox_method(SandboxClient.list_all_resources, **kwargs)


def _list_resources(**kwargs):
    from yalibrary.yandex.sandbox import SandboxClient
    return _do_sandbox_method(SandboxClient.list_resources, **kwargs)


def _do_sandbox_method(method, **kwargs):
    rest_params = {'total_wait': _SB_REST_API_TIMEOUT, 'check_for_cancel': _get_cancel_checker()}
    guards.update_guard(guards.GuardTypes.FETCH)
    from yalibrary.yandex.sandbox import SandboxClient
    try:
        return method(SandboxClient(token=_get_sandbox_token(), rest_params=rest_params), **kwargs)
    except requests.HTTPError as e:
        logger.debug("Error connecting to sandbox: %s", e)
        if e.response.status_code != 403:
            raise e
    logger.warning("Incorrect sandbox token, trying anonymous access")

    return method(SandboxClient(rest_params=rest_params), **kwargs)


def _get_cancel_checker():
    try:
        import app_ctx
        return app_ctx.state.check_cancel_state
    except (ImportError, AttributeError):
        return None


def _get_sandbox_token():
    try:
        import app_ctx
        _, _, sandbox_token = app_ctx.fetcher_params
        return sandbox_token
    except (ImportError, AttributeError):
        return None


def _dump_resources_file(dump_file, resources, bottle_name=None):
    task_resources = {}
    # Group resources by task id
    for resource in resources:
        task_resources.setdefault(resource['task']['id'], []).append(resource)
    with open(dump_file, "a") as f:
        for task_id, res in task_resources.items():
            row = {'task_id': task_id, 'list_resources': res}
            if bottle_name:
                row['bottle'] = bottle_name
            json.dump(row, f)
            f.write('\n')
