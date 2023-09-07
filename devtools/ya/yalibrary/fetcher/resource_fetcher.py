import base64
import logging
import os

from exts import uniq_id, http_client
from exts.hashing import md5_value
from yalibrary import guards
from yalibrary import platform_matcher

from core import config

from .common import (
    RENAME,
    UNTAR,
    ProgressPrinter,
    clean_dir,
    deploy_tool,
    parse_resource_uri,
)

from .cache_helper import install_resource


class MissingResourceError(Exception):
    mute = True


logger = logging.getLogger(__name__)


def fetch_base64_resource(root_dir, resource_uri):
    parsed_uri = parse_resource_uri(resource_uri)
    assert parsed_uri.resource_type == 'base64'

    base_dir = md5_value(parsed_uri.resource_url)
    result_dir = os.path.join(root_dir, base_dir)

    logger.debug("Fetching {} to {} dir)".format(parsed_uri.resource_url[:20], result_dir))

    def do_download(download_to):
        base_name, content = parsed_uri.resource_url.split(':', 1)
        with open(os.path.join(download_to), 'wb') as f:
            f.write(base64.b64decode(content))
        return {"file_name": base_name}

    def do_deploy(download_to, resource_info):
        deploy_tool(download_to, result_dir, RENAME, resource_info, resource_uri)

    return _do_fetch_resource_if_need(result_dir, do_download, do_deploy, target_is_tool_dir=False)


# install_params are
#   1. post-process:
#       - UNTAR - unpack archive from resource
#       - RENAME - rename according to file_name from resource_info
#       - FIXED_NAME - keep resource under fixed name 'resource'
#   2. tool_dir (True) or resource_dir (False)
def fetch_resource_if_need(fetcher, root_dir, resource_uri, progress_callback=lambda _: None, state=None, install_params=(UNTAR, True), binname=None, force_refetch=False, keep_directory_packed=False):
    parsed_uri = parse_resource_uri(resource_uri)
    post_process, target_is_tool_dir = install_params
    result_dir = os.path.join(root_dir, parsed_uri.resource_id)

    logger.debug("Fetching {} from {} to {} dir, post_process={})".format(parsed_uri.resource_id, parsed_uri.resource_uri, result_dir, post_process))

    downloader = _get_downloader(fetcher, parsed_uri, progress_callback, state, keep_directory_packed)

    def do_deploy(download_to, resource_info):
        deploy_tool(download_to, result_dir, post_process, resource_info, resource_uri, binname)

    return _do_fetch_resource_if_need(result_dir, downloader, do_deploy, target_is_tool_dir, force_refetch)


def select_resource(item, platform=None):
    if 'resource' in item:
        return item['resource']
    elif 'resources' in item:
        if platform:
            try:
                parsed = platform_matcher.parse_platform(platform)
                platform = platform_matcher.canonize_full_platform('-'.join((parsed['os'], parsed['arch']))).lower()
            except platform_matcher.InvalidPlatformSpecification:
                platform = platform_matcher.canonize_full_platform(platform).lower()
        else:
            platform = platform_matcher.my_platform().lower()
        for res in item['resources']:
            if res['platform'].lower() == platform:
                return res['resource']
        raise Exception('Unable to find resource {} for current platform'.format(item['pattern']))
    else:
        raise Exception('Incorrect resource format')


def _get_downloader(fetcher, parsed_uri, progress_callback, state, keep_directory_packed):
    default_resource_info = {"file_name": parsed_uri.resource_id[:20]}
    if parsed_uri.resource_type == 'https':
        return _HttpDownloader(parsed_uri.resource_url, parsed_uri.resource_id, default_resource_info)
    elif parsed_uri.resource_type == 'sbr':
        if config.has_mapping():
            return _HttpDownloaderWithConfigMapping(parsed_uri.resource_id, default_resource_info)
        else:
            return _DefaultDownloader(fetcher, parsed_uri.resource_id, progress_callback, state, keep_directory_packed)
    else:
        raise Exception('Unsupported resource_uri {}'.format(parsed_uri.resource_uri))


def _do_fetch_resource_if_need(result_dir, downloader, deployer, target_is_tool_dir=True, force_refetch=False):
    def do_install():
        guards.update_guard(guards.GuardTypes.FETCH)
        download_to = os.path.join(result_dir, 'resource.' + uniq_id.gen8())
        resource_info = downloader(download_to)
        deployer(download_to, resource_info)

    if target_is_tool_dir:
        return install_resource(result_dir, do_install, force_refetch)
    else:
        clean_dir(result_dir)
        do_install()
        return result_dir


class _DownloaderBase(object):
    def __call__(self, download_to):
        raise NotImplementedError()


class _HttpDownloader(_DownloaderBase):
    def __init__(self, resource_url, resource_md5, resource_info):
        self._url = resource_url
        self._md5 = resource_md5
        self._info = resource_info

    def __call__(self, download_to):
        http_client.download_file(
            url=self._url,
            path=download_to,
            expected_md5=self._md5
        )
        return self._info


class _HttpDownloaderWithConfigMapping(_HttpDownloader):
    def __init__(self, resource_id, resource_info):
        assert config.has_mapping()
        mapping = config.mapping()
        if resource_id in mapping.get("resources_info", []):
            resource_info = mapping["resources_info"][resource_id]

        if resource_id not in mapping["resources"]:
            raise MissingResourceError("Resource mapping doesn't have required resource: {}".format(resource_id))

        url = mapping["resources"][resource_id]

        super(_HttpDownloaderWithConfigMapping, self).__init__(
            resource_url=url,
            resource_md5=None,
            resource_info=resource_info
        )


class _DefaultDownloader(_DownloaderBase):
    def __init__(self, fetcher, resource_id, progress_callback, state, keep_directory_packed):
        self._fetcher = fetcher
        self._resource_id = resource_id
        self._progress_callback = progress_callback
        self._state = state
        self._keep_directory_packed = keep_directory_packed

    def __call__(self, download_to):
        try:
            return self._fetcher.fetch_resource(
                self._resource_id,
                download_to,
                self._progress_callback,
                self._state,
                self._keep_directory_packed
            )
        finally:
            if isinstance(self._progress_callback, ProgressPrinter):
                self._progress_callback.finalize()
