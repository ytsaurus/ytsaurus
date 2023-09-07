import logging
import os
import stat
from collections import namedtuple
from functools import wraps
from toolz.functoolz import curry, memoize

from exts import fs
import exts.yjson as json


logger = logging.getLogger(__name__)


UNTAR = 0
RENAME = 1
FIXED_NAME = 2
BINARY = 3


def clean_dir(dir):
    try:
        os.unlink(dir)
    except OSError:
        pass
    fs.remove_tree_safe(dir)
    fs.create_dirs(dir)


def deploy_tool(archive, extract_to, post_process, resource_info, resource_uri, binname=None):
    RESOURCE_INFO_JSON = "resource_info.json"
    RESOURCE_CONTENT_FILE_NAME = "resource"
    RESOURCE_URI = "lnk"

    if UNTAR == post_process:
        try:
            import exts.archive
            logger.debug("extract {0} to {1} dir".format(archive, extract_to))
            exts.archive.extract_from_tar(archive, extract_to)
        finally:
            fs.remove_file(archive)
    elif RENAME == post_process:
        base_name = os.path.basename(resource_info['file_name'])
        logger.debug("move {0} to {1} dir".format(archive, os.path.join(extract_to, base_name)))
        fs.move(archive, os.path.join(extract_to, base_name))
    elif FIXED_NAME == post_process:
        logger.debug("move {0} to {1} dir".format(archive, os.path.join(extract_to, RESOURCE_CONTENT_FILE_NAME)))
        fs.move(archive, os.path.join(extract_to, RESOURCE_CONTENT_FILE_NAME))
    elif BINARY == post_process:
        file_name = os.path.basename(binname)
        full_path = os.path.join(extract_to, file_name)
        fs.replace(archive, full_path)
        st = os.stat(full_path)
        os.chmod(full_path, st.st_mode | stat.S_IEXEC)

    meta_info = os.path.join(extract_to, RESOURCE_INFO_JSON)
    if os.path.exists(meta_info):
        logger.debug("Meta information cannot be stored: {} already exists".format(meta_info))
    else:
        with open(meta_info, "w") as f:
            json.dump(resource_info, f, indent=4)

    uri_file = os.path.join(extract_to, RESOURCE_URI)
    if os.path.exists(uri_file):
        logger.debug("Link information cannot be stored: {} already exists".format(uri_file))
    else:
        with open(uri_file, "w") as f:
            f.write(resource_uri)


ParsedResourceUri = namedtuple('ParsedResourceUri', 'resource_type, resource_uri, resource_id, resource_url')


def parse_resource_uri(resource_uri):
    resource_type, rest = resource_uri.split(':', 1)
    if resource_type == 'sbr':
        resource_id = rest
        return ParsedResourceUri(resource_type, resource_uri, resource_id, None)
    elif resource_type == 'https':
        resource_url, resource_id = resource_uri.split('#')
        return ParsedResourceUri(resource_type, resource_uri, resource_id, resource_url)
    elif resource_type == 'base64':
        return ParsedResourceUri(resource_type, resource_uri, None, rest)
    else:
        raise Exception('Unknown platform in uri: {}'.format(resource_uri))


@curry
def stringify_memoize(orig_func, cache_kwarg=None):
    '''
        Creative rethinking of pg's caching approach.

        Memoize a function using it's parameters stringification as a key.
        If cache_kwarg is not None it's value is used as a name for additional kwarg.
        Passing this kwarg with False value disable memoization for the particular call.
        Notice: this kwarg will be never passed to the original function.
    '''
    memoized_func = memoize(func=orig_func, key=lambda args, kwargs: str((args, list(sorted(kwargs)))))
    if cache_kwarg is None:
        return memoized_func

    @wraps(orig_func)
    def wrapper(*args, **kwargs):
        if kwargs.pop(cache_kwarg, True):
            return memoized_func(*args, **kwargs)
        else:
            return orig_func(*args, **kwargs)

    return wrapper


class ProgressPrinter(object):
    def __init__(self, progress_callback, finish_callback=lambda: None):
        self._progress = progress_callback
        self._finish = finish_callback

    def __call__(self, percent):
        self._progress(percent)

    def finalize(self):
        self._finish()
