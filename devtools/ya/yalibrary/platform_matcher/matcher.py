import logging
import platform
import re

import exts.func

logger = logging.getLogger(__name__)


_PLATFORM_SEP = '-'


class PlatformNotSupportedException(Exception):
    mute = True


class InvalidPlatformSpecification(Exception):
    mute = True


def compute_platform_suffix(full_platform):
    for arch in ('aarch64', 'ppc64le', 'arm64'):
        if arch in full_platform:
            return '-{}'.format(arch)
    return ''


def canonize_platform(platform_, release=None):
    platform_ = platform_.lower()

    if release is not None:
        release = release.lower()

    if platform_ == 'any':
        return 'any'

    if platform_.startswith('linux'):
        return 'linux' + compute_platform_suffix(platform_)

    if platform_.startswith('win'):
        return 'win32'

    if platform_ in ('darwin', 'darwin-arm64'):
        return platform_

    if platform_.startswith('cygwin'):
        return 'cygwin'

    if platform_ == 'freebsd':
        if release is None:
            raise PlatformNotSupportedException(platform_ + ' without release version')
        release_parts = release.split('.')
        rel_id = int(release_parts[0])
        return 'freebsd' + str(rel_id)

    raise PlatformNotSupportedException(platform_ + ' not yet supported in sandbox')


def canonize_full_platform(full_platform):
    prefix_len = len(re.match(r'^[a-zA-Z]+', full_platform).group(0))
    platform_ = full_platform[:prefix_len]
    release = full_platform[prefix_len:].strip('-')
    return canonize_platform(platform_ + compute_platform_suffix(full_platform), release)


def is_darwin_arm64():
    # native ya-bin for arm
    if platform.system() == 'Darwin' and platform.machine() == 'arm64':
        return True
    # rosetta
    if platform.system() == 'Darwin' and '/RELEASE_ARM64' in platform.version():
        return True
    return False


def my_platform():
    full_platform = platform.platform()
    # https://st.yandex-team.ru/DTCC-277
    # if is_darwin_arm64():
    #     return 'darwin-arm64'
    for arch in ('aarch64', 'ppc64le'):
        if arch in full_platform:
            return 'linux-{}'.format(arch)

    return canonize_platform(platform.system(), platform.release())


PLATFORM_REPLACEMENTS = {
    'darwin-arm64': ['darwin'],
}


def get_platform_replacements(platform, custom_platform_replacements):
    platform_replacements = PLATFORM_REPLACEMENTS if custom_platform_replacements is None else custom_platform_replacements
    return platform_replacements.get(platform, [])


def match_platform(expect, platforms, custom_platform_replacements=None):
    if expect in platforms:
        return expect

    canonized_platforms = {}
    for platform_ in platforms:
        canonized = canonize_full_platform(platform_)
        if canonized in canonized_platforms:
            raise InvalidPlatformSpecification('Platforms "{}" and "{}" have the same canonized form "{}"'.format(platform_, canonized_platforms[canonized], canonized))
        canonized_platforms[canonized] = platform_

    if expect in canonized_platforms:
        return canonized_platforms[expect]

    for replacement in get_platform_replacements(expect, custom_platform_replacements):
        if replacement in canonized_platforms:
            return canonized_platforms[replacement]

    if 'any' in platforms:
        return 'any'

    return None


@exts.func.lazy
def current_architecture():
    arch = platform.machine().upper()
    # https://st.yandex-team.ru/DTCC-277
    # if is_darwin_arm64():
    #     return "ARM64"
    if arch == 'AMD64':
        return 'X86_64'
    return arch


@exts.func.lazy
def current_os():
    platf = platform.system().upper()
    if platf.startswith('WIN'):
        return 'WIN'
    if platf.startswith('CYGWIN'):
        return 'CYGWIN'
    return platf


def current_platform():
    return {'os': current_os(), 'arch': current_architecture()}


@exts.func.lazy
def current_toolchain():
    res = current_platform().copy()

    res['toolchain'] = 'default'

    return res


def stringize_platform(p, sep=_PLATFORM_SEP):
    return (p['toolchain'] + sep + p['os'] + sep + p['arch']).upper()


def prevalidate_platform(s):
    return s.count(_PLATFORM_SEP) == 2


def parse_platform(s):
    if not prevalidate_platform(s):
        raise InvalidPlatformSpecification('Unsupported platform: %s' % s)

    toolchain, os, arch = s.split(_PLATFORM_SEP)

    return {'toolchain': toolchain.lower(), 'os': os.upper(), 'arch': arch.lower()}


# XXX: mine from ya.conf.json
def guess_os(s):
    s = s.upper()

    if s.startswith('WIN'):
        s = 'WIN'

    if s in ('LINUX', 'WIN', 'FREEBSD', 'DARWIN'):
        return s

    return None


def guess_platform(s):
    s = s.upper()

    if not s.count(_PLATFORM_SEP):
        cp = current_platform()
        os = guess_os(s)

        if os:
            return 'DEFAULT-{}-{}'.format(os, cp['arch'])

        return '{}-{}-{}'.format(s, cp['os'], cp['arch'])

    return s
