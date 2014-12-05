from common import update_from_env

PROXY = None
PROXY_SUFFIX = ".yt.yandex.net"

TOKEN = None
TOKEN_PATH = None
USE_TOKEN = True

ACCEPT_ENCODING = "gzip, identity"
CONTENT_ENCODING = "gzip"

REQUEST_RETRY_TIMEOUT = 20 * 1000
REQUEST_RETRY_COUNT = 6
REQUEST_BACKOFF = None
CONTENT_CHUNK_SIZE = 10 * 1024

RETRY_VOLATILE_COMMANDS = True

FORCE_IPV4 = False
FORCE_IPV6 = False

def get_timeout():
    return REQUEST_RETRY_TIMEOUT * REQUEST_RETRY_COUNT

update_from_env(globals())
