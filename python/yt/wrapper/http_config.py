from common import update_from_env

PROXY = None

TOKEN = None
TOKEN_PATH = None
USE_TOKEN = True

ACCEPT_ENCODING = "gzip, identity"
CONTENT_ENCODING = "gzip"

CONNECTION_TIMEOUT = 30.0

HTTP_RETRIES_COUNT = 5
HTTP_RETRY_TIMEOUT = 10

# COMPAT(ignat): remove option when version 14 become stable
RETRY_VOLATILE_COMMANDS = False

REQUESTS_RETRIES = 10

FORCE_IPV4 = False
FORCE_IPV6 = False

update_from_env(globals())
