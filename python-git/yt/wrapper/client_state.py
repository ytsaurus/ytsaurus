class ClientState(object):
    def __init__(self):
        self.RETRY = None
        self.SPEC = None
        self.MUTATION_ID = None
        self.TRACE = None
        self.TRANSACTION = "0-0-0-0"
        self.PING_ANCESTOR_TRANSACTIONS = False
        self._ENABLE_READ_TABLE_CHAOS_MONKEY = False
        self._ENABLE_HTTP_CHAOS_MONKEY = False
        self._ENABLE_HEAVY_REQUEST_CHAOS_MONKEY = False

        self._transaction_stack = None
        self._banned_proxies = {}
        self._ip_configured = False
        self._driver = None
        self._requests_session = None

        # Cache for API version (to check it only once)
        self._api_version = None
        self._commands = None


