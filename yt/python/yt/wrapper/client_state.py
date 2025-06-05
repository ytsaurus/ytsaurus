from .system_random import SystemRandom

from copy import deepcopy


class ClientState(object):
    def __init__(self, other=None):
        if other is None:
            self._init_state()
        else:
            self._copy_init_state(other)

    def __del__(self):
        try:
            if self._requests_session_origin_id and id(self) == self._requests_session_origin_id:
                self._cleanup()
        except Exception:
            # in some cases (global termination), objects may have already been destroyed
            pass

    def _cleanup(self):
        if self._requests_session:
            self._requests_session.close()

    def _init_state(self):
        self.COMMAND_PARAMS = {
            "transaction_id": "0-0-0-0",
            "ping_ancestor_transactions": False,
            "suppress_transaction_coordinator_sync": False
        }
        self._ENABLE_READ_TABLE_CHAOS_MONKEY = False
        self._ENABLE_HTTP_CHAOS_MONKEY = False
        self._ENABLE_HEAVY_REQUEST_CHAOS_MONKEY = False

        self._client_type = "single"

        self._transaction_stack = None
        self._driver = None
        self._requests_session = None  # type: requests.Session
        self._requests_session_origin_id = None
        self._heavy_proxy_provider_state = None

        # socket.getfqdn can be slow so client fqdn is cached.
        self._fqdn = None

        # Token can be received from oauth server, in this case we do not want to
        # request on each request to YT.
        self._token = None
        self._token_cached = False
        self._current_user = None

        # Cache for API version (to check it only once).
        self._api_version = None
        self._commands = None
        self._is_testing_mode = None

        # Cache for local_mode related variable (used to detect local mode in client).
        self._local_mode_fqdn = None
        self._local_mode_proxy_address = None

        self._random_generator = SystemRandom()
        self._generate_mutation_id = None

    def _copy_init_state(self, other):
        self.__dict__.update(deepcopy(other._as_dict()))

    def _as_dict(self):
        # Hacky way, can we do this more straightforward?
        result = {}
        for attr in filter(lambda attr: not attr.startswith("__"), ClientState().__dict__):
            result[attr] = getattr(self, attr)
        return result
