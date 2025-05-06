try:
    from .yandex_constants import (
        DEFAULT_HOST_SUFFIX,
        FEEDBACK_URL,
        LOCAL_MODE_URL_PATTERN,
        OAUTH_URL,
        UI_ADDRESS_PATTERN,
        SKYNET_MANAGER_URL,
        TUTORIAL_DOC_URL,
        DOC_ROOT_URL,
        YSON_PACKAGE_INSTALLATION_TEXT,
        RPC_PACKAGE_INSTALLATION_TEXT,
        PICKLING_DL_ENABLE_AUTO_COLLECTION,
        ENCRYPT_PICKLE_FILES,
        ENABLE_YP_SERVICE_DISCOVERY,
        STARTED_BY_COMMAND_LENGTH_LIMIT,
    )
except ImportError:
    DEFAULT_HOST_SUFFIX = ""
    FEEDBACK_URL = "https://ytsaurus.tech/#contact"
    LOCAL_MODE_URL_PATTERN = "{local_mode_address}"
    OAUTH_URL = ""
    UI_ADDRESS_PATTERN = ""
    SKYNET_MANAGER_URL = "{cluster_name}"
    DOC_ROOT_URL = "https://ytsaurus.tech/docs/en"
    TUTORIAL_DOC_URL = "https://ytsaurus.tech/docs/en/overview/try-yt"
    YSON_PACKAGE_INSTALLATION_TEXT = 'as pip package "ytsaurus-yson"'
    RPC_PACKAGE_INSTALLATION_TEXT = 'as pip package "ytsaurus-rpc-driver"'
    PICKLING_DL_ENABLE_AUTO_COLLECTION = True
    ENCRYPT_PICKLE_FILES = None
    ENABLE_YP_SERVICE_DISCOVERY = False
    STARTED_BY_COMMAND_LENGTH_LIMIT = 4096
