try:
    from .yandex_constants import (
        DEFAULT_HOST_SUFFIX,
        FEEDBACK_URL,
        LOCAL_MODE_URL_PATTERN,
        OAUTH_URL,
        UI_ADDRESS_PATTERN,
        SKYNET_MANAGER_URL,
        GETTINGSTARTED_DOC_URL,
        TUTORIAL_DOC_URL,
    )
except ImportError:
    DEFAULT_HOST_SUFFIX = ""
    FEEDBACK_URL = "https://ytsaurus.tech/#contact"
    LOCAL_MODE_URL_PATTERN = "{local_mode_address}"
    OAUTH_URL = ""
    UI_ADDRESS_PATTERN = ""
    SKYNET_MANAGER_URL = "{cluster_name}"
    GETTINGSTARTED_DOC_URL = "<TODO>"
    TUTORIAL_DOC_URL = "<TODO>"
