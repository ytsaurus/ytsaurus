from .config import get_config
from .common import get_value
from .format import YamrFormat

import os


def set_yamr_prefix(client=None):
    """Update prefix by yamr env-variable."""
    config = get_config(client)
    if "MR_TABLE_PREFIX" in os.environ:
        config["prefix"] = get_value(config["prefix"], "") + os.environ["MR_TABLE_PREFIX"]


def set_yamr_mode(client=None):
    """Configures global config to be yamr compatible."""
    config = get_config(client)
    for option in config["yamr_mode"]:
        if option in ["abort_transactions_with_remove", "use_yamr_style_prefix",
                      "use_yamr_defaults", "ignore_empty_tables_in_mapreduce_list",
                      "create_schema_on_tables"]:
            continue
        config["yamr_mode"][option] = True

        env_name = "YT_" + option.upper()
        if env_name in os.environ:
            config["yamr_mode"][option] = bool(int(os.environ[env_name]))

    config["tabular_data_format"] = YamrFormat(has_subkey=True, lenval=False)

    set_yamr_prefix(client=client)
