from config import get_config
from common import get_value
from format import YamrFormat

import os

def set_yamr_mode(client=None):
    """Configure global config to be yamr compatible"""
    config = get_config(client)
    for option in config["yamr_mode"]:
        if option in ["abort_transactions_with_remove", "use_yamr_style_prefix"]:
            continue
        config["yamr_mode"][option] = True
    if "MR_TABLE_PREFIX" in os.environ:
        config["prefix"] = get_value(config["prefix"], "") + os.environ["MR_TABLE_PREFIX"]
    config["tabular_data_format"] = YamrFormat(has_subkey=True, lenval=False)
