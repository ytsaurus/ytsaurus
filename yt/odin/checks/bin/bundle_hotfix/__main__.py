from yt_odin_checks.lib.check_runner import main

from yt.yson.yson_types import YsonEntity


BC_HOTFIX = "bc_hotfix"
BUNDLES_PATH = "//sys/tablet_cell_bundles"


def run_check(yt_client, logger, options, states):
    yt_client.config["proxy"]["request_timeout"] = 30000

    bundles = yt_client.get(BUNDLES_PATH, attributes=[BC_HOTFIX])

    bundles_in_hotfix = []
    for key, value in bundles.items():
        if value != YsonEntity() and value.attributes[BC_HOTFIX]:
            bundles_in_hotfix.append(key)

    if bundles_in_hotfix:
        message = f"Bundles {bundles_in_hotfix} are in hotfix mode"
        logger.warning(message)
        return states.UNAVAILABLE_STATE, message

    return states.FULLY_AVAILABLE_STATE, "OK"


if __name__ == "__main__":
    main(run_check)
