from yt_odin_checks.lib.check_runner import main
from yt_odin_checks.lib.master_chunk_management import get_bool_attribute_if_exists
from yt_odin_checks.lib.master_chunk_management import get_results


def run_check(yt_client, logger, options, states):
    chunk_refresh_enabled = "//sys/@chunk_refresh_enabled"
    chunk_requisition_update_enabled = "//sys/@chunk_requisition_update_enabled"

    final_state = states.FULLY_AVAILABLE_STATE
    messages = []

    for attr_path in [chunk_refresh_enabled, chunk_requisition_update_enabled]:
        status_flag = get_bool_attribute_if_exists(yt_client, attr_path)
        state, message = get_results(status_flag, states)
        if state == states.UNAVAILABLE_STATE:
            final_state = state
        messages.append(message.format(attr_path))

    if final_state == states.UNAVAILABLE_STATE:
        logger.info("Check failed (messages: %s)", str(messages))
    else:
        logger.info("Check is successful (messages: %s)", str(messages))

    return final_state


if __name__ == "__main__":
    main(run_check)
