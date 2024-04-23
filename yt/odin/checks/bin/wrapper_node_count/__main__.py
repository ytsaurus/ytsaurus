from yt_odin_checks.lib.check_runner import main
try:
    from yt_odin_checks.lib.yandex_helpers import get_link
except ImportError:
    get_link = lambda cluster, check: ""  # noqa


def check_node_count(yt_client, path, count_threshold, cluster_name, logger, states):
    if yt_client.exists(path):
        count = yt_client.get(f"{path}/@count")
        if count >= count_threshold:
            logger.info(f"{path}/@count:{count} -ge threshold:{count_threshold}")
            check_link = get_link(cluster_name, "wrapper_node_count")
            return states.UNAVAILABLE_STATE, \
                f"{path}/@count:{count} -ge threshold:{count_threshold} {check_link}"
    else:
        return states.PARTIALLY_AVAILABLE_STATE, f"Wrapper file path do not exist: {path}"
    logger.info(f"{path}/@count:{count} -lt threshold:{count_threshold}")
    return states.FULLY_AVAILABLE_STATE, \
        f"{path}/@count:{count} -lt threshold:{count_threshold}"


def run_check(yt_client, logger, options, states):
    cluster_name = options["cluster_name"]
    file_state = check_node_count(yt_client, "//tmp/yt_wrapper/file_storage", options["file_count_threshold"], cluster_name, logger, states)
    table_state = check_node_count(yt_client, "//tmp/yt_wrapper/table_storage", options["table_count_threshold"], cluster_name, logger, states)
    return min(file_state, table_state)


if __name__ == "__main__":
    main(run_check)
