from yt_odin_checks.lib.check_runner import main
try:
    from yt_odin_checks.lib.yandex_helpers import get_link
except ImportError:
    get_link = lambda cluster, check: ""  # noqa


def run_check(yt_client, logger, options, states):
    path = '//tmp/yt_wrapper/file_storage'
    if yt_client.exists(path):
        count = yt_client.get("{}/@count".format(path))
        if count > options["files_count_threshold"]:
            logger.info("{}/@count:{} -ge threshold:{}".format(path, count, options["files_count_threshold"]))
            return states.UNAVAILABLE_STATE, "{}/@count:{} -gt threshold:{} {}".\
                format(path, count, options["files_count_threshold"],
                       get_link(options["cluster_name"], "wrapper_files_count"))
    else:
        return states.PARTIALLY_AVAILABLE_STATE, "Wrapper file path do not exist: {}".format(path)
    logger.info("{}/@count:{} -le threshold:{}".format(path, count, options["files_count_threshold"]))
    return states.FULLY_AVAILABLE_STATE


if __name__ == "__main__":
    main(run_check)
