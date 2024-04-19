from yt_odin_checks.lib.check_runner import main
from yt.wrapper import get_retriable_errors, YtClient
from yt.common import join_exceptions, YtError


def run_check(secrets, yt_client, logger, options, states):
    cluster_name = options["cluster_name"]
    required_query_tracker_instances = options["cluster_name_to_query_tracker_count"].get(cluster_name, 1)

    logger.info("Checking for relevant query tracker alerts from query tracker stage on cluster %s", cluster_name)

    yt_client_no_retries = YtClient(
        proxy=yt_client.config["proxy"]["url"],
        token=secrets["yt_token"],
        config={"proxy": {"retries": {"count": 1, "enable": False}}})

    disconnected_query_trackers = []
    connected_query_tracker_count = 0
    query_tracker_instances = yt_client_no_retries.list("//sys/query_tracker/instances")

    all_errors = []
    for instance in query_tracker_instances:
        logger.info(f"Collecting alerts from {instance}")
        alerts = None

        try:
            alerts = yt_client_no_retries.get(f"//sys/query_tracker/instances/{instance}/orchid/alerts")
        except join_exceptions(YtError, get_retriable_errors()):
            logger.exception("Failed to check qt %s connectivity in orchid", instance)
            pass

        if alerts is None:
            logger.info("Query tracker %s is not connected", instance)
            disconnected_query_trackers.append(instance)
        else:
            logger.info("Query tracker %s is connected", instance)
            connected_query_tracker_count += 1

            for alert, raw_error in alerts.items():
                error = YtError.from_dict(raw_error)
                logger.error(f"Collected alert {alert} with error {error.simplify()}")
                all_errors.append(str(error))

    if connected_query_tracker_count < required_query_tracker_instances:
        all_errors.append("Query Tracker(s) {} has disconnected".format(", ".join(disconnected_query_trackers)))

    if all_errors:
        alert_message = "; ".join(all_errors)
        return states.UNAVAILABLE_STATE, alert_message

    return states.FULLY_AVAILABLE_STATE


if __name__ == "__main__":
    main(run_check)
