from yt_odin_checks.lib.check_runner import main
from yt.wrapper import YtClient
from yt.common import YtError


def run_check(secrets, yt_client, logger, options, states):
    cluster_name = options["cluster_name"]
    logger.info("Checking for relevant query tracker alerts from query tracker stage on cluster %s", cluster_name)

    query_tracker_stage_client = YtClient(
        proxy=cluster_name,
        token=secrets["yt_token"],
        config={"proxy": {"retries": {"count": 1, "enable": False}}})

    query_tracker_instances = query_tracker_stage_client.list("//sys/query_tracker/instances")

    all_errors = []
    for instance in query_tracker_instances:
        logger.info(f"Collecting alerts from {instance}")
        alerts = query_tracker_stage_client.get(f"//sys/query_tracker/instances/{instance}/orchid/alerts")

        for alert, raw_error in alerts.items():
            error = YtError.from_dict(raw_error)
            logger.error(f"Collected alert {alert} with error {error.simplify()}")
            all_errors.append(str(error))

    if all_errors:
        alert_message = "; ".join(all_errors)
        return states.UNAVAILABLE_STATE, alert_message

    return states.FULLY_AVAILABLE_STATE


if __name__ == "__main__":
    main(run_check)
