from yt_odin_checks.lib.check_runner import main
from yt.wrapper import YtClient
from yt.common import YtError

from collections import defaultdict


def run_check(secrets, yt_client, logger, options, states):
    cluster_name = options["cluster_name"]
    queue_agent_stage_clusters = options["queue_agent_stage_clusters"]

    all_errors = []

    generic_alerts_by_stage_cluster = defaultdict(lambda: defaultdict(dict))

    # assuming options["queue_agent_stage_clusters"] is a dict {cluster_name: proxy}
    for queue_agent_stage_cluster, proxy in queue_agent_stage_clusters.items():
        # Each queue agent stage has a corresponding host cluster.
        # General unavailability of the host cluster or the queue agent itself
        # should only cause this check to fail when running on the host cluster.
        # Otherwise, we want to skip the unavailable queue agent.
        def run_method(functor, null_value):
            try:
                return functor()
            except Exception as err:
                logger.warning("Could not perform alert collection from queue agent stage on cluster %s",
                               queue_agent_stage_cluster)
                if queue_agent_stage_cluster == cluster_name:
                    raise err
                return null_value

        logger.info("Checking for relevant queue agent alerts from queue agent stage on cluster %s", queue_agent_stage_cluster)

        queue_agent_stage_client = YtClient(
            proxy=proxy,
            token=secrets["yt_token"],
            config={"proxy": {"retries": {"count": 1, "enable": False}}})

        queue_agent_instances = run_method(lambda: queue_agent_stage_client.list("//sys/queue_agents/instances"),
                                           null_value=[])

        for instance in queue_agent_instances:
            logger.info(f"Collecting alerts from {instance}")
            alerts = run_method(lambda: queue_agent_stage_client.get(f"//sys/queue_agents/instances/{instance}/orchid/alerts"),
                                null_value=dict())

            for alert, raw_error in alerts.items():
                error = YtError.from_dict(raw_error)

                if not error.inner_errors:
                    # If there are no inner error, we consider this alert generic for the corresponding queue agent.
                    generic_alerts_by_stage_cluster[queue_agent_stage_cluster][instance][alert] = error

                for inner_error in error.inner_errors:
                    if inner_error.attributes.get("cluster") == cluster_name:
                        logger.error(f"Collected relevant alert {alert} with error {error.simplify()}")
                        logger.error(f"Relevant inner error: {inner_error.simplify()}")
                        all_errors.append(str(error.simplify()))
                    elif "cluster" not in inner_error.attributes:
                        # If there is at least one inner error without a specified cluster attribute,
                        # we consider this alert generic for the corresponding queue agent.
                        generic_alerts_by_stage_cluster[queue_agent_stage_cluster][instance][alert] = error

    if cluster_name in queue_agent_stage_clusters.keys():
        logger.info("Checking for generic alerts for queue agent stage on our cluster %s", cluster_name)
        alerts_by_instance = generic_alerts_by_stage_cluster[cluster_name]

        for instance, alerts in alerts_by_instance.items():
            logger.info(f"Checking generic alerts from {instance}")

            for alert, error in alerts.items():
                logger.error(f"Collected generic alert {alert} with error {error.simplify()}")
                all_errors.append(str(error))

    if all_errors:
        alert_message = "; ".join(all_errors)
        return states.UNAVAILABLE_STATE, alert_message

    return states.FULLY_AVAILABLE_STATE


if __name__ == "__main__":
    main(run_check)
