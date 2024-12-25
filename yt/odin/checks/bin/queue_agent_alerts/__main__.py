from yt_odin_checks.lib.check_runner import main
from yt.wrapper import YtClient
from yt.common import YtError, YtResponseError

import yt.yson as yson

from collections import defaultdict

BANNED_QUEUE_AGENT_INSTANCE_ATTRIBUTE_NAME = "banned_queue_agent_instance"


def run_check(secrets, yt_client, logger, options, states):
    cluster_name = options["cluster_name"]
    queue_agent_stage_clusters = options["queue_agent_stage_clusters"]

    all_errors = []

    generic_alerts_by_stage_cluster = defaultdict(lambda: defaultdict(dict))

    # Assuming options["queue_agent_stage_clusters"] is a dictionary from cluster name to proxy address.
    for queue_agent_stage_cluster, queue_agent_stage_proxy in queue_agent_stage_clusters.items():
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
            proxy=queue_agent_stage_proxy,
            token=secrets["yt_token"],
            config={"proxy": {"retries": {"count": 1, "enable": False}}})

        queue_agent_instances = run_method(lambda: queue_agent_stage_client.list("//sys/queue_agents/instances", attributes=[BANNED_QUEUE_AGENT_INSTANCE_ATTRIBUTE_NAME]),
                                           null_value=[])

        queue_agent_instances_count = len(queue_agent_instances)
        failed_instance_count = 0
        banned_instance_count = 0
        instances_errors = []
        has_non_transport_error = False

        for yson_instance in queue_agent_instances:
            instance = str(yson_instance)
            if yson_instance.has_attributes() and BANNED_QUEUE_AGENT_INSTANCE_ATTRIBUTE_NAME in yson_instance.attributes:
                attribute_value = yson_instance.attributes[BANNED_QUEUE_AGENT_INSTANCE_ATTRIBUTE_NAME]
                # NB(apachee): Check type to match the behavior of queue agent sharding manager (it ignores anything except bool).
                if isinstance(attribute_value, yson.YsonBoolean) and attribute_value:
                    logger.info(f"Skipping collecting alerts from {instance}, since it is banned by {BANNED_QUEUE_AGENT_INSTANCE_ATTRIBUTE_NAME!r} attribute")
                    banned_instance_count += 1
                    instances_errors.append(YtError(f"Instance {instance} is banned"))
                    continue

            logger.info(f"Collecting alerts from {instance}")

            try:
                alerts = run_method(lambda: queue_agent_stage_client.get(f"//sys/queue_agents/instances/{instance}/orchid/alerts"),
                                    null_value=dict())
            except YtResponseError as err:
                if not err.is_transport_error():
                    has_non_transport_error = True
                failed_instance_count += 1
                wrapped_err = YtError(message=f"Failed to collect alerts from {instance}", inner_errors=[err])
                instances_errors.append(wrapped_err)
                continue

            for alert, raw_error in alerts.items():
                error = YtError.from_dict(raw_error)

                if not error.inner_errors:
                    # If there are no inner error, we consider this alert generic for the corresponding queue agent.
                    generic_alerts_by_stage_cluster[queue_agent_stage_cluster][instance][alert] = error

                for inner_error in error.inner_errors:
                    if inner_error.attributes.get("cluster", None) == cluster_name:
                        logger.error(f"Collected relevant alert {alert} with error {error.simplify()}")
                        logger.error(f"Relevant inner error: {inner_error.simplify()}")
                        all_errors.append(str(error.simplify()))
                    elif "cluster" not in inner_error.attributes:
                        # If there is at least one inner error without a specified cluster attribute,
                        # we consider this alert generic for the corresponding queue agent.
                        generic_alerts_by_stage_cluster[queue_agent_stage_cluster][instance][alert] = error

        # NB(apachee): These errors only make sense on the same cluster as Queue Agent instances.
        if queue_agent_stage_cluster == cluster_name:
            short_instances_info = f"(Failed: {failed_instance_count}, Banned: {banned_instance_count}, Total: {len(queue_agent_instances)})"
            if (banned_instance_count + failed_instance_count) * 2 > queue_agent_instances_count:
                raise YtError(f"More than half of all queue agent instances are not available {short_instances_info}",
                              inner_errors=instances_errors)
            elif has_non_transport_error:
                raise YtError(f"There are {failed_instance_count} failed instances with some of them having non-transport error {short_instances_info}",
                              inner_errors=instances_errors)
            elif failed_instance_count > 0:
                description = f"Some instances are not available {short_instances_info}"
                logger.error(description)
                return states.PARTIALLY_AVAILABLE_STATE, description
            else:
                logger.info(f"There are {failed_instance_count} failed instances and {banned_instance_count} banned instances out of {len(queue_agent_instances)} instances")

    if cluster_name in queue_agent_stage_clusters:
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
