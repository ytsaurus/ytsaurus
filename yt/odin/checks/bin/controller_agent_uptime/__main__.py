from yt_odin_checks.lib.check_runner import main

from yt.common import join_exceptions

import yt.wrapper as yt


class InfoIsMissing(object):
    pass


INFO_IS_MISSING = InfoIsMissing()


def run_check(yt_client, logger, options, states):
    CONTROLLER_AGENT_UPTIME_PATH = "//sys/admin/odin/controller_agent_uptime"

    # COMPAT(ignat): remove after 21.1
    try:
        scheduler_version = yt_client.get("//sys/scheduler/orchid/service/version")
        logger.info("Scheduler has version %s", scheduler_version)
    except join_exceptions(yt.YtError, yt.get_retriable_errors()):
        return states.PARTIALLY_AVAILABLE_STATE, "Failed to get scheduler version"

    agent_to_info = {}
    for agent in yt_client.list("//sys/controller_agents/instances", attributes=["connection_time", "maintenance", "tags"]):
        if "default" not in agent.attributes.get("tags", []):
            logger.info("%s does not have the 'default' tag", str(agent))
            continue
        connection_time = agent.attributes.get("connection_time")
        is_under_maintenance = agent.attributes.get("maintenance", False)
        try:
            controller_agent_service_info = yt_client.get(
                "//sys/controller_agents/instances/{}/orchid/controller_agent/service".format(str(agent)))
        except yt.YtResponseError as err:
            if err.is_resolve_error():
                controller_agent_service_info = None
            else:
                controller_agent_service_info = INFO_IS_MISSING
        except Exception:
            controller_agent_service_info = INFO_IS_MISSING

        if controller_agent_service_info is not None and controller_agent_service_info is not INFO_IS_MISSING:
            info = {
                "connection_time": controller_agent_service_info["last_connection_time"],
                "version": controller_agent_service_info["controller_agent_version"],
                "scheduler_version": controller_agent_service_info["scheduler_version"],
                "maintenance": is_under_maintenance,
            }
            logger.info("Agent %s info: %s", str(agent), str(info))
            agent_to_info[str(agent)] = info
        # COMPAT(ignat): remove after 21.1
        elif controller_agent_service_info is None:
            if connection_time is not None:
                logger.info("Agent %s connected at %s", agent, connection_time)
            else:
                logger.info("Agent %s has no connection_time attribute", agent)

            try:
                controller_agent_version = yt_client.get("//sys/controller_agents/instances/{}/orchid/service/version"
                                                         .format(str(agent)))
                logger.info("Agent %s has version %s", agent, controller_agent_version)
            except yt.YtError:
                controller_agent_version = None
                logger.info("Failed to get controller agent version")

            agent_to_info[str(agent)] = {
                "connection_time": connection_time,
                "version": controller_agent_version,
                "scheduler_version": scheduler_version,
                "maintenance": is_under_maintenance,
            }

    try:
        last_seen_agent_to_info = yt_client.get(CONTROLLER_AGENT_UPTIME_PATH)
    except yt.YtResponseError as err:
        if err.is_resolve_error():
            last_seen_agent_to_info = {}
            yt_client.create("document", CONTROLLER_AGENT_UPTIME_PATH, attributes={"value": {}})
        else:
            raise

    reconnected_agents = []
    for agent, last_seen_info in last_seen_agent_to_info.items():
        if agent not in agent_to_info:
            continue

        was_under_maintenance = last_seen_info.get("maintenance", False)
        current_info = agent_to_info[agent]
        if current_info is INFO_IS_MISSING:
            logger.info("Skip agent %s info update since it is unavailable", agent)
            continue

        if current_info["version"] is None or current_info["connection_time"] is None:
            logger.info("Skip agent %s info update", agent)
            agent_to_info[agent] = last_seen_info
            continue

        if current_info["connection_time"] != last_seen_info["connection_time"]:
            controller_agent_version_change = ""
            if current_info["version"] != last_seen_info["version"]:
                controller_agent_version_change = "{} -> {}".format(last_seen_info["version"], current_info["version"])

            scheduler_version_change = ""
            if current_info["scheduler_version"] != last_seen_info["scheduler_version"]:
                scheduler_version_change = "{} -> {}".format(
                    last_seen_info["scheduler_version"],
                    current_info["scheduler_version"])

            if controller_agent_version_change or scheduler_version_change:
                version_change = "(" + ", ".join(filter(None, [controller_agent_version_change, scheduler_version_change])) + ")"
                logger.info("Agent %s has reconnected at %s, but version changed %s",
                            agent,
                            current_info["connection_time"],
                            version_change)
            elif was_under_maintenance:
                logger.info("Agent %s has reconnected at %s, but was under maintenance",
                            agent,
                            current_info["connection_time"])
            else:
                reconnected_agents.append(agent)
                logger.info("Agent %s has reconnected at %s", agent, current_info["connection_time"])

    yt_client.set(CONTROLLER_AGENT_UPTIME_PATH, agent_to_info)

    if reconnected_agents:
        return states.UNAVAILABLE_STATE, "Agent(s) {} has reconnected".format(", ".join(reconnected_agents))

    return states.FULLY_AVAILABLE_STATE


if __name__ == "__main__":
    main(run_check)
