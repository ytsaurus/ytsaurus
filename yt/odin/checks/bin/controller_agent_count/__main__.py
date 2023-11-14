from yt_odin_checks.lib.check_runner import main

from yt.common import join_exceptions

import yt.wrapper as yt


def run_check(yt_client, logger, options, states):
    connected_agent_count = 0
    disconnected_agents = []
    agent_instances = yt_client.list("//sys/controller_agents/instances")
    logger.info("Checking instances %r", agent_instances)

    yt_client_no_retries = yt.YtClient(config=yt_client.config)
    yt_client_no_retries.config["proxy"]["retries"]["count"] = 1
    for agent in agent_instances:
        connected = False
        try:
            connected = yt_client_no_retries.get("//sys/controller_agents/instances/{}/orchid/controller_agent/service/connected".format(agent))
        except join_exceptions(yt.YtError, yt.get_retriable_errors()):
            logger.exception("Failed to check agent %s connectivity in orchid", agent)
            pass
        if connected:
            logger.info("Agent %s is connected", agent)
            connected_agent_count += 1
        else:
            logger.info("Agent %s is not connected", agent)
            disconnected_agents.append(agent)

    threshold = max(1, len(agent_instances) - 1)
    if connected_agent_count < threshold:
        return states.UNAVAILABLE_STATE, "Agent(s) {} has disconnected".format(", ".join(disconnected_agents))

    return states.FULLY_AVAILABLE_STATE


if __name__ == "__main__":
    main(run_check)
