from yt_odin_checks.lib.check_runner import main

import yt.wrapper as yt


def run_check(yt_client, logger, options, states):
    agent_instances = yt_client.list("//sys/controller_agents/instances")
    logger.info("Checking instances %r", agent_instances)

    all_usages = []
    disconnected_agent_count = 0

    def format_usage(usage):
        return "{} - {} GB".format(usage[1], round(1.0 * usage[0] / (1024 ** 3), 3))

    yt_client_no_retries = yt.YtClient(config=yt_client.config)
    yt_client_no_retries.config["proxy"]["retries"]["count"] = 1
    for agent in agent_instances:
        orchid_path = "//sys/controller_agents/instances/{}/orchid/controller_agent/tagged_memory_statistics".format(agent)

        memory_statistics = None
        try:
            memory_statistics = yt_client_no_retries.get(orchid_path)
        except Exception:
            logger.exception("Failed to get statistics from agent")
            disconnected_agent_count += 1

        if memory_statistics is None:
            continue

        usages = []
        for info in memory_statistics:
            # COMPAT(ni-stoiko): Alive flag of tagged_memory_statistics will be deprecated since 23.2.
            # All operations will be alive.
            if not info["alive"]:
                continue
            usages.append((info["usage"], info["operation_id"]))
        usages.sort(reverse=True)

        all_usages += usages

        if usages:
            logger.info("Top operations for agent %s:\n  %s", agent, "\n  ".join(map(format_usage, usages[:5])))

    all_usages.sort(reverse=True)

    logger.info("Found %d operations in total", len(all_usages))

    large_operations = []
    index = 0
    while index < min(5, len(all_usages)):
        if all_usages[index][0] < options.get("memory_threshold", 20 * 1024 ** 3):
            break
        large_operations.append(format_usage(all_usages[index]))
        index += 1

    if large_operations:
        description = "Large operations: " + "; ".join(large_operations)
        logger.info(description)
        return states.UNAVAILABLE_STATE, description

    if disconnected_agent_count == 0:
        return states.FULLY_AVAILABLE_STATE
    else:
        return states.PARTIALLY_AVAILABLE_STATE


if __name__ == "__main__":
    main(run_check)
