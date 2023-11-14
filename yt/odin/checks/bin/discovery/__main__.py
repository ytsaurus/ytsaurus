from yt_odin_checks.lib.check_runner import main


def run_check(yt_client, logger, options, states):
    discovery_servers = yt_client.list("//sys/discovery_servers")
    if len(discovery_servers) == 0:
        return states.UNAVAILABLE_STATE

    total_alive = 0
    for discovery_server in discovery_servers:
        try:
            yt_client.get("//sys/discovery_servers/{}/orchid".format(discovery_server))
            total_alive += 1
        except Exception as e:
            logger.exception(e)

    if total_alive == len(discovery_servers):
        return states.FULLY_AVAILABLE_STATE

    return states.PARTIALLY_AVAILABLE_STATE if float(total_alive) / len(discovery_servers) > 0.5 else states.UNAVAILABLE_STATE


if __name__ == "__main__":
    main(run_check)
