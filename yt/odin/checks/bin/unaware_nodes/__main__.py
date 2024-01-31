from yt_odin_checks.lib.check_runner import main


def find_unaware_nodes(yt_client, node_flavors_to_check):
    result = []
    for row in yt_client.list("//sys/cluster_nodes", attributes=["rack", "flavors"], read_from="cache"):
        node_flavors = row.attributes.get("flavors", [])

        if node_flavors_to_check:
            check_node_rack = False
            for flavor in node_flavors:
                if flavor in node_flavors_to_check:
                    check_node_rack = True
                    break
            if not check_node_rack and node_flavors:
                continue

        rack = row.attributes.get("rack", None)
        if rack is None:
            result.append(str(row))
    return result


def run_check(yt_client, logger, options, states):
    allow_unaware_nodes = options.get("allow_unaware_nodes", False)
    if allow_unaware_nodes:
        return states.FULLY_AVAILABLE_STATE

    node_flavors_to_check = options.get("node_flavors_to_check", [])

    unaware_nodes = find_unaware_nodes(yt_client, node_flavors_to_check)
    if unaware_nodes:
        logger.info("First ten unaware nodes: {}".format(" ".join(unaware_nodes[:10])))
        return states.UNAVAILABLE_STATE, len(unaware_nodes)
    else:
        return states.FULLY_AVAILABLE_STATE


if __name__ == "__main__":
    main(run_check)
