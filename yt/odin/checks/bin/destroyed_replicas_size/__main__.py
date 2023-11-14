from yt_odin_checks.lib.check_runner import main


def run_check(yt_client, logger, options, states):
    nodes = yt_client.list("//sys/cluster_nodes", attributes=["destroyed_chunk_replica_count"])

    total = 0
    node_replicas = []
    per_node_threshold_exceeded = False
    for node in nodes:
        if "destroyed_chunk_replica_count" not in node.attributes:
            continue

        count = node.attributes["destroyed_chunk_replica_count"]
        if count > options['node_threshold']:
            per_node_threshold_exceeded = True
        total += count
        node_replicas.append((count, str(node)))

    logger.info("Total destroyed chunk replica count: {}".format(total))

    if per_node_threshold_exceeded or total > options['total_threshold']:
        node_replicas.sort(reverse=True)
        for count, node in node_replicas[:10]:
            logger.info("Address: {}, Count: {}".format(node, count))

        return states.UNAVAILABLE_STATE

    return states.FULLY_AVAILABLE_STATE


if __name__ == "__main__":
    main(run_check)
