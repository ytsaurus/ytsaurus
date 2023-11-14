from yt_odin_checks.lib.check_runner import main
try:
    from yt_odin_checks.lib.yandex_helpers import get_link
except ImportError:
    get_link = lambda cluster, check: ""  # noqa

import collections


def run_check(yt_client, logger, options, states):

    yt_batch_client = yt_client.create_batch_client()
    result = []
    for prefix in ("%02x" % num for num in range(256)):
        result.append(yt_batch_client.list("//sys/operations/{}".format(prefix), attributes=["authenticated_user"]))
    yt_batch_client.commit_batch()

    ops_per_user = collections.Counter()
    root_count = yt_client.get("//sys/operations/@count")
    children_node_count = root_count
    for request in result:
        ops = request.get_result()
        if not ops:
            continue
        children_node_count += len(ops)
        for operation in ops:
            ops_per_user[operation.attributes["authenticated_user"]] += 1

    state = states.FULLY_AVAILABLE_STATE, "{}/{} Operations count/Children node count".format(root_count, children_node_count)
    logger.info(state[1])
    state = state[0], "{} {}".format(
        state[1],
        get_link(options["cluster_name"], "operations_count"))
    logger.info("Operations count threshold: {}, children node thresholds WARN/CRIT: {}/{}".format(
                options["operations_count_threshold"],
                options["recoursive_node_count_warn"],
                options["recoursive_node_count_crit"]))

    logger.info("Top 10 users by operation count: %s", ops_per_user.most_common(10))

    if root_count > options["operations_count_threshold"] or children_node_count > options["recoursive_node_count_crit"]:
        return states.UNAVAILABLE_STATE, state[1]
    if children_node_count > options["recoursive_node_count_warn"]:
        return states.PARTIALLY_AVAILABLE_STATE, state[1]

    return state


if __name__ == "__main__":
    main(run_check)
