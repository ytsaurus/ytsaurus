from yt_odin_checks.lib.check_runner import main

import yt.wrapper as yt


def run_check(yt_client, logger, options, states):
    SCHEDULING_INFO_ORCHID_PATH = "//sys/scheduler/orchid/scheduler/scheduling_info_per_pool_tree"
    OPERATIONS_ORCHID_PATH = "//sys/scheduler/orchid/scheduler/operations"
    OUTPUT_LIMIT = 10  # Max number of operations to print in this check.
    EPS = 1e-9

    # This check persists its state on cluster.
    # State is number of continuous minutes during which operation's satisfaction
    # was below min satisfaction.
    state_path = options["state_path"]

    # Increased timeout since orchid is heavy.
    yt_client.config["proxy"]["request_timeout"] = 40000

    state_exists = False
    state = {}
    try:
        state = yt_client.get(state_path)
        state_exists = True
    except yt.YtResponseError as err:
        if not err.is_resolve_error():
            raise

    # COMPAT. Old version of check worked only for one fair-share tree and
    # state was a simple mapping "<operation id> -> <number>". Consider state empty
    # in that case.
    for value in state.values():
        if isinstance(value, int):
            state = {}
            break

    trees = set(yt_client.list(SCHEDULING_INFO_ORCHID_PATH))
    operations = set(yt_client.list(OPERATIONS_ORCHID_PATH))

    fair_share_info_per_pool_tree = {}
    for tree in trees:
        fair_share_info_per_pool_tree[tree] = yt_client.get(
            yt.ypath_join(SCHEDULING_INFO_ORCHID_PATH, tree, "fair_share_info", "operations"))

    # Removing finished operations from state.
    for operation in list(state):
        if operation not in operations:
            del state[operation]

    # Remove unexisting trees from state.
    for operation_state in state.values():
        for tree in list(operation_state):
            if tree not in trees:
                del operation_state[tree]

    for operation in operations:
        operation_state = state.setdefault(operation, {})  # Map "<tree> -> <unsatisfied minute count>".

        for tree in trees:
            if operation not in fair_share_info_per_pool_tree[tree]:
                # Operation was removed from tree, perhaps this tree was tentative.
                # Or operation just finished, nothing to do here.
                if tree in operation_state:
                    del operation_state[tree]

                continue

            info = fair_share_info_per_pool_tree[tree][operation]
            satisfaction_ratio, fair_share_ratio = info["satisfaction_ratio"], info["fair_share_ratio"]

            count = operation_state.get(tree, 0)

            # satisfaction_ratio can be less than zero, this case
            # is also considered here.
            if satisfaction_ratio < options["min_satisfaction_ratio"] and fair_share_ratio > EPS:
                count += 1
            else:
                count = 0

            operation_state[tree] = count

    unsatisfied_operations = []
    for operation, operation_state in state.items():
        for tree, count in operation_state.items():
            if count >= options["critical_unsatisfied_minute_count"]:
                unsatisfied_operations.append(operation)
                break

    if not state_exists:
        yt_client.create("document", state_path)

    yt_client.set(state_path, state)

    logger.info("Number of long unsatisfied running operations: %d", len(unsatisfied_operations))
    if len(unsatisfied_operations) > 0:
        for operation in unsatisfied_operations[:OUTPUT_LIMIT]:
            operation_state = state[operation]
            logger.info("Operation %s is unsatisfied for too long, unsatisfied minutes per tree", operation)
            for tree, count in operation_state.items():
                logger.info("    %s -> %d", tree, count)

        return states.UNAVAILABLE_STATE

    return states.FULLY_AVAILABLE_STATE


if __name__ == "__main__":
    main(run_check)
