def scheduler_orchid_path():
    return "//sys/scheduler/orchid"


def scheduler_orchid_pool_tree_path(tree):
    return scheduler_orchid_path() + "/scheduler/scheduling_info_per_pool_tree/{}/fair_share_info".format(tree)


def scheduler_orchid_default_pool_tree_path():
    return scheduler_orchid_pool_tree_path("default")


def scheduler_orchid_pool_tree_config_path(tree):
    return scheduler_orchid_path() + "/scheduler/scheduling_info_per_pool_tree/{}/config".format(tree)


def scheduler_orchid_default_pool_tree_config_path():
    return scheduler_orchid_pool_tree_config_path("default")


def scheduler_orchid_pool_path(pool, tree="default"):
    return scheduler_orchid_pool_tree_path(tree) + "/pools/{}".format(pool)


def scheduler_orchid_operation_path(op, tree="default"):
    return scheduler_orchid_pool_tree_path(tree) + "/operations/{}".format(op)


def scheduler_orchid_node_path(node):
    return scheduler_orchid_path() + "/scheduler/nodes/{}".format(node)
