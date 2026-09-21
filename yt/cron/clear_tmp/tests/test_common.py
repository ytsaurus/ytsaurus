from .conftest import yt_env, run_clear_tmp  # noqa

import yt.wrapper as yt

import pytest


COMMON_ARGS = [
    "--directory",
    "//tmp",
    "--account",
    "tmp",
    "--log-level",
    "debug",
    "--verbose",
]


def create_account_with_directory(client, account, disk_space, node_count, chunk_count):
    client.create("account", attributes={
        "name": account,
        "resource_limits": {
            "disk_space_per_medium": {"default": disk_space},
            "node_count": node_count,
            "chunk_count": chunk_count,
        },
    })
    directory = yt.ypath_join("//home", account)
    client.create("map_node", directory, attributes={"account": account})
    return directory


def test_directory_nodes_count_towards_quota(yt_env):  # noqa
    proxy_address = yt_env.yt_instance.get_proxy_address()
    client = yt_env.yt_client

    account = "directory_nodes"
    directory = create_account_with_directory(
        client, account, disk_space=1024 * 1024, node_count=8, chunk_count=10)
    tables = [yt.ypath_join(directory, f"table_{index}") for index in range(2)]
    for table in tables:
        client.create("table", table)

    args = [
        "--directory", directory,
        "--account", account,
        "--account-usage-ratio-save-total", "0.5",
        "--safe-age", "0",
        "--log-level", "debug",
        "--verbose",
    ]

    # Two tables and the home directory fit within the four-node cleanup limit.
    run_clear_tmp(proxy_address, args)
    for table in tables:
        assert client.exists(table)

    # Directories alone now exceed the cleanup limit, while the total of seven
    # nodes still fits within the account's eight-node creation limit.
    for index in range(4):
        client.create("map_node", yt.ypath_join(directory, f"dir_{index}"))

    run_clear_tmp(proxy_address, args)

    for table in tables:
        assert not client.exists(table)
    assert client.exists(directory)


def test_locked_node(yt_env):  # noqa
    proxy_address = yt_env.yt_instance.get_proxy_address()

    client = yt_env.yt_client

    client.create("map_node", "//tmp/dir")
    client.create("table", "//tmp/dir/table")

    with client.Transaction(timeout=60_000):
        client.lock("//tmp/dir/table", mode="exclusive")

        run_clear_tmp(
            proxy_address,
            COMMON_ARGS + [
                # The reason to remove.
                "--remove-empty",
                "--safe-age",
                "0",
            ])

    assert client.exists("//tmp/dir/table")

    run_clear_tmp(
        proxy_address,
        COMMON_ARGS + [
            # The reason to remove.
            "--remove-empty",
            "--safe-age",
            "0",
        ])

    assert not client.exists("//tmp/dir/table")


def test_empty_dir_removal(yt_env):  # noqa
    proxy_address = yt_env.yt_instance.get_proxy_address()

    client = yt_env.yt_client

    dir_path = "//tmp/dir/subdir/subsubdir"
    table_path = yt.ypath_join(dir_path, "table")
    client.create("map_node", dir_path, recursive=True)
    client.create("table", yt.ypath_join(dir_path, "table"))

    assert client.exists(table_path)

    run_clear_tmp(
        proxy_address,
        COMMON_ARGS + [
            # The reason to remove.
            "--remove-empty",
            "--safe-age",
            "0",
        ])

    assert not client.exists("//tmp/dir")

    # We avoid removing directory itself.
    assert client.exists("//tmp")


def test_dont_prune(yt_env):  # noqa
    proxy_address = yt_env.yt_instance.get_proxy_address()

    client = yt_env.yt_client

    dir_path = "//tmp/dir/subdir/subsubdir"
    table_path = yt.ypath_join(dir_path, "table")
    client.create("map_node", dir_path, recursive=True)
    client.create("table", yt.ypath_join(dir_path, "table"))
    client.set("//tmp/dir/@clear_tmp_config", {"dont_prune": True})

    assert client.exists(table_path)

    run_clear_tmp(
        proxy_address,
        COMMON_ARGS + [
            # The reason to remove.
            "--remove-empty",
            "--safe-age",
            "0",
        ])

    assert not client.exists("//tmp/dir/subdir")
    assert client.exists("//tmp/dir")


@pytest.mark.parametrize("white_list_args", [
    [],
    ["--dont-prune-white-list", "owner_a", "owner_b"],
    ["--dont-prune-white-list", "owner_a", "--dont-prune-white-list", "owner_b"],
])
def test_dont_prune_white_list(yt_env, white_list_args):  # noqa
    proxy_address = yt_env.yt_instance.get_proxy_address()
    client = yt_env.yt_client

    for owner in ("owner_a", "owner_b", "owner_c"):
        client.create("user", attributes={"name": owner})
        attributes = {"owner": owner, "clear_tmp_config": {"dont_prune": True}}
        client.create("map_node", f"//tmp/{owner}_dir", attributes=attributes)
        client.create("table", f"//tmp/{owner}_table", attributes=attributes)
        # An allowed owner alone does not protect a node or directory children.
        client.create("table", f"//tmp/{owner}_dir/child", attributes={"owner": owner})

    run_clear_tmp(
        proxy_address,
        COMMON_ARGS + ["--remove-empty", "--safe-age", "0"] + white_list_args)

    for owner in ("owner_a", "owner_b", "owner_c"):
        protected = not white_list_args or owner in ("owner_a", "owner_b")
        assert client.exists(f"//tmp/{owner}_table") == protected
        assert client.exists(f"//tmp/{owner}_dir") == protected
        assert not client.exists(f"//tmp/{owner}_dir/child")
