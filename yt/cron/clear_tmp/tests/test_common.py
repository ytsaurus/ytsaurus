from .conftest import yt_env, run_clear_tmp  # noqa

import yt.wrapper as yt


COMMON_ARGS = [
    "--directory",
    "//tmp",
    "--account",
    "tmp",
    "--log-level",
    "debug",
    "--verbose",
]


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
