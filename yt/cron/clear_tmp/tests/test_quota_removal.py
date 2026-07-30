from .conftest import yt_env, run_clear_tmp  # noqa


COMMON_ARGS = [
    "--directory",
    "//tmp",
    "--account",
    "tmp",
    "--log-level",
    "debug",
    "--verbose",
]


def test_locked_files_block_quota_removal(yt_env):  # noqa
    """Snapshot-locked file pushes node count over quota.
    The non-locked file must still be removed to satisfy the limit.
    """
    proxy_address = yt_env.yt_instance.get_proxy_address()
    client = yt_env.yt_client

    client.create("map_node", "//tmp/dir")
    client.create("table", "//tmp/dir/z_locked")
    client.create("table", "//tmp/dir/a_removable")

    with client.Transaction(timeout=60_000):
        client.lock("//tmp/dir/z_locked", mode="snapshot")

        run_clear_tmp(
            proxy_address,
            COMMON_ARGS + [
                "--max-node-count", "1",
                "--safe-age", "0",
                "--do-not-remove-objects-with-locks",
            ])

    assert client.exists("//tmp/dir/z_locked"), "locked file must be kept"
    assert not client.exists("//tmp/dir/a_removable"), "non-locked file must be removed to satisfy quota"


def test_dont_prune_blocks_quota_removal(yt_env):  # noqa
    """dont_prune file pushes node count over quota.
    The removable file must still be removed.
    """
    proxy_address = yt_env.yt_instance.get_proxy_address()
    client = yt_env.yt_client

    client.create("map_node", "//tmp/dir")
    client.create("table", "//tmp/dir/z_protected")
    client.set("//tmp/dir/z_protected/@clear_tmp_config", {"dont_prune": True})
    client.create("table", "//tmp/dir/a_removable")

    run_clear_tmp(
        proxy_address,
        COMMON_ARGS + [
            "--max-node-count", "1",
            "--safe-age", "0",
        ])

    assert client.exists("//tmp/dir/z_protected"), "dont_prune file must be kept"
    assert not client.exists("//tmp/dir/a_removable"), "removable file must be removed to satisfy quota"


def test_mixed_non_deletable_block_removal(yt_env):  # noqa
    """Both a locked file and a dont_prune file push quota over the limit.
    The single removable candidate must be removed.
    """
    proxy_address = yt_env.yt_instance.get_proxy_address()
    client = yt_env.yt_client

    client.create("map_node", "//tmp/dir")
    client.create("table", "//tmp/dir/z_locked")
    client.create("table", "//tmp/dir/y_protected")
    client.set("//tmp/dir/y_protected/@clear_tmp_config", {"dont_prune": True})
    client.create("table", "//tmp/dir/a_removable")

    with client.Transaction(timeout=60_000):
        client.lock("//tmp/dir/z_locked", mode="snapshot")

        run_clear_tmp(
            proxy_address,
            COMMON_ARGS + [
                "--max-node-count", "2",
                "--safe-age", "0",
                "--do-not-remove-objects-with-locks",
            ])

    assert client.exists("//tmp/dir/z_locked"), "locked file must be kept"
    assert client.exists("//tmp/dir/y_protected"), "dont_prune file must be kept"
    assert not client.exists("//tmp/dir/a_removable"), "removable file must be removed to satisfy quota"


def test_quota_removal_without_non_deletable(yt_env):  # noqa
    """Standard quota removal without any non-deletable files.
    The oldest file exceeding the quota must be removed.
    """
    proxy_address = yt_env.yt_instance.get_proxy_address()
    client = yt_env.yt_client

    client.create("map_node", "//tmp/dir")
    client.create("table", "//tmp/dir/c_file")
    client.create("table", "//tmp/dir/b_file")
    client.create("table", "//tmp/dir/a_file")

    run_clear_tmp(
        proxy_address,
        COMMON_ARGS + [
            "--max-node-count", "2",
            "--safe-age", "0",
        ])

    assert not client.exists("//tmp/dir/c_file"), "oldest file must be removed"
    assert client.exists("//tmp/dir/b_file"), "middle file must be kept"
    assert client.exists("//tmp/dir/a_file"), "youngest file must be kept"


def test_locked_within_quota_no_removal(yt_env):  # noqa
    """Locked files present but total count is within quota.
    Nothing should be removed.
    """
    proxy_address = yt_env.yt_instance.get_proxy_address()
    client = yt_env.yt_client

    client.create("map_node", "//tmp/dir")
    client.create("table", "//tmp/dir/z_locked")
    client.create("table", "//tmp/dir/a_file")

    with client.Transaction(timeout=60_000):
        client.lock("//tmp/dir/z_locked", mode="snapshot")

        run_clear_tmp(
            proxy_address,
            COMMON_ARGS + [
                "--max-node-count", "3",
                "--safe-age", "0",
                "--do-not-remove-objects-with-locks",
            ])

    assert client.exists("//tmp/dir/a_file"), "file must be kept when within quota"
    assert client.exists("//tmp/dir/z_locked"), "locked file must be kept"


def test_per_user_quota_contributes_to_global_quota(yt_env):  # noqa
    proxy_address = yt_env.yt_instance.get_proxy_address()
    client = yt_env.yt_client

    client.create("map_node", "//tmp/dir")

    client.create("table", "//tmp/dir/root_oldest")

    client.create("user", attributes={"name": "user_a"})

    client.create("table", "//tmp/dir/a_file1")
    client.set("//tmp/dir/a_file1/@owner", "user_a")

    client.create("table", "//tmp/dir/a_file2")
    client.set("//tmp/dir/a_file2/@owner", "user_a")

    client.create("table", "//tmp/dir/a_file3")
    client.set("//tmp/dir/a_file3/@owner", "user_a")

    run_clear_tmp(
        proxy_address,
        COMMON_ARGS + [
            "--max-node-count", "3",
            "--max-node-count-per-owner", "2",
            "--safe-age", "0",
        ])

    assert not client.exists("//tmp/dir/a_file1"), \
        "user_a oldest file must be removed (per-owner quota exceeded)"
    assert not client.exists("//tmp/dir/root_oldest"), \
        "root file must be removed (global quota exceeded after counting user_a's per-owner violation)"
    assert client.exists("//tmp/dir/a_file2"), "user_a younger file must be kept"
    assert client.exists("//tmp/dir/a_file3"), "user_a youngest file must be kept"
