from .conftest import yt_env, run_clear_tmp  # noqa


def create_portal(client, portal_path):
    portal_cell_tag_id = 2

    if client.exists(portal_path):
        client.remove(portal_path, recursive=True, force=True)

    entrance_id = client.create("portal_entrance", portal_path, attributes={"exit_cell_tag": portal_cell_tag_id})
    assert client.exists(f"//sys/portal_entrances/{entrance_id}")

    exit_id = client.get(portal_path + "&/@exit_node_id")
    assert client.get(f"#{exit_id}/@entrance_node_id") == entrance_id

    assert client.get(portal_path + "&/@type") == "portal_entrance"
    assert client.get(portal_path + "&/@path") == portal_path

    exit_id = client.get(portal_path + "&/@exit_node_id")
    assert client.get(f"#{exit_id}/@type") == "portal_exit"
    assert client.get(f"#{exit_id}/@inherit_acl")
    assert client.get(f"#{exit_id}/@path") == portal_path

    assert client.exists(f"//sys/portal_exits/{exit_id}")


def test_clear_tmp_location(yt_env):  # noqa
    client = yt_env.yt_client

    portal_path = "//tmp/yt_wrapper"
    create_portal(client, portal_path)

    client.create("map_node", portal_path + "/home")
    assert client.exists(portal_path + "/home")

    subdirs = 10
    for i in range(subdirs):
        client.create("table", portal_path + f"/{i}")
        assert client.exists(portal_path + f"/{i}")

    proxy_address = yt_env.yt_instance.get_proxy_address()
    run_clear_tmp(
        proxy_address,
        [
            "--directory",
            "//tmp",
            "--account",
            "tmp",
            "--do-not-remove-objects-with-other-account",
            "--account-usage-ratio-save-total",
            "0.25",
            "--account-usage-ratio-save-per-owner",
            "0.005",
            "--max-dir-node-count",
            f"{subdirs}",
            "--remove-batch-size",
            "500",
            "--log-level",
            "debug",
            "--verbose",
        ])

    assert not client.exists(portal_path + "/home")

    for i in range(subdirs):
        assert client.exists(portal_path + f"/{i}")

    assert client.exists(portal_path)
    assert client.get(portal_path + "&/@type") == "portal_entrance"
    assert client.get(portal_path + "&/@path") == portal_path
