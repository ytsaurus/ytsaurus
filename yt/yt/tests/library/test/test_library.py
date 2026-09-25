from yt_env_setup import YTEnvSetup, _YtrecipeToolsBinaryMount

from yt_commands import (
    authors, wait)

from time import sleep

import os
import porto
import pytest


@authors("pogorelov")
@pytest.mark.parametrize("link_fails", [False, True])
def test_ytrecipe_tools_binary_mount_restores_symlink(tmp_path, monkeypatch, link_fails):
    binary_path = tmp_path / "ytserver-all"
    binary_path.write_bytes(b"binary")
    bin_path = tmp_path / "bin"
    bin_path.mkdir()
    tools_path = bin_path / "ytserver-tools"
    tools_path.symlink_to(binary_path)

    class FakeVolume:
        path = "/fake/volume"
        destroyed = False

        def Destroy(self):
            self.destroyed = True

    volume = FakeVolume()

    class FakeConnection:
        def CreateVolume(self, **properties):
            assert properties == {"backend": "bind", "storage": str(binary_path), "read_only": "true"}
            return volume

        def LinkVolume(self, path, container, target, read_only):
            assert (path, container, target) == (volume.path, "self", str(tools_path))
            assert read_only
            assert tools_path.is_file() and not tools_path.is_symlink()
            if link_fails:
                raise RuntimeError("LinkVolume failed")

    monkeypatch.setattr(porto, "Connection", FakeConnection)

    mount = _YtrecipeToolsBinaryMount(str(bin_path))
    if link_fails:
        with pytest.raises(RuntimeError, match="LinkVolume failed"):
            mount.mount()
    else:
        mount.mount()
        assert not tools_path.is_symlink()
        mount.close()

    assert volume.destroyed
    assert tools_path.is_symlink()
    assert os.readlink(tools_path) == str(binary_path)


class TestYtTestLibrary(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 1

    @authors("lukyan")
    @pytest.mark.timeout(2)
    @pytest.mark.xfail
    def test_timeout_plugin(self):
        sleep(5)

    @authors("lukyan")
    @pytest.mark.xfail
    def test_wait(self):
        def predicate():
            pytest.fail("Test is definitely failed. We do not want to wait.")
        wait(predicate, ignore_exceptions=True)

    @authors("ni-stoiko")
    @pytest.mark.skip(reason="This test should fail in teardown")
    def test_check_disabled_locations(self):
        location_key_lists = [
            ["data_node", "cache_locations"],
            ["data_node", "volume_manager", "layer_locations"],
            ["data_node", "store_locations"],
            ["exec_agent", "slot_manager", "locations"],
            ["exec_node", "slot_manager", "locations"],
        ]
        location_disabled = False
        for node_config in self.Env.configs["node"]:
            for location_key_list in location_key_lists:
                try:
                    locations = self._walk_dictionary(node_config, location_key_list)
                    if locations:
                        for location in locations:
                            path = location["path"]
                            if not os.path.exists(path):
                                os.mkdir(path)
                            with open(f"{path}/disabled", "w") as file:
                                file.flush()
                            location_disabled = True
                except KeyError:
                    pass
        assert location_disabled, "Some locations must be disabled"
