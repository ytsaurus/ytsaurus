from yt.environment.arcadia_interop import arcadia_interop

import errno
import pytest


@pytest.mark.parametrize("cross_device", [False, True])
def test_sudo_wrapper_makes_tools_binary_available_without_host_symlink(tmp_path, monkeypatch, cross_device):
    build_dir = tmp_path / "build"
    build_dir.mkdir()
    ytserver_all = build_dir / "ytserver-all"
    ytserver_all.write_bytes(b"test binary")
    ytserver_all.chmod(0o755)

    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    (bin_dir / "ytserver-tools").symlink_to(ytserver_all)

    monkeypatch.setattr(
        arcadia_interop,
        "search_binary_path",
        lambda binary_name, binary_root: str(tmp_path / "yt-sudo-fixup"),
    )
    if cross_device:
        def reject_hard_link(source, target):
            raise OSError(errno.EXDEV, "Cross-device link")

        monkeypatch.setattr(arcadia_interop.os, "link", reject_hard_link)

    arcadia_interop.insert_sudo_wrapper(str(bin_dir), str(tmp_path))

    tools_binary = bin_dir / ".real" / "ytserver-tools"
    assert tools_binary.is_file()
    assert not tools_binary.is_symlink()
    assert tools_binary.read_bytes() == ytserver_all.read_bytes()
    assert tools_binary.stat().st_mode & 0o111


def test_sudo_wrapper_does_not_wrap_executor(tmp_path, monkeypatch):
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()

    binaries = ["ytserver-exec", "ytserver-job-proxy", "ytserver-tools"]
    for binary in binaries:
        (bin_dir / binary).write_text(binary)

    sudo_fixup = tmp_path / "yt-sudo-fixup"
    monkeypatch.setattr(
        arcadia_interop,
        "search_binary_path",
        lambda binary_name, binary_root: str(sudo_fixup),
    )

    arcadia_interop.insert_sudo_wrapper(str(bin_dir), str(tmp_path))

    assert (bin_dir / "ytserver-exec").read_text() == "ytserver-exec"
    assert not (bin_dir / "ytserver-exec.orig").exists()

    for binary in ["ytserver-job-proxy", "ytserver-tools"]:
        assert (bin_dir / f"{binary}.orig").read_text() == binary
        wrapper = (bin_dir / binary).read_text()
        assert wrapper.startswith("#!/bin/sh")
        assert str(sudo_fixup) in wrapper
