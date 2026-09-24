from yt.environment.arcadia_interop import arcadia_interop


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
