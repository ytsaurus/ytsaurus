import tempfile

import pytest

from .conftest import authors

import yt.environment.arcadia_interop as arcadia_interop

from yt.wrapper import respawn_in_docker
from yt.wrapper.py_wrapper import DockerRespawner


@authors("thenno")
def test_docker_respawner(monkeypatch):
    if arcadia_interop.yatest_common is not None:
        pytest.skip("There are other YT_* variables in the environment")
    monkeypatch.setenv("YT_SOME_KEY", "YT_SOME_VALUE")
    # Env var without YT_ prefix should be skipped.
    monkeypatch.setenv("SOME_KEY", "SOME_VALUE")
    respawner = DockerRespawner(
        image="some_image",
        target_platform="arm64",
        docker="docker_test",
        python="/custom/docker/python/path",
        env=None,
        main_scipt_path="/home/user2/yt/main.py",
        cwd="/root",
        homedir="/home/user",
        python_lib_paths=[
            "/usr/lib/python4.2/site-packages",
            # Should be skipped, because these directories are in homedir.
            "/home/user/.venv/",
        ],
    )
    assert respawner.make_command() == [
        "docker_test",
        "run",
        "--platform", "arm64",
        "-it",
        "--rm",
        "-e", "CWD=/root",
        "-e", "PYTHONPATH=/usr/lib/python4.2/site-packages:/home/user/.venv/",
        "-e", "YT_RESPAWNED_IN_CONTAINER=1",
        "-e", "YT_SOME_KEY=YT_SOME_VALUE",
        "-e", "YT_BASE_LAYER=some_image",
        # Specifying user's homedir.
        "-v", "/home/user:/home/user",
        # Specifying main script's dir.
        "-v", "/home/user2/yt:/home/user2/yt",
        # Specifying cwd.
        "-v", "/root:/root",
        # A part of PATHONPATH outside homedir.
        "-v", "/usr/lib/python4.2/site-packages:/usr/lib/python4.2/site-packages",
        "some_image",
        "/custom/docker/python/path", "/home/user2/yt/main.py",
    ]


@authors("thenno")
def test_docker_respawner_user_overrides():
    respawner = DockerRespawner(
        image="some_image",
        target_platform="arm64",
        docker="docker_test",
        python="python3",
        env={},
        main_scipt_path="/home/user/yt/main.py",
        cwd="/home/user/yt/lib",
        homedir="/root",
        python_lib_paths=[
            # All paths should be skipped.
            "/usr/lib/python4.2/site-packages",
            "/root/python/site-packages",
            "/home/user/.venv/",
        ],
        mount=[
            "/custom/path",
            "/etc/custom/path2",
        ],
    )
    assert respawner.make_command() == [
        "docker_test",
        "run",
        "--platform", "arm64",
        "-it",
        "--rm",
        "-e", "CWD=/home/user/yt/lib",
        "-e", "PYTHONPATH=/usr/lib/python4.2/site-packages:/root/python/site-packages:/home/user/.venv/",
        "-e", "YT_RESPAWNED_IN_CONTAINER=1",
        "-e", "YT_BASE_LAYER=some_image",
        # There are only paths from mount param.
        "-v", "/custom/path:/custom/path",
        "-v", "/etc/custom/path2:/etc/custom/path2",
        "some_image",
        "python3", "/home/user/yt/main.py",
    ]


@authors("thenno")
def test_docker_respawner_with_sudo():
    respawner_withour_sudo = DockerRespawner(
        image="some_image",
        target_platform="arm64",
        docker="docker_test",
        python="python3",
        env={},
        main_scipt_path="/home/user2/yt/main.py",
        cwd="/root",
        homedir="/home/user",
        python_lib_paths=[],
    )
    assert respawner_withour_sudo.make_command()[0] != "sudo"
    respawner_with_sudo = DockerRespawner(
        image="some_image",
        target_platform="arm64",
        docker="docker_test",
        python="python3",
        env={},
        main_scipt_path="/home/user2/yt/main.py",
        cwd="/root",
        homedir="/home/user",
        python_lib_paths=[],
        need_sudo=True,
    )
    assert respawner_with_sudo.make_command()[0] == "sudo"


@authors("thenno")
def test_docker_respawner_escaping():
    respawner_withour_sudo = DockerRespawner(
        image="some image",
        target_platform="arm 64",
        docker="docker test",
        python="python\n3",
        env={
            "SOME STRANGE VARIABLE": "\nbla",
        },
        main_scipt_path="/home/user2/yt/main.py",
        cwd="/bla",
        homedir="/\t  bla",
        python_lib_paths=["/some path"],
    )
    assert respawner_withour_sudo.make_command() == [
        "'docker test'",
        "run",
        "--platform",
        "'arm 64'",
        "-it",
        "--rm",
        "-e",
        "CWD=/bla",
        "-e",
        "'PYTHONPATH=/some path'",
        "-e",
        "YT_RESPAWNED_IN_CONTAINER=1",
        "-e",
        "'SOME STRANGE VARIABLE=\nbla'",
        "-e",
        "'YT_BASE_LAYER=some image'",
        "-v",
        "'/\t  bla:/\t  bla'",
        "-v",
        "/bla:/bla",
        "-v",
        "/home/user2/yt:/home/user2/yt",
        "-v",
        "'/some path:/some path'",
        "'some image'",
        "'python\n3'",
        "/home/user2/yt/main.py",
    ]


@authors("thenno")
def test_respawn_in_docker(monkeypatch):
    # `docker = echo` allows us to avoid running real docker.
    @respawn_in_docker("some_image", docker="echo")
    def foo():
        return "main func"

    # We proxy stdin to the subprocess call,
    # so we have to mock it.
    # The standard solution (io.BytesIO) doesn't suit here
    # because BytesIO/StringIO doesn't have the fileno attribute,
    # but subprocess.Popen works with file descriptors (not paths).
    _, filename = tempfile.mkstemp()
    with open(filename, "w") as mock_stdin:
        monkeypatch.setattr("sys.stdin", mock_stdin)

        # Without env variables we try to respawn in docker
        # in this case our function returns None.
        assert foo() is None

        # Script "was" restarted in a container.
        monkeypatch.setenv("YT_RESPAWNED_IN_CONTAINER", "1")
        assert foo() == "main func"

        # Script "was" run on YT.
        monkeypatch.setenv("YT_RESPAWNED_IN_CONTAINER", None)
        monkeypatch.setenv("YT_FORBID_REQUESTS_FROM_JOB", "1")
        assert foo() == "main func"
