from .conftest import authors

from yt.wrapper.py_wrapper import DockerRespawner, respawn_in_docker


@authors("thenno")
def test_docker_respawner():
    respawner = DockerRespawner(
        image="some_image",
        platform="arm64",
        docker_path="docker_test",
        env={
            "SOME_KEY": "SOME_VALUE",
            "YT_SOME_KEY": "YT_SOME_VALUE",
            "ANOTHER_SOME_KEY": "ANOTHER_SOME_VALUE",
            "YT_TEST_KEY": "YT_TEST_VALUE",
        },
        main_scipt_path="/home/user/yt/main.py",
        cwd="/home/user/yt/lib",
        homedir="/root",
        python_lib_paths=[
            "/usr/lib/python4.2/site-packages",
            "/root/python/site-packages",
            "/home/user/.venv/",
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
        "-e", "YT_SOME_KEY=YT_SOME_VALUE",
        "-e", "YT_TEST_KEY=YT_TEST_VALUE",
        "-e", "BASE_LAYER=some_image",
        "-v", "/home/user/.venv/:/home/user/.venv/",
        "-v", "/home/user/yt:/home/user/yt",
        "-v", "/home/user/yt/lib:/home/user/yt/lib",
        "-v", "/root:/root",
        "-v", "/usr/lib/python4.2/site-packages:/usr/lib/python4.2/site-packages",
        "some_image",
        "python3", "/home/user/yt/main.py",
    ]


@authors("thenno")
def test_docker_respawner_user_overrides():
    respawner = DockerRespawner(
        image="some_image",
        platform="arm64",
        docker_path="docker_test",
        env={
            "SOME_KEY": "SOME_VALUE",
            "YT_SOME_KEY": "YT_SOME_VALUE",
            "ANOTHER_SOME_KEY": "ANOTHER_SOME_VALUE",
            "YT_TEST_KEY": "YT_TEST_VALUE",
        },
        main_scipt_path="/home/user/yt/main.py",
        cwd="/home/user/yt/lib",
        homedir="/root",
        python_lib_paths=[
            "/usr/lib/python4.2/site-packages",
            "/root/python/site-packages",
            "/home/user/.venv/",
        ],
        mount=["/custom/path", "/etc/custom/path2"],
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
        "-e", "YT_SOME_KEY=YT_SOME_VALUE",
        "-e", "YT_TEST_KEY=YT_TEST_VALUE",
        "-e", "BASE_LAYER=some_image",
        "-v", "/custom/path:/custom/path",
        "-v", "/etc/custom/path2:/etc/custom/path2",
        "some_image",
        "python3", "/home/user/yt/main.py",
    ]


@authors("thenno")
def test_respawn_in_docker(monkeypatch):
    # `docker_path = echo` allows us to avoid running real docker
    @respawn_in_docker("some_image", docker_path="echo")
    def foo():
        return "main func"

    # without env variables we try to respawn in docker
    # in this case our function returns None
    assert foo() is None

    # script "was" restarted in a container
    monkeypatch.setenv("YT_RESPAWNED_IN_CONTAINER", "1")
    assert foo() == "main func"

    # script "was" run on YT
    monkeypatch.setenv("YT_RESPAWNED_IN_CONTAINER", None)
    monkeypatch.setenv("YT_FORBID_REQUESTS_FROM_JOB", "1")
    assert foo() == "main func"
