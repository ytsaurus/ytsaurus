import pytest
from _pytest.fixtures import FixtureLookupError

from .yt_stuff import YtStuff


@pytest.fixture(scope="module")
def yt_stuff(request):
    """Will start local YT and return mapreduce.yt.python.YtStuff object.
    You may define pytest fixture ``yt_config`` in your code to pass custom
    config to YtStuff contructor. Example:

        import pytest
        from mapreduce.yt.python.yt_stuff import YtConfig

        @pytest.fixture
        def yt_config(request):
            return YtConfig(node_count=12)

    To use yt_stuff you only need to add to your ya.make:
        PEERDIR(mapreduce/yt/python)
    or
        PEERDIR(mapreduce/yt/python/lite)
    The fixture will be imported by standard pytest mechanism
    https://docs.pytest.org/en/latest/fixture.html#conftest-py-sharing-fixture-functions
    """
    try:
        yt_config = request.getfixturevalue("yt_config")
    except FixtureLookupError:
        yt_config = None
    yt = YtStuff(yt_config)
    yt.start_local_yt()
    request.addfinalizer(yt.stop_local_yt)
    return yt
