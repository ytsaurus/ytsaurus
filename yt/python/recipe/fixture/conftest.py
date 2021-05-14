import pytest
from yt.python.recipe.lib import get_yt_cluster


@pytest.fixture(scope="session")
def yt_cluster(request):
    return get_yt_cluster()
