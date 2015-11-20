from yt.common import get_value
import pytest

def pytest_addoption(parser):
    parser.addoption("--url", help="Test Transfer Manager backend url")

@pytest.fixture(scope="module")
def backend_url(request):
    url = request.config.getoption("--url")
    return get_value(url, "http://localhost:6000")
