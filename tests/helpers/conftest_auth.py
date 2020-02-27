from .conftest import *

from yp.tests.blackbox_recipe.lib import fake_blackbox, fake_tvm


AUTH_YP_MASTER_CONFIG = {
    "authentication_manager": {
        "require_authentication": True,
        "tvm_service": {"host": "localhost", "port": None, "token": None, "src": "yp",},
        "blackbox_service": {"host": "localhost", "port": None, "secure": False, "use_tvm": True,},
        "blackbox_token_authenticator": {"scope": "yp:api",},
    },
}

TEST_OAUTH_TOKEN = "MyOauthToken"

AUTH_YP_CLIENT_CONFIG = {
    "token": TEST_OAUTH_TOKEN,
}

BLACKBOX_OAUTH_REQUEST_TEMPLATE = (
    "method=oauth&oauth_token={}&userip=%3A%3A1&attributes=1008&format=json"
)

BLACKBOX_OAUTH_RESPONSE = {
    "status": {"value": "VALID", "id": 0,},
    "attributes": {"1008": "root",},
    "oauth": {"client_id": "deadbeef", "client_name": "Mr. R00t", "scope": "yp:api",},
    "error": "OK",
}


def _make_auth_yp_master_config():
    yp_master_config = copy.deepcopy(AUTH_YP_MASTER_CONFIG)
    auth_config = yp_master_config["authentication_manager"]
    auth_config["tvm_service"]["port"] = fake_tvm.get_port()
    auth_config["tvm_service"]["token"] = fake_tvm.get_token()
    auth_config["blackbox_service"]["port"] = fake_blackbox.get_port()
    return yp_master_config


@pytest.fixture(scope="class")
def set_bb_response(request):
    def _set(bb_request, bb_response):
        fake_blackbox.set_response(bb_request, bb_response)
        request.addfinalizer(lambda: fake_blackbox.delete_response(bb_request))

    return _set


@pytest.fixture(scope="class")
def test_environment_auth(request, set_bb_response):
    assert fake_tvm.initialized()
    assert fake_blackbox.initialized()

    req = BLACKBOX_OAUTH_REQUEST_TEMPLATE.format(TEST_OAUTH_TOKEN)
    set_bb_response(req, BLACKBOX_OAUTH_RESPONSE)

    yp_master_config = update(
        _make_auth_yp_master_config(), getattr(request.cls, "YP_MASTER_CONFIG", None)
    )
    yp_client_config = update(AUTH_YP_CLIENT_CONFIG, getattr(request.cls, "YP_CLIENT_CONFIG", None))
    environment = YpTestEnvironment(
        yp_master_config=yp_master_config,
        yp_client_config=yp_client_config,
        enable_ssl=getattr(request.cls, "ENABLE_SSL", False),
        local_yt_options=getattr(request.cls, "LOCAL_YT_OPTIONS", None),
        start=getattr(request.cls, "START", True),
        start_yp_heavy_scheduler=getattr(request.cls, "START_YP_HEAVY_SCHEDULER", False),
        yp_heavy_scheduler_config=getattr(request.cls, "YP_HEAVY_SCHEDULER_CONFIG", None),
    )
    request.addfinalizer(lambda: environment.cleanup())
    return environment


@pytest.fixture(scope="function")
def yp_env_auth(request, test_environment_auth):
    test_method_setup(test_environment_auth)
    if not getattr(request.cls, "NO_TEARDOWN", False):
        request.addfinalizer(lambda: test_method_teardown(test_environment_auth))
    return test_environment_auth
