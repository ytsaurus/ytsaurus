import yt.packages.requests as requests


class TestClient:
    def test_http_health_check(self, example_env):
        client = example_env.create_client(transport="http")
        rsp = requests.get("http://{}/health_check".format(client.address))
        rsp.raise_for_status()
