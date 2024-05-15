from string import Template

import pytest

import yt.wrapper as yt
from yt.testlib import authors


PROGRAM = Template("""
class Mapper:
    def __call__(self, input_row):
        yield input_row


from yt.wrapper.client import YtClient
yt_cli = YtClient(proxy="$yt_proxy")
yt_cli.write_table("//tmp/foo", [{"x": 1}])
yt_cli.run_map(Mapper(), "//tmp/foo", "//tmp/foo")
""")


@pytest.mark.usefixtures("yt_env_v4")
class TestJupyter:
    @authors("dmifedorov")
    async def test_run_class_based_mapper(self, jp_start_kernel):
        try:
            import pytest_jupyter  # noqa
        except ImportError:
            pytest.skip("pytest_jupyter not installed")

        km, kc = await jp_start_kernel()
        assert km.kernel_name == "python3"
        proxy_url = yt.config.config["proxy"]["url"]
        script_to_run = PROGRAM.substitute(yt_proxy=proxy_url)
        msg = await kc.execute(script_to_run, reply=True)
        assert msg["content"]["status"] == "ok"
