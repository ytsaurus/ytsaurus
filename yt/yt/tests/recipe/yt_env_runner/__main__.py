from yt_env_setup import YTEnvSetup

from yt_recipe_common import (
    Pipe, PIPE_FROM_RECIPE_TO_RUNNER, PIPE_FROM_RUNNER_TO_RECIPE,
    get_config_path_from_args,
)

from yt.test_helpers import get_source_root

import yt.yson as yson

from library.python.testing.recipe import get_options, set_env

import os
import traceback
from typing import List


RECIPE_PIPE = Pipe(PIPE_FROM_RECIPE_TO_RUNNER, PIPE_FROM_RUNNER_TO_RECIPE)


class YTEnvRunner(YTEnvSetup):
    # To avoid "Request is missing credentials" error.
    USE_NATIVE_AUTH = False

    def setup(self) -> None:
        self.setup_class()
        self.setup_method(None)

    def teardown(self) -> None:
        self.teardown_method(None)
        self.teardown_class()


def run_yt_env(config_path: str) -> None:
    with open(config_path, "rb") as f:
        config: yson.YsonMap = yson.load(f)

    for k, v in config.items():
        assert k.isupper(), "All YT env options must be in upper case"
        assert hasattr(YTEnvRunner, k), f"Unknown yt env config option {k}"
        setattr(YTEnvRunner, k, v)

    # NB: this option affects YT env setup only. Tests still may use whatever
    # proxies/drivers they want.
    YTEnvRunner.DRIVER_BACKEND = "native"

    runner = YTEnvRunner()
    runner.setup()

    # Set environment variables.
    # NB: set_env(k, v) writes (k, v) into the special file instead of actually
    # setting environment variable so it's OK to call it in YT runner process.

    if runner.Env.yt_config.http_proxy_count > 0 and runner.ENABLE_HTTP_PROXY:
        set_env("YT_HTTP_PROXY_ADDRESS", runner.Env.get_http_proxy_address())
        set_env("YT_PROXY", runner.Env.get_http_proxy_address())
        set_env("YT_PROXY_URL_ALIASING_CONFIG", yson.dumps(yson.YsonMap({
            runner.Env._cluster_name: runner.Env.get_http_proxy_address()
        })).decode("ascii"))

    driver_backend = config.get("DRIVER_BACKEND", "native")
    if driver_backend == "native":
        set_env("YT_DRIVER_CONFIG_PATH", runner.Env.config_paths["driver"])
        set_env("YT_DRIVER_LOGGING_CONFIG_PATH", runner.Env.config_paths["driver_logging"])
    elif driver_backend == "rpc":
        set_env("YT_NATIVE_DRIVER_CONFIG_PATH", runner.Env.config_paths["driver"])
        set_env("YT_DRIVER_CONFIG_PATH", runner.Env.config_paths["rpc_driver"])
        set_env("YT_DRIVER_LOGGING_CONFIG_PATH", runner.Env.config_paths["driver_logging"])
    else:
        raise RuntimeError(f"Incorrect driver backend: {driver_backend}")

    RECIPE_PIPE.send("ready")
    RECIPE_PIPE.wait("stop")

    runner.teardown()
    RECIPE_PIPE.send("stopped")


def main(args: List[str]) -> None:
    try:
        config_path = os.path.join(get_source_root(), get_config_path_from_args(args))

        run_yt_env(config_path)
    except BaseException:
        # It's important to notify recipe process to avoid dead lock.
        tb = traceback.format_exc().replace("\n", "\\n")
        RECIPE_PIPE.send(f"failed: \"{tb}\"")


if __name__ == "__main__":
    # TODO(kvk1920): this ugly hack can be avoided.
    # yatest is needed here for 2 reasons:
    #   - get path to config
    #   - prepare environment variables for test
    # All of these can be done in recipe process, but it requires a bit more
    # complicated communication between recipe and runner.

    # Initialize yatest if needed.
    runner_args = get_options()[1]
    # Since these args were passed to recipe the first arg is either "start" or
    # "stop". Skip it.
    runner_args = runner_args[1:]
    main(runner_args)
