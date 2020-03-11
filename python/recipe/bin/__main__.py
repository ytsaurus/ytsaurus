from yp.local import YpInstance

from yt.environment import arcadia_interop

from yt.wrapper.common import generate_uuid

from yt.common import update

from library.python.testing.recipe import (
    declare_recipe,
    set_env,
)
import yatest.common

import argparse
import json
import logging
import os


DEFAULT_YP_MASTER_OPTIONS = {}

DEFAULT_LOCAL_YT_OPTIONS = {
    "http_proxy_count": 1,
}

logger = logging.getLogger("yp-recipe")


def configure_logger():
    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter("%(asctime)s %(levelname)s\t%(message)s")
    stderr_handler = logging.StreamHandler()
    stderr_handler.setLevel(logging.DEBUG)
    stderr_handler.setFormatter(formatter)
    logger.addHandler(stderr_handler)


def start_yp(inside_arcadia, yp_master_config, local_yt_options, port_locks_path):
    destination = os.path.join(yatest.common.output_path(), "yt_%s" % generate_uuid())
    os.makedirs(destination)
    path = arcadia_interop.prepare_yt_environment(destination, inside_arcadia=inside_arcadia)
    os.environ["PATH"] = os.pathsep.join([path, os.environ.get("PATH", "")])

    instance = YpInstance(
        destination,
        yp_master_config=yp_master_config,
        enable_ssl=True,
        local_yt_options=local_yt_options,
        port_locks_path=port_locks_path,
    )
    instance.start()
    return destination, instance


def stop_yp(master_id):
    path = os.path.abspath(master_id)
    instance = YpInstance(path)
    instance.stop()


def parse_args(argv):
    parser = argparse.ArgumentParser(description="Local YP")
    parser.add_argument(
        "--outside-arcadia", action="store_true", help="Whether recipe is executed outside Arcadia",
    )
    parser.add_argument("--master-config", help="YP Master config file")
    parser.add_argument("--yt-config", help="YT local config file")
    parser.add_argument("--count", help="Count of YP instances", type=int, default=1)
    return parser.parse_args(argv)


def config_from_file(config_path):
    if not config_path:
        return dict()

    with open(config_path, "r") as f:
        return json.loads(f.read())


def start(argv):
    configure_logger()

    args = parse_args(argv)

    yp_master_config = update(DEFAULT_YP_MASTER_OPTIONS, config_from_file(args.master_config))
    local_yt_options = update(DEFAULT_LOCAL_YT_OPTIONS, config_from_file(args.yt_config))

    for index in range(1, args.count + 1):
        logger.info("Starting YP #{}".format(index))

        yp_master_path, yp_master = start_yp(
            inside_arcadia=not args.outside_arcadia,
            yp_master_config=yp_master_config,
            local_yt_options=local_yt_options,
            port_locks_path="ports",  # Common port locks path for all instances.
        )

        # Backward compatibility.
        if index == 1:
            set_env("YP_MASTER_GRPC_SECURE_ADDR", yp_master.yp_client_secure_grpc_address)
            set_env("YP_MASTER_GRPC_INSECURE_ADDR", yp_master.yp_client_grpc_address)
            set_env("YT_HTTP_PROXY_ADDR", yp_master.yt_instance.get_proxy_address())

        set_env(
            "YP_MASTER_GRPC_SECURE_ADDR_{}".format(index), yp_master.yp_client_secure_grpc_address
        )
        set_env("YP_MASTER_GRPC_INSECURE_ADDR_{}".format(index), yp_master.yp_client_grpc_address)
        set_env("YT_HTTP_PROXY_ADDR_{}".format(index), yp_master.yt_instance.get_proxy_address())

        with open("yp_master_{}.path".format(index), "w") as f:
            f.write(yp_master_path)


def stop(argv):
    configure_logger()

    args = parse_args(argv)

    for index in range(1, args.count + 1):
        logger.info("Stopping YP #{}".format(index))
        try:
            with open("yp_master_{}.path".format(index)) as f:
                stop_yp(f.read())
        except Exception:
            logger.exception("Error stopping YP #{}".format(index))


if __name__ == "__main__":
    declare_recipe(start, stop)
