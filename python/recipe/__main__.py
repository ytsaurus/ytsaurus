from yp.local import YpInstance

from yt.environment import arcadia_interop

from yt.wrapper.common import generate_uuid

from yt.common import update

from library.python.testing.recipe import declare_recipe, set_env
import yatest.common

import argparse
import json
import os


DEFAULT_YP_MASTER_OPTIONS = {
    "address_resolver": {
        "localhost_fqdn": "127.0.0.1"
    },
}


DEFAULT_LOCAL_YT_OPTIONS = {
    "fqdn": "127.0.0.1",
    "http_proxy_count": 1,
}


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
    parser = argparse.ArgumentParser(description="Local YP-Master server")
    parser.add_argument(
        "--outside-arcadia",
        action="store_true",
        help="Whether recipe is executed outside Arcadia",
    )
    parser.add_argument("--master-config", help="YP-Master config file")
    parser.add_argument("--yt-config", help="YT config file")
    return parser.parse_args(argv)


def config_from_file(config_path):
    if not config_path:
        return dict()

    with open(config_path, "r") as f:
        return json.loads(f.read())


def start(argv):
    args = parse_args(argv)
    yp_master_path, yp_master = start_yp(
        inside_arcadia=not args.outside_arcadia,
        yp_master_config=update(DEFAULT_YP_MASTER_OPTIONS, config_from_file(args.master_config)),
        local_yt_options=update(DEFAULT_LOCAL_YT_OPTIONS, config_from_file(args.yt_config)),
        port_locks_path=None,
    )

    set_env("YP_MASTER_GRPC_SECURE_ADDR", yp_master.yp_client_secure_grpc_address)
    set_env("YP_MASTER_GRPC_INSECURE_ADDR", yp_master.yp_client_grpc_address)
    set_env("YT_HTTP_PROXY_ADDR", yp_master.yt_instance.get_proxy_address())

    with open("yp_master.path", "w") as f:
        f.write(yp_master_path)


def stop(argv):
    with open("yp_master.path") as f:
        stop_yp(f.read())


if __name__ == "__main__":
    declare_recipe(start, stop)
