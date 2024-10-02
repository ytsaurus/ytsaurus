from yt.wrapper.common import generate_uuid

import yt.yson as yson

from yt.common import update

from yt.environment import arcadia_interop

from library.python.testing.recipe import set_env

import yatest.common

from abc import ABCMeta, abstractmethod
import argparse
import concurrent.futures
import json
import logging
import os


DEFAULT_ORM_MASTER_OPTIONS = {}
DEFAULT_LOCAL_YT_OPTIONS = {"http_proxy_count": 1}


class Recipe(object):
    __metaclass__ = ABCMeta

    def __init__(self, name):
        self.name = name
        self.logger = logging.getLogger("{}-recipe".format(self.name.lower()))
        self._configure_logger()
        self.args = None

    @abstractmethod
    def make_orm_instance(self, *args, **kwargs):
        raise NotImplementedError

    def get_package_dir(self):
        return self.args.package_dir

    def generate_destination_path(self):
        base_path = arcadia_interop.get_output_path()
        output_ram_drive_path = yatest.common.output_ram_drive_path()
        if output_ram_drive_path and base_path.startswith(output_ram_drive_path):
            self.logger.info("Using output path inside tmpfs (path: {})".format(base_path))
        return os.path.join(base_path, "yt_{}".format(generate_uuid()))

    def get_orm_master_path_file(self, index):
        return "{}_master_{}.path".format(self.name.lower(), index)

    def get_address_env_name(self, transport: str, is_secure=False, index=None):
        if transport not in ("grpc", "http"):
            raise f"Unknown transuort: {transport}"
        transport = transport.upper()

        if is_secure:
            security = "SECURE"
        else:
            security = "INSECURE"

        if index is not None:
            return f"{self.name}_MASTER_{transport}_{security}_ADDR_{index}"
        else:
            return f"{self.name}_MASTER_{transport}_{security}_ADDR"

    def get_config_path_index_env_name(self, index):
        return "{}_MASTER_CONFIG_PATH_{}".format(self.name, index)

    def get_config_path_env_name(self):
        return "{}_MASTER_CONFIG_PATH".format(self.name)

    def get_args_parser(self):
        parser = argparse.ArgumentParser(description="Local {}".format(self.name))
        parser.add_argument(
            "--outside-arcadia",
            action="store_true",
            help="Whether recipe is executed outside Arcadia",
        )
        parser.add_argument("--master-config", help="{} master config file".format(self.name))
        parser.add_argument(
            "--master-config-yson", help="{} master config in YSON format".format(self.name)
        )
        parser.add_argument("--yt-config", help="YT local config file")
        parser.add_argument("--yt-config-yson", help="YT local config file")
        parser.add_argument(
            "--count", help="Count of {} instances".format(self.name), type=int, default=1
        )
        parser.add_argument(
            "--master-count", help="Count of masters per instance", type=int, default=1
        )
        parser.add_argument("--package-dir", help="Use YT from package by given build path")
        parser.add_argument(
            "--startup-threads",
            help="Number of concurrently launched {} instances".format(self.name),
            type=int,
            default=1,
        )
        parser.add_argument(
            "--do-not-start-master",
            help="Do not start {} master, just prepare underlying YT environment and master configuration".format(
                self.name,
            ),
            dest="start_master",
            default=True,
            action="store_false",
        )
        return parser

    def _configure_logger(self):
        self.logger.setLevel(logging.DEBUG)
        formatter = logging.Formatter("%(asctime)s %(levelname)s\t%(message)s")
        stderr_handler = logging.StreamHandler()
        stderr_handler.setLevel(logging.DEBUG)
        stderr_handler.setFormatter(formatter)
        self.logger.addHandler(stderr_handler)

    def _start_orm(self, master_config, local_yt_options, port_locks_path, master_count):
        destination = self.generate_destination_path()

        instance = self.make_orm_instance(
            destination, master_config, local_yt_options, port_locks_path, master_count=master_count
        )
        # Explicit preparation allows to override package directory.
        instance.prepare_yt(self.get_package_dir())
        instance.prepare()
        if self.args.start_master:
            instance.start()
        else:
            instance.prepare_master_start()
        return destination, instance

    def _stop_orm(self, master_id):
        path = os.path.abspath(master_id)
        instance = self.make_orm_instance(path)
        instance.stop()

    def _parse_args(self, argv):
        return self.get_args_parser().parse_args(argv)

    def _config_from_file(self, config_path):
        if not config_path:
            return dict()

        with open(config_path, "r") as f:
            return json.loads(f.read())

    def start(self, argv):
        self.args = args = self._parse_args(argv)

        master_config = update(
            DEFAULT_ORM_MASTER_OPTIONS, self._config_from_file(args.master_config)
        )
        if args.master_config_yson:
            master_config = update(
                master_config, yson.loads(args.master_config_yson.encode("utf-8"))
            )

        local_yt_options = update(DEFAULT_LOCAL_YT_OPTIONS, self._config_from_file(args.yt_config))
        if args.yt_config_yson:
            local_yt_options = update(
                local_yt_options, yson.loads(args.yt_config_yson.encode("utf-8"))
            )

        pool = concurrent.futures.ThreadPoolExecutor(args.startup_threads)
        futures = []

        for index in range(args.count):
            self.logger.info("Starting {} #{}".format(self.name, index + 1))

            futures.append(
                pool.submit(
                    self._start_orm,
                    master_config=master_config,
                    local_yt_options=local_yt_options,
                    port_locks_path="ports",  # Common port locks path for all instances.
                    master_count=args.master_count,
                )
            )

        for index, future in enumerate(futures):
            index += 1
            orm_master_path, orm_master = future.result()
            ssl_enabled = orm_master.is_ssl_enabled()
            # Backward compatibility.
            if index == 1:
                set_env(
                    self.get_address_env_name("grpc", is_secure=False), orm_master.orm_client_grpc_address
                )
                set_env("YT_HTTP_PROXY_ADDR", orm_master.yt_instance.get_proxy_address())
                if ssl_enabled:
                    set_env(
                        self.get_address_env_name("grpc", is_secure=True),
                        orm_master.orm_client_secure_grpc_address,
                    )
                if orm_master.orm_client_http_address is not None:
                    set_env(
                        self.get_address_env_name("http", is_secure=False), orm_master.orm_client_http_address
                    )
                if orm_master.orm_client_secure_http_address is not None:
                    set_env(
                        self.get_address_env_name("http", is_secure=True), orm_master.orm_client_secure_http_address
                    )
                set_env(
                    self.get_config_path_env_name(),
                    orm_master.config_path,
                )

            set_env(
                self.get_address_env_name("grpc", is_secure=False, index=index),
                orm_master.orm_client_grpc_address,
            )
            set_env(
                "YT_HTTP_PROXY_ADDR_{}".format(index), orm_master.yt_instance.get_proxy_address()
            )
            if ssl_enabled:
                set_env(
                    self.get_address_env_name("grpc", is_secure=True, index=index),
                    orm_master.orm_client_secure_grpc_address,
                )
            if orm_master.orm_client_http_address is not None:
                set_env(
                    self.get_address_env_name("http", is_secure=False, index=index),
                    orm_master.orm_client_http_address
                )
            if orm_master.orm_client_secure_http_address is not None:
                set_env(
                    self.get_address_env_name("http", is_secure=True, index=index),
                    orm_master.orm_client_http_address
                )
            set_env(
                self.get_config_path_index_env_name(index),
                orm_master.config_path,
            )

            with open(self.get_orm_master_path_file(index), "w") as f:
                f.write(orm_master_path)

    def stop(self, argv):
        self.args = args = self._parse_args(argv)

        for index in range(1, args.count + 1):
            self.logger.info("Stopping {} #{}".format(self.name, index))
            try:
                path = self.get_orm_master_path_file(index)
                if os.path.exists(path):
                    with open(path) as f:
                        self._stop_orm(f.read())
            except Exception:
                self.logger.exception("Error stopping {} #{}".format(self.name, index))
