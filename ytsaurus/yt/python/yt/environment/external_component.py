from .configs_provider import init_singletons, init_jaeger_collector, _init_logging

try:
    from yt.packages.six.moves import xrange
except ImportError:
    from six.moves import xrange


class ExternalComponent:
    LOWERCASE_NAME = None
    DASHED_NAME = None
    PLURAL_HUMAN_READABLE_NAME = None
    # If not set, component is expected to be hardlinked from ytserver-all.
    PATH = None

    def __init__(self, env, count):
        assert self.LOWERCASE_NAME is not None
        assert self.DASHED_NAME is not None
        assert self.PLURAL_HUMAN_READABLE_NAME

        self.env = env
        self.pids = []
        configs, addresses = self.build_configs(count, env.yt_config, env._cluster_configuration["cluster_connection"],
                                                env._open_port_iterator, env.logs_path)
        self.addresses = addresses
        self.config_paths = env.prepare_external_component(
            "ytserver-" + self.DASHED_NAME,
            self.LOWERCASE_NAME,
            self.PLURAL_HUMAN_READABLE_NAME,
            configs)

    @staticmethod
    def get_default_config():
        raise NotImplementedError("Override me in the derived class")

    def build_configs(self, count, yt_config, cluster_connection, ports_generator, logs_dir):
        configs = []
        addresses = []

        for index in xrange(count):
            config = self.get_default_config()

            init_singletons(config, yt_config, index)

            init_jaeger_collector(config, self.LOWERCASE_NAME, {self.LOWERCASE_NAME + "_index": str(index)})

            config["cluster_connection"] = cluster_connection
            config["rpc_port"] = next(ports_generator)
            config["monitoring_port"] = next(ports_generator)
            config["logging"] = _init_logging(logs_dir,
                                              self.DASHED_NAME + "-" + str(index),
                                              yt_config,
                                              has_structured_logs=False)

            configs.append(config)
            addresses.append("{}:{}".format(yt_config.fqdn, config["rpc_port"]))

        return configs, addresses

    def wait_for_readiness(self, address):
        raise NotImplementedError("Override me in the derived class")

    def on_start(self):
        pass

    def on_finish(self):
        pass

    def __enter__(self):
        custom_paths = []
        if self.PATH is not None:
            custom_paths.append(self.PATH)
        self.pids = self.env.run_yt_component(self.DASHED_NAME, self.config_paths, name=self.LOWERCASE_NAME,
                                              custom_paths=custom_paths)
        for address in self.addresses:
            self.wait_for_readiness(address)
        self.on_start()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.env.kill_service(self.LOWERCASE_NAME)
        self.on_finish()
