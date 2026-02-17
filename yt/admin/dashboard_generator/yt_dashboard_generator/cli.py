import yt.wrapper as yt

import tabulate

import json
import inspect
import logging
import pathlib
import sys
from abc import ABC, abstractmethod
from argparse import ArgumentParser


logger = logging.getLogger(__name__)

TEST_DASHBOARD_KEY = "test-dashboard"


class FacadeBase(ABC):
    def __init__(self, dashboard_name):
        super().__init__()
        self.dashboard_name = dashboard_name
        self.dashboard_config = None
        self.slug = None

    @staticmethod
    @abstractmethod
    def get_backend_name():
        pass

    @abstractmethod
    def set_dashboard_id(self, dashboard_id):
        pass

    @classmethod
    def register_params(cls, parser):
        pass

    @classmethod
    def on_args_parsed(cls, args):
        pass

    @abstractmethod
    def show(self):
        pass

    @abstractmethod
    def preview(self):
        pass

    @abstractmethod
    def do_submit(self, verbose):
        pass

    @abstractmethod
    def generate_serialized_dashboard(self, verbose):
        pass

    @abstractmethod
    def json(self, file):
        pass

    def submit(self, need_confirmation, verbose):
        if need_confirmation:
            self._confirm('You are about to submit dashboard "{}" to {} (dashboard_id: {}), continue?'.format(
                self.slug, self.get_backend_name(), self.dashboard_name))
        self.do_submit(verbose=verbose)

    def submit_cypress(self, need_confirmation, verbose, cypress_path, cypress_document_name):
        if cypress_document_name is None:
            cypress_document_name = self.slug
        dashboard_path = f"{cypress_path}/{cypress_document_name}"

        if need_confirmation:
            self._confirm('You are about to submit dashboard "{}" to cypress (backend_type: {}, dashboard_id: {}, cluster_proxy: {}, dashboard_path: {}), continue?'.format(
                self.slug, self.get_backend_name(), self.dashboard_name, yt.config["proxy"]["url"], dashboard_path))
        serialized_dashboard = self.generate_serialized_dashboard(verbose=verbose)
        client = yt.YtClient(config=yt.config.config)
        client.create("document", dashboard_path, ignore_existing=True)
        client.set(dashboard_path, serialized_dashboard)
        logger.info(f'Dashboard "{self.slug}" with backend "{self.get_backend_name()}" was submitted to cypress')

    @staticmethod
    def _confirm(msg):
        print("{} [y/N]".format(msg), file=sys.stderr, end=" ")
        if input() == "y":
            return
        raise RuntimeError("Aborting")

    def _call_func(self, func):
        signature = inspect.signature(func)
        has_config_parameter = False
        config_required = False
        for name, param in signature.parameters.items():
            if name == "config":
                has_config_parameter = True
                config_required = param.default == inspect.Parameter.empty
                break

        if config_required and self.dashboard_config is None:
            raise ValueError(f"This dashboard requires dashboard config (title: {self.title})")

        if self.dashboard_config is not None and not has_config_parameter:
            raise ValueError(f"This dashboard is not configurable, but config is set (title: {self.title})")

        if not has_config_parameter or self.dashboard_config is None:
            return func()

        return func(config=self.dashboard_config)


class Cli():
    def __init__(self, parser: ArgumentParser):
        self.parser = parser
        self.backend_subparser = self.parser.add_argument_group(conflict_handler="resolve")
        self.backend_classes = {}
        self.dashboards = []
        self.db_choices = ["all"]
        self.backend_choices = []

        parser.add_argument("--config", help="Path to config with dashboard ids in JSON format")
        group = parser.add_mutually_exclusive_group()
        group.add_argument("--dashboard-id",
                           help="dashboard id; this option is applicable only if one dashboard is selected")
        group.add_argument("--use-test-dashboard-id", default=False, action="store_true",
                           help="use test dashboard id specified in config; "
                                "this option is applicable only if one dashboard is selected")

        dashboard_config_group = parser.add_mutually_exclusive_group(required=False)
        dashboard_config_group.add_argument("--dashboard-config",
                                            default=None,
                                            type=json.loads,
                                            help="json to be used as the dashboard config; "
                                            "this option is only applicable if exactly one dashboard is selected")
        dashboard_config_group.add_argument("--dashboard-config-path",
                                            default=None,
                                            type=pathlib.Path,
                                            help="path to json file to be used as the dashboard config; "
                                                 "this option is only applicable if exactly one dashboard is selected")

        sp = parser.add_subparsers(dest="command", required=True)
        sp.add_parser("list")
        for command in ("diff", "show", "preview", "submit", "submit-cypress", "json"):
            p = sp.add_parser(command)
            p.add_argument("dashboards", choices=self.db_choices, nargs="*", default="")
            p.add_argument("--backend", choices=self.backend_choices, nargs="+")
            if command in ("submit", "submit-cypress"):
                p.add_argument("-y", action="store_true", help="skip confirmation")
                p.add_argument("--verbose", action="store_true", help="print raw dashboard representation before submit")
            if command == "submit-cypress":
                p.add_argument("--proxy", type=yt.config.set_proxy, help="[YT] Cluster proxy to which you need to upload the dashboard")
                p.add_argument("--cypress-path", default="//sys/interface-monitoring", help="[YT] Path to the node where the dashboard will be saved")
                p.add_argument(
                    "--cypress-document-name",
                    default=None,
                    help="[YT] The name of the document the dashboard will be saved with. If not specified, it will be equal to slug",
                )
            if command == "json":
                p.add_argument("-f", action="store_true", help="output to file")

    def add_dashboard(self, slug: str, backend: FacadeBase):
        self.dashboards.append((slug, backend))
        if slug not in self.db_choices:
            self.db_choices.append(slug)

        backend.slug = slug

        name = backend.get_backend_name()
        if name in self.backend_classes:
            assert type(backend) is self.backend_classes[name]
        else:
            self.backend_classes[name] = type(backend)
            type(backend).register_params(self.parser)
            self.backend_choices.append(name)

    def run(self, args):
        config = None
        if args.config is None:
            config = self._try_get_config_from_resource()
        else:
            config = json.load(open(args.config))

        for slug, backend in self.dashboards:
            if config is None or slug not in config:
                continue
            if backend.get_backend_name() not in config[slug]:
                continue

            full_dashboard_config = config[slug][backend.get_backend_name()]
            if isinstance(full_dashboard_config, str):
                dashboard_id = full_dashboard_config
                dashboard_config = None
            elif isinstance(full_dashboard_config, dict):
                dashboard_id = full_dashboard_config["id"]
                dashboard_config = full_dashboard_config.get("config", None)
            else:
                raise ValueError(f"Unsupported config type {type(full_dashboard_config)}, it should either be str (only id) or dict")

            if dashboard_config:
                backend.dashboard_config = dashboard_config
            backend.set_dashboard_id(dashboard_id)

        if args.command == "list":
            print(tabulate.tabulate(
                [["slug", "backend"]] +
                [[slug, backend.get_backend_name()] for slug, backend in self.dashboards],
                headers="firstrow"))
            return

        if args.command in ["diff", "show", "submit"]:
            if args.backend is not None:
                for backend_name in args.backend:
                    self.backend_classes[backend_name].on_args_parsed(args)
            else:
                for cls in self.backend_classes.values():
                    cls.on_args_parsed(args)

        selected_dashboards = []
        if "all" in args.dashboards:
            for slug, backend in self.dashboards:
                if args.backend is None or backend.get_backend_name() in args.backend:
                    selected_dashboards.append(backend)
        else:
            for slug, backend in self.dashboards:
                if slug in args.dashboards and (args.backend is None or backend.get_backend_name() in args.backend):
                    selected_dashboards.append(backend)

        if not selected_dashboards:
            raise Exception("No matching dashboards found")

        if len(selected_dashboards) == 1:
            dashboard = selected_dashboards[0]
            if args.dashboard_id is not None:
                dashboard.set_dashboard_id(args.dashboard_id)
            if args.use_test_dashboard_id:
                if config is None:
                    raise Exception("Config is not specified for using test dashboard id")
                if TEST_DASHBOARD_KEY not in config or backend.get_backend_name() not in config[TEST_DASHBOARD_KEY]:
                    raise Exception("Test dashboard id is not specified for {} backend".format(dashboard.get_backend_name()))
                dashboard.set_dashboard_id(config[TEST_DASHBOARD_KEY][dashboard.get_backend_name()])

            if args.dashboard_config is not None or args.dashboard_config_path is not None:
                config = args.dashboard_config
                if args.dashboard_config_path is not None:
                    with open(args.dashboard_config_path) as f:
                        config = json.load(f)

                dashboard.dashboard_config = config
        else:
            if args.dashboard_id is not None:
                raise Exception("Option --dashboard-id is applicable iff exactly one dashboard is selected")
            if args.use_test_dashboard_id:
                raise Exception("Option --use-test-dashboard-id is applicable iff exactly one dashboard is selected")
            if args.dashboard_config is not None or args.dashboard_config_path is not None:
                raise Exception("Options --dashboard-config and --dashboard-config-path are applicable iff exactly one dashboard is selected")

        if args.command == "diff":
            for d in selected_dashboards:
                d.diff()
        if args.command == "preview":
            for d in selected_dashboards:
                d.preview()
        if args.command == "submit":
            for d in selected_dashboards:
                d.submit(need_confirmation=not args.y, verbose=args.verbose)
        if args.command == "submit-cypress":
            for d in selected_dashboards:
                d.submit_cypress(
                    need_confirmation=not args.y,
                    verbose=args.verbose,
                    cypress_path=args.cypress_path,
                    cypress_document_name=args.cypress_document_name,
                )
        if args.command == "show":
            for d in selected_dashboards:
                d.show()
        if args.command == "json":
            for d in selected_dashboards:
                d.json(file=args.f)

    def _try_get_config_from_resource(self):
        try:
            import library.python.resource
        except ImportError:
            return None

        config = library.python.resource.find("/yt_dashboards/config.json")
        if config is None:
            return None
        return json.loads(config)
