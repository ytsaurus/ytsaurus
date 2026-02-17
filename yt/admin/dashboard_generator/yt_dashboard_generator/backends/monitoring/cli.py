from .. import monitoring
from ... import cli
from ...helpers import pretty_print_fixed_indent

import os
from typing import Any


class MonitoringFacade(cli.FacadeBase):
    token = None
    endpoint = None
    tag_postprocessor = None

    def __init__(self, dashboard_id, func, uid, title):
        super().__init__(dashboard_id)
        self.dashboard_id = dashboard_id
        self.func = func
        self.uid = uid
        self.title = title

    @staticmethod
    def get_backend_name():
        return "monitoring"

    def set_dashboard_id(self, dashboard_id):
        self.dashboard_id = dashboard_id
        self.dashboard_name = dashboard_id

    @classmethod
    def register_params(cls, parser):
        parser.add_argument(
            "--solomon-token", type=str,
            help="token for Solomon and Monitoring (read from ~/.solomon/token by default)")
        parser.add_argument("--monitoring-endpoint", type=str)
        parser.add_argument("--disable-monitoring-set-name", action='store_true', help="Disable name set")

    @classmethod
    def on_args_parsed(cls, args):
        cls.token = cls.token or args.solomon_token or open(os.path.expanduser("~/.solomon/token")).read().rstrip()
        if args.monitoring_endpoint is not None:
            cls.endpoint = args.monitoring_endpoint
        cls.set_name = not args.disable_monitoring_set_name

    def show(self):
        proxy = monitoring.MonitoringProxy(self.endpoint, self.token)
        print(proxy.fetch_dashboard(self.dashboard_id))

    def preview(self):
        dashboard = self._call_func(self.func)
        serializer = monitoring.MonitoringDebugSerializer(self.tag_postprocessor)
        result = dashboard.serialize(serializer)
        pretty_print_fixed_indent(result)

    def _prepare_serialized_dashboard(self, verbose):
        dashboard = self._call_func(self.func)
        if self.set_name:
            dashboard.try_set_name(self.uid)
        serializer = monitoring.MonitoringDictSerializer(self.tag_postprocessor)
        widgets = dashboard.serialize(serializer)
        return widgets

    def do_submit(self, verbose):
        serialized_dashboard = self._prepare_serialized_dashboard(verbose)
        proxy = monitoring.MonitoringProxy(self.endpoint, self.token)
        proxy.submit_dashboard(serialized_dashboard, self.dashboard_id, verbose=verbose)

    def generate_serialized_dashboard(self, verbose: bool) -> dict[str, Any]:
        return self._prepare_serialized_dashboard(verbose)

    def json(self, file=False):
        raise NotImplementedError()
