from .. import monitoring
from ... import cli
from ...helpers import pretty_print_fixed_indent

import os


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

    @classmethod
    def on_args_parsed(cls, args):
        cls.token = cls.token or args.solomon_token or open(os.path.expanduser("~/.solomon/token")).read().rstrip()
        if args.monitoring_endpoint is not None:
            cls.endpoint = args.monitoring_endpoint

    def diff(self):
        # TODO: Implement some kind of JSON diff?
        raise NotImplementedError()

    def show(self):
        proxy = monitoring.MonitoringProxy(self.endpoint, self.token)
        print(proxy.fetch_dashboard(self.dashboard_id))

    def preview(self):
        dashboard = self.func()
        serializer = monitoring.MonitoringDebugSerializer(self.tag_postprocessor)
        result = dashboard.serialize(serializer)
        pretty_print_fixed_indent(result)

    def do_submit(self, verbose):
        dashboard = self.func()
        serializer = monitoring.MonitoringDictSerializer(self.tag_postprocessor)
        widgets = dashboard.serialize(serializer)

        proxy = monitoring.MonitoringProxy(self.endpoint, self.token)
        proxy.submit_dashboard(widgets, self.dashboard_id, verbose=verbose)

    def json(self, file=False):
        raise NotImplementedError()
