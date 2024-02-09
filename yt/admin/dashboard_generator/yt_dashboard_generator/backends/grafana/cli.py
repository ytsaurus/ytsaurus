from .. import grafana
from ... import cli
from ...diff import diff as diff_dashboards

import json
import os
import sys
import tabulate


class GrafanaFacade(cli.FacadeBase):
    api_key = None
    base_url = None
    datasource = {"type": "prometheus", "uid": "${PROMETHEUS_DS_UID}"}
    tag_postprocessor = None

    def __init__(self, dashboard_id, func, uid, title):
        super().__init__(dashboard_id)
        self.dashboard_id = dashboard_id
        self.func = func
        self.uid = uid
        self.title = title

    @staticmethod
    def get_backend_name():
        return "grafana"

    def set_dashboard_id(self, dashboard_id):
        self.dashboard_id = dashboard_id
        self.dashboard_name = dashboard_id

    @classmethod
    def register_params(cls, parser):
        parser.add_argument(
            "--grafana-api-key", type=str,
            help="Grafana API key (taken from GRAFANA_API_KEY_PATH by default)")
        parser.add_argument(
            "--grafana-base-url", type=str,
            help="Grafana base url (taken from GRAFANA_BASE_URL by default)")
        parser.add_argument(
            "--grafana-datasource", type=str,
            help="Grafana datasource in JSON")

    @classmethod
    def on_args_parsed(cls, args):
        def _get_key_from_env():
            key_path = os.getenv("GRAFANA_API_KEY_PATH")
            if key_path is not None:
                return open(key_path).read().rstrip()

        cls.api_key = cls.api_key or args.grafana_api_key or _get_key_from_env()
        cls.base_url = args.grafana_base_url or os.getenv("GRAFANA_BASE_URL") or cls.base_url
        if args.grafana_datasource:
            cls.datasource = json.loads(args.grafana_datasource)

    def diff(self):
        # TODO: Current implementation with pretty dashboard rendering in text format is impossible to maintain,
        # given various features of Grafana. To be fair, it is not very useful either.
        # Should make it similar to Monitoring dashboard generator CLI,
        # i.e. no "diff" command and just output final JSON for "show" and "preview" commands.
        raise NotImplementedError()

        dashboard = self.func()
        serializer = grafana.GrafanaDictSerializer(None, self.tag_postprocessor)
        ours = dashboard.serialize(serializer)
        ours = grafana.GrafanaDashboardParser().parse(ours, diff_format=True)

        proxy = grafana.GrafanaProxy(self.base_url, self.api_key)
        dashboard = proxy.fetch_dashboard(self.dashboard_id)
        theirs = grafana.GrafanaDashboardParser().parse(
            dashboard["dashboard"]["panels"],
            diff_format=True)

        result = diff_dashboards(theirs, ours)
        print(tabulate.tabulate(result, tablefmt="grid"))

    def show(self):
        # NB: See comment for diff().
        raise NotImplementedError()

        proxy = grafana.GrafanaProxy(self.base_url, self.api_key)
        dashboard = proxy.fetch_dashboard(self.dashboard_id)
        parsed = grafana.GrafanaDashboardParser().parse(dashboard["dashboard"]["panels"])
        print(tabulate.tabulate(parsed, tablefmt="grid"))

    def preview(self):
        dashboard = self.func()
        serializer = grafana.GrafanaDebugSerializer(self.tag_postprocessor)
        result = dashboard.serialize(serializer)
        print(tabulate.tabulate(result, tablefmt="grid"))

    def json(self, file=False):
        dashboard = self.func()
        serializer = grafana.GrafanaDictSerializer(self.datasource, self.tag_postprocessor)
        result = dashboard.serialize(serializer)
        result["uid"] = self.uid
        if "title" not in result:
            result["title"] = self.title
            result["title"] = " ".join(map(str.capitalize, self.uid.split("-"))).replace(
                "Ytsaurus", "YTsaurus")
        if not file:
            json.dump(result, sys.stdout, indent=4)
        else:
            os.makedirs("generated/grafana", exist_ok=True)
            with open(f"generated/grafana/{self.uid}.json", "w") as fd:
                json.dump(result, fd, indent=4)

    def do_submit(self, verbose):
        dashboard = self.func()
        serializer = grafana.GrafanaDictSerializer(self.datasource, self.tag_postprocessor)
        serialized_dashboard = dashboard.serialize(serializer)
        if verbose:
            json.dump(serialized_dashboard, sys.stderr, indent=4)
        proxy = grafana.GrafanaProxy(self.base_url, self.api_key)
        proxy.submit_dashboard(serialized_dashboard, self.dashboard_id)
