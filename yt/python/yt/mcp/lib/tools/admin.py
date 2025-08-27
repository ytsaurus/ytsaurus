import yt.wrapper as yt

from .helpers import YTToolBase


_ATTRIBUTES_PROXY = ("type", "role", "version")


class GetProxy(YTToolBase):
    def get_tool_description(self):
        return (
            self.ToolName(
                name="get_proxy",
                description="""
A tool for getting proxy list with attributes/propertions of specific type. Proxy are component of YT cluster.
""",
            ),
            [
                self.ToolInputField(
                    field_type=list[str] | None,
                    name="proxies",
                    description="Names of proxy in fqdn format with port. Or absent for list of all proxies",
                    default=None,
                ),
                self.ToolInputField(
                    field_type=list[str],
                    name="attributes",
                    description="Attribute/property names to get. One of: proxy_type," + ",".join(_ATTRIBUTES_PROXY) + """
Attribute "role" means specifyc role of proxy.
Attribute "version" means version of software of proxy.
Attribute "proxy_type" means type of proxy. "http" or "rpc".
"""
                ),
                self.ToolInputField(
                    field_type=str | None,
                    name="proxy_type",
                    description="Proxy type. Which proxies showld returns. One of: http, rpc. Absent means all types.",
                    default=None,
                ),
                self.ToolInputField(
                    field_type=str,
                    name="cluster",
                    description=f"Cluster name. One of: {self.runner.helper_get_public_clusters(delimeter=', ')}",
                    examples=self.runner.helper_get_public_clusters(),
                ),
            ]
        )

    def get_tool_variants(self):
        return []

    def on_handle_request(
        self,
        *,
        cluster: str,
        attributes: list[str],
        proxy_type: str = None,
        proxies: list[str] = None,
        request_context,
        **kwargs
    ):
        self.runner._logger.debug(f"Starting {self}")
        yt_client = self.runner.helper_get_yt_client(cluster, request_context)

        if "proxy_type" in attributes:
            attributes.remove("proxy_type")

        proxy_locations = {
            "all": zip(
                ({"proxy_type": "rpc"}, {"proxy_type": "http"}),
                ("//sys/rpc_proxies", "//sys/http_proxies")
            ),
            "http": zip(
                ({"proxy_type": "http"}, ),
                ("//sys/http_proxies", )
            ),
            "rpc": zip(
                ({"proxy_type": "rpc"}, ),
                ("//sys/rpc_proxies", )
            ),
        }

        try:
            result = []
            for presets, path in proxy_locations[proxy_type or "all"]:
                if proxies:
                    for proxy in proxies:
                        try:
                            value = yt_client.get(f"{path}/{proxy}", attributes=attributes)
                        except yt.YtError:
                            continue
                        proxy_details = {
                            proxy: {
                                **presets,
                                **value.attributes,
                            }
                        }
                        result.append(proxy_details)
                else:
                    proxy_all = yt_client.list(path, attributes=attributes)
                    result.extend([{str(proxy): {**presets, **proxy.attributes}} for proxy in proxy_all])

            return self.runner.return_structured(result)
        except Exception as ex:
            self.helper_process_common_exception(ex)
