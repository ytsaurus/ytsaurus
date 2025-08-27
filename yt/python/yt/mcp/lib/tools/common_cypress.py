from .helpers import YTToolBase


class CommonCypress(YTToolBase):
    METHODS = set(["get_table_schema"])

    def get_tool_description(self):
        return (
            self.ToolName(
                name="common_cypress",
                description="""
A tool for common cypress method.
""",
            ),
            [
                self.ToolInputField(
                    field_type=str,
                    name="cluster",
                    description=f"Cluster name. One of: {self.runner.helper_get_public_clusters(delimeter=', ')}",
                    examples=self.runner.helper_get_public_clusters(),
                ),
                self.ToolInputField(
                    field_type=str,
                    name="method",
                    description="Method to call",
                    examples=self.METHODS,
                ),
            ]
        )

    def on_handle_request(
        self,
        *,
        cluster,
        method,
        request_context,
        **kwargs
    ):
        self.runner._logger.debug(f"Starting {self}")
        yt_client = self.runner.helper_get_yt_client(cluster, request_context)

        if method not in self.METHODS:
            raise RuntimeError("Method not allowed")

        method_func = getattr(yt_client, method)

        try:
            result = method_func(**kwargs)
            return self.runner.return_structured(result)
        except Exception as ex:
            self.helper_process_common_exception(ex)

    def get_tool_variants(self):
        return [
            {
                "name": "get_table_schema",
                "description": "A tool for getting table schema. Schema stored in \"value\" field, field \"attributes\" has schema flags \"strict\" and \"unique_keys\"",
                "input": [
                    {"name": "cluster"},
                    {
                        "name": "method",
                        "description": "Method to call. Should be set to \"get_table_schema\".",
                        # "default": "PydanticUndefined",
                    },
                    {
                        "name": "table_path",
                        "description": "Path to table",
                    },
                ]
            },
        ]
