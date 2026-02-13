from .helpers import YTToolBase


class CommonCypress(YTToolBase):
    ALLOWED_CLIENT_METHODS = set(["get_table_schema", "read_table", "infer_table_schema", "get_current_user"])

    def get_tool_description(self):
        return (
            self.ToolName(
                name="common_client",
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

        if method not in self.ALLOWED_CLIENT_METHODS:
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
                "description": "A tool for getting table schema. Schema stored in \"value\" field, field \"attributes\" has schema flags \"strict\" and \"unique_keys\". Empty schema means error getting schema and tool \"infer_table_schema\" can infer schema from table conent",  # noqa
                "input": [
                    {"name": "cluster"},
                    {
                        "name": "method",
                        "description": "Method to call. Should be set to \"get_table_schema\".",
                        "examples": ["get_table_schema"],
                        # "default": "PydanticUndefined",
                    },
                    {
                        "name": "table_path",
                        "description": "Path to table",
                    },
                ]
            },
            {
                "name": "read_table",
                "description": "A tool for getting whole table content. Data size can by large.",
                "input": [
                    {"name": "cluster"},
                    {
                        "name": "method",
                        "description": "Method to call. Should be set to \"read_table\".",
                        "examples": ["read_table"],
                        # "default": "PydanticUndefined",
                    },
                    {
                        "name": "table",
                        "description": "Path to table.",
                    },
                ]
            },
            {
                "name": "sample_static_table",
                "description": "A tool for getting sample of static table content. Table must be \"static\" (\"type\" attribute should be \"static\").",
                "input": [
                    {"name": "cluster"},
                    {
                        "name": "method",
                        "description": "Method to call. Should be set to \"read_table\".",
                        "examples": ["read_table"],
                        # "default": "PydanticUndefined",
                    },
                    {
                        "name": "table",
                        "description": "Path to table. Path should be appended with row selector \"[#0:#1]\" for data sampling (selecting first row).",
                    },
                ]
            },
            {
                "name": "infer_table_schema",
                "description": "Tool infers tables schema from its content.",
                "input": [
                    {"name": "cluster"},
                    {
                        "name": "method",
                        "description": "Method to call. Should be set to \"infer_table_schema\".",
                        "examples": ["infer_table_schema"],
                        # "default": "PydanticUndefined",
                    },
                    {
                        "name": "table",
                        "description": "Path to table.",
                    },
                ]
            },
            {
                "name": "whoami",
                "description": "Tool get current user info on cluster.",
                "input": [
                    {"name": "cluster"},
                    {
                        "name": "method",
                        "description": "Method to call. Should be set to \"get_current_user\".",
                        "examples": ["get_current_user"],
                        # "default": "PydanticUndefined",
                    },
                ]
            },
        ]
