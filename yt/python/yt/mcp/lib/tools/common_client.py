from .helpers import YTToolBase
from typing import List, Optional


class CommonCypress(YTToolBase):
    ALLOWED_CLIENT_METHODS = set(["get_table_schema", "read_table", "infer_table_schema", "get_current_user", "list_jobs", "get_operation", "get_job_stderr"])

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
                    },
                ]
            },
            {
                "name": "get_operation",
                "description": "Tool for getting operation info",
                "input": [
                    {"name": "cluster"},
                    {
                        "name": "method",
                        "description": "Method to call. Should be set to \"get_operation\".",
                        "examples": ["get_operation"],
                    },
                    {
                        "name": "operation_id",
                        "description": "Operation id (UUID).",
                    },
                    {
                        "name": "attributes",
                        "field_type": Optional[List[str]],
                        "description": (
                            "List of operation attributes. Some attributes are large "
                            "(\"progress\", \"full_spec\"). Valid attributes are "
                            "\"id\", \"state\", \"authenticated_user\", \"type\", "
                            "\"operation_type\", \"progress\", \"spec\", \"provided_spec\", "
                            "\"experiment_assignments\", \"experiment_assignment_names\", "
                            "\"full_spec\", \"unrecognized_spec\", \"brief_progress\", "
                            "\"brief_spec\", \"runtime_parameters\", \"start_time\", "
                            "\"finish_time\", \"result\", \"events\", \"memory_usage\", "
                            "\"suspended\", \"scheduling_attributes_per_pool_tree\", "
                            "\"slot_index_per_pool_tree\", \"alerts\", \"task_names\", "
                            "\"controller_features\", \"alert_events\", \"has_failed_jobs\"."
                        ),
                        "default": ["id", "state", "type", "operation_type", "authenticated_user", "start_time", "finish_time", "suspended", "result", "alerts", "task_names", "has_failed_jobs"],
                    },
                    {
                        "name": "include_runtime",
                        "field_type": Optional[bool],
                        "description": (
                            "Fetch live progress, brief_progress, and memory_usage from the "
                            "scheduler and controller agent when available."
                        ),
                        "default": False,
                    },
                ]
            },
            {
                "name": "get_operation_spec",
                "description": "Tool for getting operation specification and state",
                "input": [
                    {"name": "cluster"},
                    {
                        "name": "method",
                        "description": "Method to call. Should be set to \"get_operation\".",
                        "examples": ["get_operation"],
                    },
                    {
                        "name": "operation_id",
                        "description": "Operation id (UUID).",
                    },
                    {
                        "name": "attributes",
                        "field_type": List[str],
                        "description": "List of operation attributes. Should set be [\"id\", \"spec\", \"state\"]",
                        "default": ["id", "spec", "state"],
                    },
                ]
            },
            {
                "name": "get_operation_jobs",
                "description": "Tool for getting jobs of operation",
                "input": [
                    {"name": "cluster"},
                    {
                        "name": "method",
                        "description": "Method to call. Should be set to \"list_jobs\".",
                        "examples": ["list_jobs"],
                    },
                    {
                        "name": "operation_id",
                        "description": "Operation id (UUID).",
                    },
                    {
                        "name": "job_state",
                        "field_type": Optional[str],
                        "description": "Filter jobs by state. State one of 'failed', 'aborted', 'completed'",
                    },
                    {
                        "name": "limit",
                        "field_type": Optional[int],
                        "default": 10,
                    },
                    {
                        "name": "offset",
                        "field_type": Optional[int],
                        "default": 0,
                    },
                    {
                        "name": "with_stderr",
                        "description": "Filter jobs that has stderr or not",
                        "field_type": Optional[bool],
                    },
                ]
            },
            {
                "name": "get_operation_job_stderr",
                "description": "Tool for getting stderr of operation job",
                "input": [
                    {"name": "cluster"},
                    {
                        "name": "method",
                        "description": "Method to call. Should be set to \"get_job_stderr\".",
                        "examples": ["get_job_stderr"],
                    },
                    {
                        "name": "operation_id",
                        "description": "Operation id (UUID).",
                    },
                    {
                        "name": "job_id",
                        "description": "Job id (UUID).",
                    },
                ]
            },
        ]
