from .helpers import YTToolBase
from .security import check_system_paths, check_truncate, check_write_keywords

from typing import Dict, List, Optional


class StartQuery(YTToolBase):
    def get_tool_description(self):
        return (
            self.ToolName(
                name="qt_start_query",
                description="""
Start a new query in YT Query Tracker on specified cluster.
Executes query asynchronously with specified engine.

QUERY ENGINES (select one):
  yql: RECOMMENDED
    Syntax: "USE <cluster>; SELECT ... FROM `//path`"

  chyt:
    Settings: {"cluster": "<cluster>"}

  ql:
    Settings: {"cluster": "<cluster>"}

  spyt:
    Settings: {"cluster": "<cluster>"}

WORKFLOW: This tool starts a query. Always follow with qt_get_query to check status, then qt_get_query_results to retrieve results.

NOTE: Query runs asynchronously. Must check status before retrieving results.
""".strip(),
            ),
            [
                self.ToolInputField(
                    name="cluster",
                    description="Cluster name.",
                ),
                self.ToolInputField(
                    name="engine",
                    description=(
                        "Query engine. Available engines depends on cluster configuration. "
                        'yql: recommended. chyt: requires settings. '
                        'ql: for dynamic tables only. spyt: may be unavailable.'
                    ),
                    examples=["yql", "chyt", "ql", "spyt"],
                ),
                self.ToolInputField(
                    name="query",
                    description="Query text in syntax of selected engine.",
                    examples=["SELECT * FROM `//tmp/table`"],
                ),
                self.ToolInputField(
                    name="stage",
                    description='Query tracker stage (default: "production").',
                    default=None,
                ),
                self.ToolInputField(
                    field_type=Optional[Dict],
                    name="settings",
                    description='Engine-specific settings. yql: optional. chyt/ql/spyt: REQUIRED {"cluster": "<cluster>"}',
                    default=None,
                ),
                self.ToolInputField(
                    field_type=Optional[List[str]],
                    name="access_control_objects",
                    description="List of ACO names that control who can view/modify results.",
                    default=None,
                ),
                self.ToolInputField(
                    field_type=Optional[List],
                    name="files",
                    description="Optional files attached to query. Each: name, content, type (raw_inline_data or url).",
                    default=None,
                ),
                self.ToolInputField(
                    field_type=Optional[Dict],
                    name="annotations",
                    description="Annotations dict for custom metadata.",
                    default=None,
                ),
                self.ToolInputField(
                    field_type=Optional[bool],
                    name="is_indexed",
                    description="Make query searchable in Query Tracker UI.",
                    default=None,
                ),
            ],
        )

    def on_handle_request(
        self,
        *,
        cluster,
        engine,
        query,
        stage=None,
        settings=None,
        files=None,
        access_control_objects=None,
        annotations=None,
        is_indexed=None,
        request_context,
        **kwargs,
    ):
        check_system_paths(query)
        check_truncate(query)
        check_write_keywords(query, self.runner._rw_mode)
        yt_client = self.runner.helper_get_yt_client(cluster, request_context)
        try:
            query_id = yt_client.start_query(
                engine=engine,
                query=query,
                settings=settings,
                files=files,
                stage=stage,
                annotations=annotations,
                access_control_objects=access_control_objects,
                is_indexed=is_indexed,
            )
        except Exception as ex:
            self.helper_process_common_exception(ex)
        return self.runner.return_structured({"query_id": query_id})


class GetQuery(YTToolBase):
    def get_tool_description(self):
        return (
            self.ToolName(
                name="qt_get_query",
                description="""
Check status and retrieve metadata of a Query Tracker query.

WORKFLOW: Use this repeatedly to poll query status until it reaches 'completed', 'failed', or 'aborted'.

IMPORTANT: Poll no more than once per minute. Query execution typically takes tens of seconds to minutes.

OPTIMIZATION: Use 'attributes' parameter to fetch only 'state' field while polling (faster).
""".strip(),
            ),
            [
                self.ToolInputField(
                    name="cluster",
                    description="Cluster name.",
                ),
                self.ToolInputField(
                    name="query_id",
                    description="ID of the query to retrieve.",
                ),
                self.ToolInputField(
                    field_type=Optional[List[str]],
                    name="attributes",
                    description="Specific attributes to retrieve. Omit for all. Available: state, engine, query, start_time, finish_time, error, result_count, progress, annotations, user, settings.",
                    default=None,
                    examples=[["state", "engine", "error"]],
                ),
                self.ToolInputField(
                    name="stage",
                    description='Query tracker stage. Defaults to "production".',
                    default=None,
                ),
            ],
        )

    def on_handle_request(self, *, cluster, query_id, attributes=None, stage=None, request_context, **kwargs):
        yt_client = self.runner.helper_get_yt_client(cluster, request_context)
        try:
            result = yt_client.get_query(
                query_id=query_id,
                attributes=attributes,
                stage=stage,
            )
        except Exception as ex:
            self.helper_process_common_exception(ex)
        return self.runner.return_structured(result)


class GetQueryResults(YTToolBase):
    def get_tool_description(self):
        return (
            self.ToolName(
                name="qt_get_query_results",
                description="""
Retrieve results from a completed Query Tracker query.

PREREQUISITE: Query must be in 'completed' state. Use qt_get_query to verify status first.

IMPORTANT NOTES:
1. Only call when query state = 'completed'
2. Results may be large - check 'row_count' in metadata to understand data size
3. For multi-output queries, use result_index to select which result set
4. Schema provides column information (names, types, nullability)
5. data_statistics includes row_count, compressed/uncompressed size, etc.
""".strip(),
            ),
            [
                self.ToolInputField(
                    name="cluster",
                    description="Cluster name.",
                ),
                self.ToolInputField(
                    name="query_id",
                    description="ID of the query whose results to read.",
                ),
                self.ToolInputField(
                    field_type=Optional[int],
                    name="result_index",
                    description="Which result set to read (for multi-output queries). Defaults to 0.",
                    default=0,
                ),
                self.ToolInputField(
                    name="stage",
                    description='Query tracker stage. Defaults to "production".',
                    default=None,
                ),
            ],
        )

    def on_handle_request(
        self,
        *,
        cluster,
        query_id,
        result_index=0,
        stage=None,
        request_context,
        **kwargs,
    ):
        yt_client = self.runner.helper_get_yt_client(cluster, request_context)
        try:
            meta = yt_client.get_query_result(
                query_id=query_id,
                result_index=result_index,
                stage=stage,
            )
            rows = list(
                yt_client.read_query_result(
                    query_id=query_id,
                    result_index=result_index,
                    stage=stage,
                )
            )
        except Exception as ex:
            self.helper_process_common_exception(ex)

        return self.runner.return_structured(
            {
                "meta": meta,
                "rows": rows,
            }
        )
