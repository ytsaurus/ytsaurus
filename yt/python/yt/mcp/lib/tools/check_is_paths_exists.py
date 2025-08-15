from .helpers import YTToolBase


# based on @marat-validov, @alexanikin method
class CheckIsPathsExists(YTToolBase):
    def get_tool_description(self):
        return (
            self.ToolName(
                name="check_is_paths_exist",
                description="""
A tool for checking the existence of list of paths on a YT cluster.
List can contain 1 and not more than 500 items.
MUST pass single item as a list.
Each path in MUST start with //.
Еach path MUST not have trailing slash /.
Example result:
    //path : exists
    //path : not exists
    //path : check failed with error - exception
""",
            ),
            [
                self.ToolInputField(
                    field_type=list[str],
                    name="paths",
                    description=f"""
List of paths on a cluster.
List can contain 1 and not more than 500 items.
MUST pass single item as a list.
Each path in MUST start with //.
Еach path MUST not have trailing slash.
The path to the node cannot start from: //yt,
{self.runner.helper_get_public_clusters(template='`//{}`')}
""",
                ),
                self.ToolInputField(
                    field_type=str,
                    name="cluster",
                    description=f"Cluster name. MUST be one of: {self.runner.helper_get_public_clusters(delimeter=', ')}",
                    examples=self.runner.helper_get_public_clusters(template="`//{}`"),
                ),
            ]
        )

    def get_tool_variants(self):
        return []

    def on_handle_request(self, paths, cluster, request_context, **kwargs):

        self.runner._logger.debug(f"Starting {self}")
        yt_client = self.runner.helper_get_yt_client(cluster, request_context)

        result = []
        for path in paths:
            try:
                is_exists = yt_client.exists(path)
                r = {"path": path, "is_exists": is_exists, "error": None}
            except Exception as ex:
                error_description = self.helper_process_common_exception(ex, raise_exception=False)
                r = {"path": path, "is_exists": None, "error": error_description}
            result.append(r)

        return self.runner.return_structured(result)
