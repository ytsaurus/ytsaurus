from .helpers import YTToolBase

import fnmatch
import os


class ListDir(YTToolBase):
    def get_tool_description(self):
        return (
            self.ToolName(
                name="list_dir",
                description="""
A tool for getting content of node or directory on YT cluster
Results has metadata (attributes) for each node: type (file, table, map_node), account, creation_time, row_count.
Output, by default, must be in table format, sorted by node name and shows only node name and creation time.
"""
            ),
            [
                self.ToolInputField(
                    field_type=str,
                    name="directory",
                    description="""
Path to cluster node or directory.
The path to the directory must start from `//` and cannot start from: {}
""".format(self.runner.helper_get_public_clusters(template="`//{}`")),
                ),
                self.ToolInputField(
                    field_type=str,
                    name="cluster",
                    description=f"Cluster. One of: {self.runner.helper_get_public_clusters(delimeter=", ")}",
                    examples=self.runner.helper_get_public_clusters(),
                )
            ]
        )

    def on_handle_request(self, *, cluster, directory, request_context, **kwargs):
        yt_client = self.runner.helper_get_yt_client(cluster, request_context)

        try:
            nodes = yt_client.list(
                directory,
                attributes=[
                    "creation_time",
                    "account",
                    "type",
                    "row_count",
                    "resource_usage",
                    "primary_medium",
                ],
            )
        except Exception as e:
            self.helper_process_common_exception(e)

        return self.runner.return_structured(nodes)


class Search(YTToolBase):
    def get_tool_description(self):
        return (
            self.ToolName(
                name="find",
                description="""
A tool for serach objects in cluster subtree
""",
            ),
            [
                self.ToolInputField(
                    name="root_path",
                    description="""
Root path to cluster node to start search.
The path must start from `//` and cannot start from: {}
""".format(self.runner.helper_get_public_clusters(template="`//{}`")),
                ),
                self.ToolInputField(
                    field_type=list[str],
                    name="type",
                    description="""
Type of object: table, file, document, account, user, list_node, map_node
""",
                    default="table",
                ),
                self.ToolInputField(
                    name="name",
                    description="""
Pattern of object name. In shell-style wildcards.
""",
                    default=None,
                ),
                self.ToolInputField(
                    field_type=dict[str, str],
                    name="attributes_to_match",
                    description="""
Object attributes to filter: owner, account
""",
                    default=None,
                ),
                self.ToolInputField(
                    field_type=list[str],
                    name="attributes",
                    description="""
List of result attributes: account, owner
""",
                    default=None,
                ),
                self.ToolInputField(
                    name="cluster",
                    description=f"Cluster. One of: {self.runner.helper_get_public_clusters(delimeter=", ")}",
                    examples=self.runner.helper_get_public_clusters(),
                ),
            ]
        )

    def get_tool_variants(self):
        return []

    def on_handle_request(
        self,
        *,
        cluster,
        root_path,
        name,
        type,
        attributes_to_match,
        attributes,
        request_context,
        **kwargs
    ):
        yt_client = self.runner.helper_get_yt_client(cluster, request_context)

        if name:
            path_filter = lambda path: fnmatch.fnmatch(os.path.basename(path), name)  # noqa
        else:
            path_filter = None

        if not attributes:
            attributes = []

        if attributes_to_match:
            attributes.extend(attributes_to_match.keys())

            object_filter = lambda obj: all([  # noqa
                obj.attributes.get(k) == v for k, v in attributes_to_match.items()
            ])
        else:
            object_filter = None

        attributes.extend([
            "creation_time",
            "account",
            "type",
        ])

        try:
            nodes = yt_client.search(
                root=root_path,
                node_type=type,
                path_filter=path_filter,
                object_filter=object_filter,
                attributes=attributes,
                read_from="cache",
            )
        except Exception as e:
            self.runner.helper_process_common_exception(e)

        return self.runner.return_structured(nodes)
