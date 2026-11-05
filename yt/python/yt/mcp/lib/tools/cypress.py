from .helpers import YTToolBase

from typing import Annotated, Any, Optional


class CreateNode(YTToolBase):
    _MUTABLE = True

    def on_handle_request(
        self,
        *,
        cluster: Annotated[
            str,
            YTToolBase.ToolInputField(
                description="Cluster",
            )
        ],
        type: Annotated[
            str,
            YTToolBase.ToolInputField(
                description="Node type: map_node, table, file, document, etc.",
            )
        ],
        path: Annotated[
            str,
            YTToolBase.ToolInputField(
                description="Path to the node to create",
            )
        ],
        attributes: Annotated[
            Optional[dict],
            YTToolBase.ToolInputField(
                description=(
                    "Initial node attributes (dict). Common keys: "
                    "schema (for tables, list of column descriptors), "
                    "compression_codec (e.g. \"zstd_5\"), "
                    "account (YT account that owns the node), "
                    "dynamic (true for dynamic tables), "
                    "optimize_for (\"scan\"/\"lookup\")."
                ),
                default=None,
            )
        ] = None,
        recursive: Annotated[
            bool,
            YTToolBase.ToolInputField(
                description="Create missing intermediate nodes",
                default=False,
            )
        ] = False,
        ignore_existing: Annotated[
            bool,
            YTToolBase.ToolInputField(
                description="Do not fail if the node already exists",
                default=False,
            )
        ] = False,
        request_context,
        **kwargs,
    ):
        "A tool for creating a Cypress node (create). Returns the created node id."
        self.helper_check_write_path(path)
        yt_client = self.runner.helper_get_yt_client(cluster, request_context)

        try:
            node_id = yt_client.create(
                type, path, attributes=attributes, recursive=recursive, ignore_existing=ignore_existing,
            )
        except Exception as e:
            self.helper_process_common_exception(e)

        return self.runner.return_structured({"path": path, "id": str(node_id)})


class CopyNode(YTToolBase):
    _MUTABLE = True

    def on_handle_request(
        self,
        *,
        cluster: Annotated[
            str,
            YTToolBase.ToolInputField(
                description="Cluster",
            )
        ],
        source_path: Annotated[
            str,
            YTToolBase.ToolInputField(
                description="Source node path",
            )
        ],
        destination_path: Annotated[
            str,
            YTToolBase.ToolInputField(
                description="Destination node path",
            )
        ],
        recursive: Annotated[
            bool,
            YTToolBase.ToolInputField(
                description="Create missing intermediate nodes of the destination path",
                default=False,
            )
        ] = False,
        force: Annotated[
            bool,
            YTToolBase.ToolInputField(
                description="Overwrite the destination if it already exists",
                default=False,
            )
        ] = False,
        request_context,
        **kwargs,
    ):
        "A tool for copying a Cypress node (copy)."
        self.helper_check_write_path(destination_path)
        yt_client = self.runner.helper_get_yt_client(cluster, request_context)

        try:
            yt_client.copy(
                source_path, destination_path, recursive=recursive, force=force,
            )
        except Exception as e:
            self.helper_process_common_exception(e)

        return self.runner.return_structured({"source": source_path, "destination": destination_path})


class MoveNode(YTToolBase):
    _MUTABLE = True

    def on_handle_request(
        self,
        *,
        cluster: Annotated[
            str,
            YTToolBase.ToolInputField(
                description="Cluster",
            )
        ],
        source_path: Annotated[
            str,
            YTToolBase.ToolInputField(
                description="Source node path",
            )
        ],
        destination_path: Annotated[
            str,
            YTToolBase.ToolInputField(
                description="Destination node path",
            )
        ],
        recursive: Annotated[
            bool,
            YTToolBase.ToolInputField(
                description="Create missing intermediate nodes of the destination path",
                default=False,
            )
        ] = False,
        force: Annotated[
            bool,
            YTToolBase.ToolInputField(
                description="Overwrite the destination if it already exists",
                default=False,
            )
        ] = False,
        request_context,
        **kwargs,
    ):
        "A tool for moving (renaming) a Cypress node (move)."
        self.helper_check_write_path(source_path)
        self.helper_check_write_path(destination_path)
        yt_client = self.runner.helper_get_yt_client(cluster, request_context)

        try:
            yt_client.move(
                source_path, destination_path, recursive=recursive, force=force,
            )
        except Exception as e:
            self.helper_process_common_exception(e)

        return self.runner.return_structured({"source": source_path, "destination": destination_path})


class RemoveNode(YTToolBase):
    _MUTABLE = True

    def on_handle_request(
        self,
        *,
        cluster: Annotated[
            str,
            YTToolBase.ToolInputField(
                description="Cluster",
            )
        ],
        path: Annotated[
            str,
            YTToolBase.ToolInputField(
                description="Path to the node to remove",
            )
        ],
        recursive: Annotated[
            bool,
            YTToolBase.ToolInputField(
                description="Remove the whole subtree",
                default=False,
            )
        ] = False,
        force: Annotated[
            bool,
            YTToolBase.ToolInputField(
                description="Do not fail if the node does not exist",
                default=False,
            )
        ] = False,
        request_context,
        **kwargs,
    ):
        "A tool for removing a Cypress node (remove)."
        self.helper_check_write_path(path)
        yt_client = self.runner.helper_get_yt_client(cluster, request_context)

        try:
            yt_client.remove(path, recursive=recursive, force=force)
        except Exception as e:
            self.helper_process_common_exception(e)

        return self.runner.return_structured({"path": path, "removed": True})


class SetAttribute(YTToolBase):
    _MUTABLE = True

    def on_handle_request(
        self,
        *,
        cluster: Annotated[
            str,
            YTToolBase.ToolInputField(
                description="Cluster",
            )
        ],
        path: Annotated[
            str,
            YTToolBase.ToolInputField(
                description=(
                    "Path to a Cypress node. If `attribute` is given, the attribute on "
                    "this node is set. Otherwise the value of the node itself is set "
                    "(legacy form: //node/@attr in path also works)."
                ),
            )
        ],
        value: Annotated[
            Any,
            YTToolBase.ToolInputField(
                description="Value to set (JSON-compatible: string, number, bool, list or dict)",
            )
        ],
        attribute: Annotated[
            Optional[str],
            YTToolBase.ToolInputField(
                description=(
                    "Attribute name to set on `path` (e.g. \"my_attr\"). "
                    "If omitted, the value of the node itself is set."
                ),
                default=None,
            )
        ] = None,
        request_context,
        **kwargs,
    ):
        "A tool for setting a Cypress node value or attribute (set)."
        target_path = f"{path}/@{attribute}" if attribute else path
        self.helper_check_write_path(target_path)
        yt_client = self.runner.helper_get_yt_client(cluster, request_context)

        try:
            yt_client.set(target_path, value)
        except Exception as e:
            self.helper_process_common_exception(e)

        return self.runner.return_structured({"path": target_path})
