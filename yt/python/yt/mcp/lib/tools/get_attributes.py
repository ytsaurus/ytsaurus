from .helpers import YTToolBase


_ATTRIBUTES_COMMON = ("inherit_acl", "effective_acl")

_ATTRIBUTES_MAIN = _ATTRIBUTES_COMMON + ("replication_factor", "erasure_codec", "resource_usage", "recursive_resource_usage", "resource_usage")

_ATTRIBUTES_BUNDLE = _ATTRIBUTES_COMMON + ("resource_limits", "resource_quota")

_ATTRIBUTES_POOL = ("strong_guarantee_resources", "integral_guaranties", "max_operation_count", "max_running_operation_count")

_ATTRIBUTES_ACCOUNT = _ATTRIBUTES_COMMON + ("abc", "resource_limits", "resource_usage")

_ATTRIBUTES_DESCRIPTION_COMMON = """
For "inherit_acl" returns flag which means inherrinance acl from upper level.

For "effective_acl" returns acl (access control list) which specify permissions ("use", "read", "write", "remove", "administer", "modify children", "modify")
and inheritance mode for different subjects (users, groups, idm groups).
"""


class GetAttributes(YTToolBase):
    def get_tool_description(self):
        return (
            self.ToolName(
                name="get_attributes",
                description="""
A tool for getting named attributes from cluster node or cluster path or cluster account or cluster bundle.
""",
            ),
            [
                self.ToolInputField(
                    field_type=str,
                    name="path",
                    description="Path to cluster directory or cluster node. The path must start from '//'",
                    default=None,
                ),
                self.ToolInputField(
                    field_type=str,
                    name="account",
                    description="Account name on cluster. String without whitespace.",
                    default=None,
                ),
                self.ToolInputField(
                    field_type=str,
                    name="bundle",
                    description="Bundle name on cluster. String without whitespaces.",
                    default=None,
                ),
                self.ToolInputField(
                    field_type=str,
                    name="pool",
                    description="Pool name on cluster. Unique name inside specific pool tree. String without whitespaces.",
                    default=None,
                ),
                self.ToolInputField(
                    field_type=str,
                    name="pool_tree",
                    description="Name of pool tree on cluster. String without whitespaces.",
                    default="physical",
                ),
                self.ToolInputField(
                    field_type=list[str],
                    name="attributes",
                    description="Attributes names. One of " + ",".join(_ATTRIBUTES_MAIN)
                ),
                self.ToolInputField(
                    field_type=str,
                    name="cluster",
                    description=f"Cluster name. One of: {self.runner.helper_get_public_clusters(delimeter=', ')}",
                    examples=self.runner.helper_get_public_clusters(),
                )
            ]
        )

    def get_tool_variants(self):
        return [
            {
                "name": "account",
                "description": "A tool for getting named attributes from cluster account.",
                "input": [
                    {"name": "account"},
                    {
                        "name": "attributes",
                        "description": "Attribute/property names. One of " + ",".join(_ATTRIBUTES_ACCOUNT) + _ATTRIBUTES_DESCRIPTION_COMMON + """
For attribute "resource_usage" returns account resources such as "node count"; "chunk count"; "tablet count";
"tablet static memory"; total "disk space" in KiB and detalization by different mediums: "default" (means hdd space in KiB), "ssd blobs" (means ssd space in KiB), "ssd journals" (means ssd space in KiB);
total "master memory" and detalization by "chunk host", "per cell" with detalization by specific cell id;
"detailed master memory" with detalization by "nodes", "chunks", "attributes", "tables", "schemas".
Output MUST retund in TiB.

For attribute "resource_limits" returns account limits such as "node count"; "chunk count"; "tablet count"; "tablet static memory";
"disk space per medium" with detalization by "default" (means hdd space in KiB), "ssd_blobs" (means ssd space in KiB), "ssd_journals" (means ssd space in KiB);
"disk space" (total, any medium in KiB); "master memory" total and with detalization by "chunk host" and by specific cell id.

For attribute "abc" returns "abc" entity properties such as "id", "name" (human readable), "slug" (computer readable).
"""  # noqa
                    },
                    {"name": "cluster"},
                ]
            },
            {
                "name": "account_limits_disk",
                "description": """
A tool for getting account quota. It means disk quota and free space (hdd and ssd)

`resource_limits.disk_space_per_medium.default` is account quota for HDD in KiB.
`resource_limits.disk_space_per_medium.ssd_blobs` as account quota for SSD in KiB.
`resource_usage.disk_space_per_medium.default` as account usage for HDD in KiB.
`resource_usage.disk_space_per_medium.ssd_blobs` as account usage for SSD in KiB.

Show only disk quota and free space, without other resources such as node count, tablet count or tablet static memory).
Output example

Account name
    Account HDD quota (in TiB)
    Account SSD quota (in TiB)
    Free HDD (diff between account quota and usage, convert it to TiB)
    Free SSD (diff between account quota and usage, convert it to TiB)



""",
                "input": [
                    {"name": "account"},
                    {
                        "name": "attributes",
                        "description": "Attributes/property names. Simultantly \"resource_usage\" and \"resource_limits\"",
                        # "default": ["resource_usage", "resource_limits"],
                    },
                    {"name": "cluster"},
                ]
            },
            {
                "name": "bundle",
                "description": """A tool for getting named attributes from cluster bundle."

Returns resource limits with detalization by "tablet count", "tablet static memory".
Returns resource quota with detalization by "cpu" (by cores), "memory".
""",
                "input": [
                    {"name": "bundle"},
                    {
                        "name": "attributes",
                        "description": "Attribute/property names. One of " + ",".join(_ATTRIBUTES_BUNDLE) + _ATTRIBUTES_DESCRIPTION_COMMON,
                    },
                    {"name": "cluster"},
                ]
            },
            {
                "name": "pool",
                "description": "A tool for getting named attributes from cluster pool.",
                "input": [
                    {"name": "pool"},
                    {"name": "pool_tree"},
                    {
                        "name": "attributes",
                        "description": "Attribute/property names. One of " + ",".join(_ATTRIBUTES_POOL) + _ATTRIBUTES_DESCRIPTION_COMMON,
                    },
                    {"name": "cluster"},
                ]
            }
        ]

    def on_handle_request(
        self,
        *,
        cluster,
        attributes,
        path=None,
        account=None,
        bundle=None,
        pool=None,
        pool_tree=None,
        request_context,
        **kwargs
    ):
        self.runner._logger.debug(f"Starting {self}")
        yt_client = self.runner.helper_get_yt_client(cluster, request_context)

        try:
            if path:
                pass
            elif account:
                path = f"//sys/accounts/{account}"
            elif bundle:
                path = f"//sys/tablet_cell_bundles/{bundle}"
            elif pool:
                pool_tree = pool_tree if pool_tree else "physical"
                path = f"//sys/scheduler/orchid/scheduler/pool_trees/{pool_tree}/pools/{pool}"
            else:
                raise ValueError("No input path")

            if "." in path or " " in path or "@" in path:
                raise ValueError("Parameter should not contains white space or dots")

            if pool:
                # TODO: move to GetPoolAttributes
                all_attrs = yt_client.get(f"{path}")
                attrs = dict((k, all_attrs[k]) for k in attributes)
            else:
                attrs = yt_client.get(f"{path}/@", attributes=attributes)
            return self.runner.return_structured(attrs)
        except Exception as ex:
            self.helper_process_common_exception(ex)
