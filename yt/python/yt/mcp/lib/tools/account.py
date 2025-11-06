from .helpers import YTToolBase


class CheckPermissions(YTToolBase):
    def get_tool_description(self):
        return (
            self.ToolName(
                name="check_permission",
                description="""
Check user real permission to access (read, write, use) specific path.
Result is allow or deny (stored in action filed) by which rule (stored in subject_name field)
""",
            ),
            [
                self.ToolInputField(
                    name="user_login",
                    description="User login",
                ),
                self.ToolInputField(
                    name="permission",
                    description="Which permission needs to be checked. One of read, write, administer, create, use",
                ),
                self.ToolInputField(
                    name="path",
                    description="Path to object being checked",
                ),
                self.ToolInputField(
                    name="cluster",
                    description="Cluster name",
                ),
            ]
        )

    def on_handle_request(self, user_login, permission, path, cluster, request_context):
        yt_client = self.runner.helper_get_yt_client(cluster, request_context)
        try:
            result = yt_client.check_permission(
                user=user_login,
                permission=permission,
                path=path,
            )
        except Exception as ex:
            self.helper_process_common_exception(ex)

        return self.runner.return_structured(result)


class AccountProperty(YTToolBase):
    def get_tool_description(self):
        return (
            self.ToolName(
                name="get_account_property",
                description="""
Get some account properties.
""",
            ),
            [
                self.ToolInputField(
                    name="account",
                    description="Account name",
                ),
                self.ToolInputField(
                    name="property",
                    description="""Property to retrieve.
If set to "childrens" returns tree of children accounts.
""",
                ),
                self.ToolInputField(
                    name="cluster",
                    description="Cluster name",
                ),
            ]
        )

    def on_handle_request(self, account, property, cluster, request_context):
        yt_client = self.runner.helper_get_yt_client(cluster, request_context)

        try:
            if property == "childrens":
                result = yt_client.get(f"//sys/accounts/{account}")
            else:
                raise ValueError("No valid property")
        except Exception as ex:
            self.helper_process_common_exception(ex, raise_exception=True)

        return self.runner.return_structured(result)
