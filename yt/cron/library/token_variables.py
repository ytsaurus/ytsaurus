import os
import enum
import collections


VariableWithDefault = collections.namedtuple("VariableWithDefault", ("name", "default"))


class TokenEnvironmentVariables(enum.Enum):
    YT_TOKEN = VariableWithDefault("YT_TOKEN_PATH", "/opt/cron/token")
    YT_MERGE_TOKEN = VariableWithDefault("YT_MERGE_TOKEN_PATH", "/opt/cron/merge_token")
    YT_RESOURCE_USAGE_TOKEN = VariableWithDefault("YT_RESOURCE_USAGE_TOKEN_PATH", "/opt/cron/resource_usage_token")
    YT_ACCESS_LOG_VIEWER_TOKEN = VariableWithDefault("YT_ACCESS_LOG_VIEWER_TOKEN_PATH", "/opt/cron/access_log_viewer_token")
    YT_BULK_ACL_CHECKER_TOKEN = VariableWithDefault("YT_BULK_ACL_CHECKER_TOKEN_PATH", "/opt/cron/bulk_acl_checker_token")
    YT_SNAP_TOKEN = VariableWithDefault("YT_SNAP_TOKEN_PATH", "/opt/cron/snap_token")
    DNS_STORAGE_TOKEN = VariableWithDefault("DNS_STORAGE_TOKEN_PATH", "/opt/cron/dns_storage_token")
    YP_TOKEN = VariableWithDefault("YP_TOKEN_PATH", "/opt/cron/yp_token")
    SOLOMON_TOKEN = VariableWithDefault("SOLOMON_TOKEN_PATH", "/opt/cron/solomon_token")
    INFRA_TOKEN = VariableWithDefault("INFRA_TOKEN_PATH", "/opt/cron/infra_token")
    TRANSFER_MANAGER_TOKEN = VariableWithDefault("TRANSFER_MANAGER_TOKEN_PATH", "/opt/cron/transfer_manager_token")
    STAFF_TOKEN = VariableWithDefault("YT_CRON_STAFF", "/opt/cron/staff_token")
    STATFACE_ROBOT_PASSWORD = VariableWithDefault("YT_CRON_STATFACE_ROBOT_PASSWORD_PATH", "/opt/cron/statface_robot_password")


TokenPaths = enum.Enum(
    "TokenPaths",
    {
        token.name: os.environ.get(token.value.name, token.value.default)
        for token in TokenEnvironmentVariables
    }
)


def replace_envar_str(target, source):
    if source in os.environ:
        os.environ[target] = os.environ[source]


def replace_envar(target, source):
    if isinstance(target, TokenEnvironmentVariables) and isinstance(source, TokenEnvironmentVariables):
        replace_envar_str(target.name, source.name)
        replace_envar_str(target.value.name, source.value.name)
    else:
        if isinstance(target, TokenEnvironmentVariables):
            target = target.name
        if isinstance(source, TokenEnvironmentVariables):
            source = source.name

        replace_envar_str(target, source)
