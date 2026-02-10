RESOURCE_REQUEST_DESCRIPTION = """
You can learn more about quotas in the [documentation](https://ytsaurus.tech/docs/en/user-guide/storage/quotas).
"""

JOB_STATISTICS_DOCUMENTATION_URL = "https://ytsaurus.tech/docs/en/user-guide/problems/jobstatistics"
INTEGRAL_GUARANTEES_DOCUMENTATION_URL = (
    "https://ytsaurus.tech/docs/en/user-guide/data-processing/scheduler/integral-guarantees"
)

BUNDLE_UI_DASHBOARD_DEFAULT_CLUSTER = ".*"
BUNDLE_UI_DASHBOARD_DEFAULT_TABLET_CELL_BUNDLE = "sys"
BUNDLE_UI_DASHBOARD_DEFAULT_PROXY_ROLE = "default"

MASTER_LOCAL_DASHBOARD_DEFAULT_CLUSTER = ".*"
MASTER_LOCAL_DASHBOARD_DEFAULT_CONTAINER = "ms-.*"

MASTER_GLOBAL_DASHBOARD_DEFAULT_CLUSTER = ".*"

CYPRESS_PROXIES_DASHBOARD_DEFAULT_CLUSTER = ".*"
CYPRESS_PROXIES_DASHBOARD_DEFAULT_HOST = ".*"

MASTER_MERGE_JOBS_DASHBOARD_DEFAULT_CLUSTER = ".*"

KEY_FILTER_DASHBOARD_DEFAULT_CLUSTER = ".*"

CLUSTER_RESOURCES_DASHBOARD_DEFAULT_CLUSTER = ".*"
CLUSTER_RESOURCES_DASHBOARD_DEFAULT_TREE = "default"

SCHEDULER_DASHBOARD_DEFAULT_TREE = "default"
SCHEDULER_DASHBOARD_DEFAULT_POOL = "<Root>"
SCHEDULER_DASHBOARD_DEFAULT_CLUSTER = ".*"

HTTP_PROXIES_DASHBOARD_DEFAULT_CLUSTER = ".*"
HTTP_PROXIES_DASHBOARD_DEFAULT_PROXY_ROLE = "default"
HTTP_PROXIES_DASHBOARD_DEFAULT_HOST = ".*"

QUEUE_DASHBOARD_DEFAULT_CLUSTER = ".*"
QUEUE_DASHBOARD_DEFAULT_PATH = ".*"
QUEUE_DASHBOARD_DEFAULT_TAG = ".*|"
QUEUE_CONSUMER_DASHBOARD_DEFAULT_CLUSTER = ".*"
QUEUE_CONSUMER_DASHBOARD_DEFAULT_PATH = ".*"
QUEUE_CONSUMER_DASHBOARD_DEFAULT_TAG = ".*|"

MASTER_DISK_USAGE_DESCRIPTION = """
Disk space usage and limit represents raw space available for account and total usage of this raw space.
If you are running out of quota, you can contact your administrator.
"""

MASTER_CHUNK_USAGE_DESCRIPTION = """
All data stored in tables and files are split into parts called [chunks](https://ytsaurus.tech/docs/en/user-guide/storage/chunks).
**Nodes** - is a number of objects in the account (tables, files, folders, locks, etc.).
If you are running out of quota, you can contact your administrator.
"""
