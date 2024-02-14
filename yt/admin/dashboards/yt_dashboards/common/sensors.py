from yt_dashboard_generator.sensor import Sensor
from yt_dashboard_generator.backends.monitoring import MonitoringTag
from yt_dashboard_generator.backends.grafana import GrafanaTag

from enum import Enum, auto

##################################################################


class YtSystemTags(Enum):
    HostContainer = auto()


yt_host = YtSystemTags.HostContainer

##################################################################


class ProjectSensorBase:
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if self.cls_monitoring_service is not None:
            self.tags[MonitoringTag("service")] = self.cls_monitoring_service
        if self.cls_grafana_service is not None:
            self.tags[GrafanaTag("service")] = self.cls_grafana_service


def ProjectSensor(monitoring_service=None, grafana_service=None, base=Sensor):
    return type(
        f'ProjectSensor<{base.__qualname__}>[{monitoring_service}]',
        (ProjectSensorBase, base),
        dict(
            cls_grafana_service=grafana_service,
            cls_monitoring_service=monitoring_service,
        ),
    )


class RpcBase(Sensor):
    def __init__(self, *args, **kwargs):
        super(RpcBase, self).__init__(*args, **kwargs)

    def service_method(self, service, method):
        return (self
            .value("yt_service", service)  # noqa: E128
            .value("method", method))  # noqa: E128

##################################################################


# Schedulers.
Scheduler =         ProjectSensor("scheduler",          "yt-scheduler")  # noqa: E222
SchedulerMemory =   ProjectSensor("scheduler_memory",   "yt-scheduler")  # noqa: E222
SchedulerCpu =      ProjectSensor("scheduler_cpu",      "yt-scheduler")  # noqa: E222
SchedulerPools =    ProjectSensor("scheduler_pools",    "yt-scheduler")  # noqa: E222
SchedulerInternal = ProjectSensor("scheduler_internal", "yt-scheduler")  # noqa: E222
SchedulerRpc =      ProjectSensor("scheduler_rpc",      "yt-scheduler", base=RpcBase)  # noqa: E222

# Controller agents.
CA =         ProjectSensor("controller_agent",          "yt-controller-agent")  # noqa: E222
CAMemory =   ProjectSensor("controller_agent_memory",   "yt-controller-agent")  # noqa: E222
CACpu =      ProjectSensor("controller_agent_cpu",      "yt-controller-agent")  # noqa: E222
CAInternal = ProjectSensor("controller_agent_internal", "yt-controller-agent")  # noqa: E222
CARpc =      ProjectSensor("controller_agent_rpc",      "yt-controller-agent", base=RpcBase)  # noqa: E222

# Ye olde Nodes. Used for old pre-flavored dashboards in a compat fashion. Prefer using specific flavored node sensors.
Node =          ProjectSensor("tab_node|exe_node|node",                                  "yt-data-node|yt-exec-node|yt-tablet-node")  # noqa: E222
NodeMemory =    ProjectSensor("tab_node_memory|exe_node_memory|node_memory",             "yt-data-node|yt-exec-node|yt-tablet-node")  # noqa: E222
NodeCpu =       ProjectSensor("tab_node_cpu|exe_node_cpu|node_cpu",                      "yt-data-node|yt-exec-node|yt-tablet-node")  # noqa: E222
NodeLocation =  ProjectSensor("dat_node_location|node_location",                         "yt-data-node|yt-exec-node|yt-tablet-node")  # noqa: E222
NodeInternal =  ProjectSensor("tab_node_internal|exe_node_internal|node_internal",       "yt-data-node|yt-exec-node|yt-tablet-node")  # noqa: E222
NodePorto =     ProjectSensor("node_porto",                                              "yt-data-node|yt-exec-node|yt-tablet-node")  # noqa: E222
NodeUserJob =   ProjectSensor("node_user_job",                                           "yt-data-node|yt-exec-node|yt-tablet-node")  # noqa: E222
NodeRpc =       ProjectSensor("tab_node_rpc|exe_node_rpc|node_rpc",                      "yt-data-node|yt-exec-node|yt-tablet-node", base=RpcBase)  # noqa: E222
NodeRpcClient = ProjectSensor("tab_node_rpc_client|exe_node_rpc_client|node_rpc_client", "yt-data-node|yt-exec-node|yt-tablet-node", base=RpcBase)  # noqa: E222
NodeMonitor =   ProjectSensor("node_monitor",                                            "yt-node-monitor")  # noqa: E222

# Data nodes.
DatNode =          ProjectSensor("dat_node",            "yt-data-node")  # noqa: E222
DatNodeMemory =    ProjectSensor("dat_node_memory",     "yt-data-node")  # noqa: E222
DatNodeCpu =       ProjectSensor("dat_node_cpu",        "yt-data-node")  # noqa: E222
DatNodeLocation =  ProjectSensor("dat_node_location",   "yt-data-node")  # noqa: E222
DatNodeInternal =  ProjectSensor("dat_node_internal",   "yt-data-node")  # noqa: E222
DatNodePorto =     ProjectSensor("dat_node_porto",      "yt-data-node")  # noqa: E222
DatNodeRpc =       ProjectSensor("dat_node_rpc",        "yt-data-node", base=RpcBase)  # noqa: E222
DatNodeRpcClient = ProjectSensor("dat_node_rpc_client", "yt-data-node", base=RpcBase)  # noqa: E222

# Exec nodes.
ExeNode =          ProjectSensor("exe_node",            "yt-exec-node")  # noqa: E222
ExeNodeMemory =    ProjectSensor("exe_node_memory",     "yt-exec-node")  # noqa: E222
ExeNodeCpu =       ProjectSensor("exe_node_cpu",        "yt-exec-node")  # noqa: E222
ExeNodeInternal =  ProjectSensor("exe_node_internal",   "yt-exec-node")  # noqa: E222
ExeNodePorto =     ProjectSensor("exe_node_porto",      "yt-exec-node")  # noqa: E222
ExeNodeUserJob =   ProjectSensor("exe_node_user_job",   "yt-exec-node")  # noqa: E222
ExeNodeRpc =       ProjectSensor("exe_node_rpc",        "yt-exec-node", base=RpcBase)  # noqa: E222
ExeNodeRpcClient = ProjectSensor("exe_node_rpc_client", "yt-exec-node", base=RpcBase)  # noqa: E222

# Tablet nodes.
TabNode =          ProjectSensor("tab_node",            "yt-tablet-node")  # noqa: E222
TabNodeMemory =    ProjectSensor("tab_node_memory",     "yt-tablet-node")  # noqa: E222
TabNodeCpu =       ProjectSensor("tab_node_cpu",        "yt-tablet-node")  # noqa: E222
TabNodeInternal =  ProjectSensor("tab_node_internal",   "yt-tablet-node")  # noqa: E222
TabNodePorto =     ProjectSensor("tab_node_porto",      "yt-tablet-node")  # noqa: E222
TabNodeRpc =       ProjectSensor("tab_node_rpc",        "yt-tablet-node", base=RpcBase)  # noqa: E222
TabNodeRpcClient = ProjectSensor("tab_node_rpc_client", "yt-tablet-node", base=RpcBase)  # noqa: E222

# Generic sensor for tablet metrics.
NodeTablet =       ProjectSensor("node_tablet",         "yt-tablet-node")  # noqa: E222

# Master.
Master =           ProjectSensor("master",            "yt-master")  # noqa: E222
MasterCpu =        ProjectSensor("master_cpu",        "yt-master")  # noqa: E222
MasterMemory =     ProjectSensor("master_memory",     "yt-master")  # noqa: E222
MasterInternal =   ProjectSensor("master_internal",   "yt-master")  # noqa: E222
MasterRpc =        ProjectSensor("master_rpc",        "yt-master")  # noqa: E222
MasterRpcClient =  ProjectSensor("master_rpc_client", "yt-master")  # noqa: E222

# Misc.
HttpProxy = ProjectSensor("http_proxy", "yt_proxies")
RpcProxy = ProjectSensor("rpc_proxy", "yt_rpc_proxies")
RpcProxyRpc = ProjectSensor("rpc_proxy_rpc", "yt_rpc_proxies", base=RpcBase)
RpcProxyInternal = ProjectSensor("rpc_proxy_internal", "yt_rpc_proxies")
RpcProxyCpu = ProjectSensor("rpc_proxy_cpu", "yt_rpc_proxies")
RpcProxyPorto =  ProjectSensor("rpc_proxy_porto", base=RpcBase)  # noqa: E222

# BundleController
BundleController = ProjectSensor("bundle_controller", "yt_bundle_controller")  # noqa: E222

# TabletBalancer
TabletBalancer = ProjectSensor("tablet_balancer", "yt_tablet_balancer")  # noqa: E222

# CHYT
Chyt = ProjectSensor("clickhouse", "chyt")


class SplitNodeSensorsGuard:
    _entered = False

    def __init__(self, dat=None, exe=None, tab=None, common=None):
        self.prefixes = {
            "Dat": dat,
            "Exe": exe,
            "Tab": tab,
            "": common,
        }

    def __enter__(self):
        assert not SplitNodeSensorsGuard._entered, "Split node guard cannot be nested"
        SplitNodeSensorsGuard._entered = True

        self.saved_services = {}
        for flavor in "Dat", "Exe", "Tab", "":
            if self.prefixes[flavor] is None:
                continue
            for name, cls in globals().items():
                if name.startswith(flavor + "Node") and isinstance(cls, type):
                    self.saved_services[cls] = cls.cls_monitoring_service
                    suffix = cls.cls_monitoring_service[4:]
                    assert suffix.startswith("node"), f"{cls.cls_monitoring_service}[4:] does not start with \"node\""
                    cls.cls_monitoring_service = "|".join(
                        prefix + ("_" if prefix else "") + suffix
                        for prefix in self.prefixes[flavor])

    def __exit__(self, *args):
        SplitNodeSensorsGuard._entered = False
        for cls, value in self.saved_services.items():
            cls.cls_monitoring_service = value
        pass
