#include "job_info.h"

namespace NYT::NExecNode {

using namespace NClusterNode;
using namespace NYTree;
using namespace NServer;

////////////////////////////////////////////////////////////////////////////////

TBriefJobInfo::TBriefJobInfo(
    TJobId jobId,
    EJobState jobState,
    EJobPhase jobPhase,
    EJobType jobType,
    bool stored,
    bool interrupted,
    int jobSlotIndex,
    TInstant jobStartTime,
    TDuration jobDuration,
    const NYson::TYsonString& jobStatistics,
    TOperationId operationId,
    const TJobResources& baseResourceUsage,
    const TJobResources& additionalResourceUsage,
    const TJobResources& initialResourceDemand,
    const std::vector<int>& jobPorts,
    std::optional<int> jobProxyRpcServerPort,
    const TJobEvents& jobEvents,
    const NControllerAgent::TCoreInfos& jobCoreInfos,
    const TExecAttributes& jobExecAttributes,
    std::optional<std::string> monitoringDescriptor,
    TInstant jobResendBackoffStartTime)
    : JobId_(jobId)
    , OperationId_(operationId)
    , JobState_(jobState)
    , JobPhase_(jobPhase)
    , JobType_(jobType)
    , Stored_(stored)
    , Interrupted_(interrupted)
    , JobSlotIndex_(jobSlotIndex)
    , JobStartTime_(jobStartTime)
    , JobDuration_(jobDuration)
    , JobStatistics_(jobStatistics)
    , BaseResourceUsage_(baseResourceUsage)
    , AdditionalResourceUsage_(additionalResourceUsage)
    , InitialResourceDemand_(initialResourceDemand)
    , JobPorts_(jobPorts)
    , JobProxyRpcServerPort_(jobProxyRpcServerPort)
    , JobEvents_(jobEvents)
    , JobCoreInfos_(jobCoreInfos)
    , JobExecAttributes_(jobExecAttributes)
    , MonitoringDescriptor_(std::move(monitoringDescriptor))
    , JobResendBackoffStartTime_(jobResendBackoffStartTime)
{ }

void Serialize(const TBriefJobInfo& value, NYson::IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .BeginMap()
            .Item("operation_id").Value(value.OperationId_)
            .Item("job_state").Value(value.JobState_)
            .Item("job_phase").Value(value.JobPhase_)
            .Item("job_type").Value(value.JobType_)
            .Item("stored").Value(value.Stored_)
            .Item("interrupted").Value(value.Interrupted_)
            .Item("slot_index").Value(value.JobSlotIndex_)
            .Item("start_time").Value(value.JobStartTime_)
            .Item("duration").Value(value.JobDuration_)
            .OptionalItem("statistics", value.JobStatistics_)
            .Item("base_resource_usage").Value(value.BaseResourceUsage_)
            .Item("additional_resource_usage").Value(value.AdditionalResourceUsage_)
            .Item("initial_resource_demand").Value(value.InitialResourceDemand_)
            .Item("job_ports").Value(value.JobPorts_)
            .OptionalItem("job_proxy_rpc_server_port", value.JobProxyRpcServerPort_)
            .Item("events").Value(value.JobEvents_)
            .Item("core_infos").Value(value.JobCoreInfos_)
            .Item("exec_attributes").Value(value.JobExecAttributes_)
            .OptionalItem("monitoring_descriptor", value.MonitoringDescriptor_)
            .OptionalItem("job_resend_backoff_start_time", value.JobResendBackoffStartTime_)
        .EndMap();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
