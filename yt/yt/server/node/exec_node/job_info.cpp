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
    const TJobEvents& jobEvents,
    const NControllerAgent::TCoreInfos& jobCoreInfos,
    const TExecAttributes& jobExecAttributes)
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
    , JobEvents_(jobEvents)
    , JobCoreInfos_(jobCoreInfos)
    , JobExecAttributes_(jobExecAttributes)
{ }

void TBriefJobInfo::BuildOrchid(TFluentMap fluent) const
{
    fluent
        .Item("operation_id").Value(OperationId_)
        .Item("job_state").Value(JobState_)
        .Item("job_phase").Value(JobPhase_)
        .Item("job_type").Value(JobType_)
        .Item("stored").Value(Stored_)
        .Item("interrupted").Value(Interrupted_)
        .Item("slot_index").Value(JobSlotIndex_)
        .Item("start_time").Value(JobStartTime_)
        .Item("duration").Value(JobDuration_)
        .OptionalItem("statistics", JobStatistics_)
        .Item("base_resource_usage").Value(BaseResourceUsage_)
        .Item("additional_resource_usage").Value(AdditionalResourceUsage_)
        .Item("initial_resource_demand").Value(InitialResourceDemand_)
        .Item("job_ports").Value(JobPorts_)
        .Item("events").Value(JobEvents_)
        .Item("core_infos").Value(JobCoreInfos_)
        .Item("exec_attributes").Value(JobExecAttributes_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
