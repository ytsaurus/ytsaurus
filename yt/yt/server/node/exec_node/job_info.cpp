#include "job_info.h"

namespace NYT::NExecNode {

////////////////////////////////////////////////////////////////////////////////

TBriefJobInfo::TBriefJobInfo(
    TJobId jobId,
    EJobState jobState,
    EJobPhase jobPhase,
    EJobType jobType,
    bool stored,
    int jobSlotIndex,
    TInstant jobStartTime,
    TDuration jobDuration,
    const NYson::TYsonString& jobStatistics,
    TOperationId operationId,
    const NClusterNode::TJobResources& jobResourceUsage,
    const TJobEvents& jobEvents,
    const NControllerAgent::TCoreInfos& jobCoreInfos,
    const TExecAttributes& jobExecAttributes)
    : JobId_(jobId)
    , OperationId_(operationId)
    , JobState_(jobState)
    , JobPhase_(jobPhase)
    , JobType_(jobType)
    , Stored_(stored)
    , JobSlotIndex_(jobSlotIndex)
    , JobStartTime_(jobStartTime)
    , JobDuration_(jobDuration)
    , JobStatistics_(jobStatistics)
    , JobResourceUsage_(jobResourceUsage)
    , JobEvents_(jobEvents)
    , JobCoreInfos_(jobCoreInfos)
    , JobExecAttributes_(jobExecAttributes)
{ }

void TBriefJobInfo::BuildOrchid(NYTree::TFluentMap fluent) const
{
    fluent
        .Item(ToString(JobId_)).BeginMap()
            .Item("job_state").Value(JobState_)
            .Item("job_phase").Value(JobPhase_)
            .Item("job_type").Value(JobType_)
            .Item("stored").Value(Stored_)
            .Item("slot_index").Value(JobSlotIndex_)
            .Item("start_time").Value(JobStartTime_)
            .Item("duration").Value(JobDuration_)
            .OptionalItem("statistics", JobStatistics_)
            .OptionalItem("operation_id", OperationId_)
            .Item("resource_usage").Value(JobResourceUsage_)
            .Item("events").Value(JobEvents_)
            .Item("core_infos").Value(JobCoreInfos_)
            .Item("exec_attributes").Value(JobExecAttributes_)
        .EndMap();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
