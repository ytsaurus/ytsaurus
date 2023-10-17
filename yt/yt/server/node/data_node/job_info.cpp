#include "job_info.h"

namespace NYT::NDataNode {

using namespace NJobAgent;
using namespace NClusterNode;

////////////////////////////////////////////////////////////////////////////////

TBriefJobInfo::TBriefJobInfo(
    NJobTrackerClient::TJobId jobId,
    NJobAgent::EJobState jobState,
    NJobAgent::EJobType jobType,
    TString jobTrackerAddress,
    TInstant jobStartTime,
    TDuration jobDuration,
    const NClusterNode::TJobResources& baseResourceUsage,
    const NClusterNode::TJobResources& additionalResourceUsage,
    const std::vector<int>& jobPorts)
    : JobId_(jobId)
    , JobState_(jobState)
    , JobType_(jobType)
    , JobTrackerAddress_(std::move(jobTrackerAddress))
    , JobStartTime_(jobStartTime)
    , JobDuration_(jobDuration)
    , BaseResourceUsage_(baseResourceUsage)
    , AdditionalResourceUsage_(additionalResourceUsage)
    , JobPorts_(jobPorts)
{ }

void TBriefJobInfo::BuildOrchid(NYTree::TFluentMap fluent) const
{
    fluent.Item(ToString(JobId_)).BeginMap()
        .Item("job_state").Value(JobState_)
        .Item("job_type").Value(JobType_)
        .Item("job_tracker_address").Value(JobTrackerAddress_)
        .Item("start_time").Value(JobStartTime_)
        .Item("duration").Value(JobDuration_)
        .Item("base_resource_usage").Value(BaseResourceUsage_)
        .Item("additional_resource_usage").Value(AdditionalResourceUsage_)
        .Item("job_ports").Value(JobPorts_)
    .EndMap();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode
