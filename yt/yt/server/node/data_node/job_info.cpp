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
    const NClusterNode::TJobResources& jobResourceUsage)
    : JobId_(jobId)
    , JobState_(jobState)
    , JobType_(jobType)
    , JobTrackerAddress_(std::move(jobTrackerAddress))
    , JobStartTime_(jobStartTime)
    , JobDuration_(jobDuration)
    , JobResourceUsage_(jobResourceUsage)
{ }

void TBriefJobInfo::BuildOrchid(NYTree::TFluentMap fluent) const
{
    fluent.Item(ToString(JobId_)).BeginMap()
        .Item("job_state").Value(JobState_)
        .Item("job_type").Value(JobType_)
        .Item("job_tracker_address").Value(JobTrackerAddress_)
        .Item("start_time").Value(JobStartTime_)
        .Item("duration").Value(JobDuration_)
        .Item("resource_usage").Value(JobResourceUsage_)
    .EndMap();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode
