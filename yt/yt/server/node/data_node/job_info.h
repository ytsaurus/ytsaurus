#pragma once

#include "public.h"

#include <yt/yt/server/node/job_agent/job_resource_manager.h>

#include <yt/yt/core/ytree/fluent.h>

#include <yt/yt/server/lib/misc/job_report.h>

namespace NYT::NDataNode {

////////////////////////////////////////////////////////////////////////////////

class TBriefJobInfo
{
public:
    void BuildOrchid(NYTree::TFluentMap fluent) const;

private:
    NJobTrackerClient::TJobId JobId_;

    NJobAgent::EJobState JobState_;
    NJobAgent::EJobType JobType_;

    TString JobTrackerAddress_;

    TInstant JobStartTime_;
    TDuration JobDuration_;

    NClusterNode::TJobResources JobResourceUsage_;

    friend class TMasterJobBase;

    TBriefJobInfo(
        NJobTrackerClient::TJobId jobId,
        NJobAgent::EJobState jobState,
        NJobAgent::EJobType jobType,
        TString jobTrackerAddress,
        TInstant jobStartTime,
        TDuration jobDuration,
        const NClusterNode::TJobResources& jobResourceUsage);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode
