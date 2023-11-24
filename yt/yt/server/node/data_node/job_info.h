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
    NChunkServer::TJobId JobId_;

    NJobAgent::EJobState JobState_;
    NJobAgent::EJobType JobType_;

    TString JobTrackerAddress_;

    TInstant JobStartTime_;
    TDuration JobDuration_;

    NClusterNode::TJobResources BaseResourceUsage_;
    NClusterNode::TJobResources AdditionalResourceUsage_;

    std::vector<int> JobPorts_;

    friend class TMasterJobBase;

    TBriefJobInfo(
        NChunkServer::TJobId jobId,
        NJobAgent::EJobState jobState,
        NJobAgent::EJobType jobType,
        TString jobTrackerAddress,
        TInstant jobStartTime,
        TDuration jobDuration,
        const NClusterNode::TJobResources& baseResourceUsage,
        const NClusterNode::TJobResources& additionalResourceUsage,
        const std::vector<int>& jobPorts);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode
