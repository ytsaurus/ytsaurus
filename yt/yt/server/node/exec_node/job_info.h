#pragma once

#include "volume_manager.h"

#include <yt/yt/server/node/job_agent/job_resource_manager.h>

#include <yt/yt/server/lib/exec_node/public.h>

#include <yt/yt/server/lib/misc/job_report.h>

namespace NYT::NExecNode {

////////////////////////////////////////////////////////////////////////////////

class TBriefJobInfo
{
public:
    void BuildOrchid(NYTree::TFluentMap fluent) const;

private:
    TJobId JobId_;
    TOperationId OperationId_;

    EJobState JobState_;
    EJobPhase JobPhase_;

    EJobType JobType_;

    bool Stored_;

    int JobSlotIndex_;

    TInstant JobStartTime_;
    TDuration JobDuration_;

    NYson::TYsonString JobStatistics_;

    NClusterNode::TJobResources JobResourceUsage_;

    TJobEvents JobEvents_;

    NControllerAgent::TCoreInfos JobCoreInfos_;

    TExecAttributes JobExecAttributes_;

    friend class TJob;

    TBriefJobInfo(
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
        const TExecAttributes& jobExecAttributes);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
