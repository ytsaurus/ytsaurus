#pragma once

#include <yt/ytlib/scheduler/public.h>

#include <yt/core/misc/phoenix.h>

#include <yt/core/ytree/node.h>

#include <yt/ytlib/scheduler/job_resources.h>

namespace NYT::NSchedulerSimulator {

////////////////////////////////////////////////////////////////////////////////

struct TJobDescription
{
    TDuration Duration;
    NScheduler::TJobResources ResourceLimits;
    NScheduler::TJobId Id;
    NJobTrackerClient::EJobType Type;
    TString State;

    void Persist(const NPhoenix::TPersistenceContext& context);
};

void Deserialize(TJobDescription& value, NYTree::INodePtr node);

////////////////////////////////////////////////////////////////////////////////

struct TOperationDescription
{
    NScheduler::TOperationId Id;
    std::vector<TJobDescription> JobDescriptions;
    TInstant StartTime;
    TDuration Duration;
    TString AuthenticatedUser;
    NScheduler::EOperationType Type;
    TString State;
    bool InTimeframe;
    NYTree::IMapNodePtr Spec;

    void Persist(const NPhoenix::TPersistenceContext& context);
};

void Deserialize(TOperationDescription& value, NYTree::INodePtr node);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSchedulerSimulator
