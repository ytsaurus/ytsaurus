#pragma once

#include <yt/yt/ytlib/scheduler/public.h>

#include <yt/yt/core/misc/phoenix.h>

#include <yt/yt/core/ytree/node.h>

#include <yt/yt/ytlib/scheduler/job_resources_helpers.h>

namespace NYT::NSchedulerSimulator {

////////////////////////////////////////////////////////////////////////////////

struct TJobDescription
{
    TDuration Duration;
    NScheduler::TJobResources ResourceLimits;
    NScheduler::TJobId Id;
    NJobTrackerClient::EJobType Type;
    TString State;

    void Persist(const TStreamPersistenceContext& context);
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
    NYson::TYsonString Spec;

    void Persist(const TStreamPersistenceContext& context);
};

void Deserialize(TOperationDescription& value, NYTree::INodePtr node);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSchedulerSimulator
