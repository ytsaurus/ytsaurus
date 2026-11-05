#pragma once

#include <yt/yt/ytlib/controller_agent/public.h>

#include <yt/yt/ytlib/scheduler/public.h>

#include <yt/yt/core/misc/serialize.h>

#include <yt/yt/core/phoenix/context.h>
#include <yt/yt/core/phoenix/type_decl.h>

#include <yt/yt/core/ytree/node.h>

#include <yt/yt/ytlib/scheduler/job_resources_helpers.h>

namespace NYT::NSchedulerSimulator {

////////////////////////////////////////////////////////////////////////////////

struct TJobDescription
{
    TDuration Duration;
    NScheduler::TJobResources ResourceLimits;
    NControllerAgent::TJobId Id;
    NJobTrackerClient::EJobType Type;
    std::string State;

    using TSaveContext = NPhoenix::TSaveContext;
    using TLoadContext = NPhoenix::TLoadContext;

    PHOENIX_DECLARE_TYPE(TJobDescription, 0x3a17c8d2);
};

void Deserialize(TJobDescription& value, NYTree::INodePtr node);

////////////////////////////////////////////////////////////////////////////////

struct TOperationDescription
{
    NScheduler::TOperationId Id;
    std::vector<TJobDescription> JobDescriptions;
    TInstant StartTime;
    TDuration Duration;
    std::string AuthenticatedUser;
    NScheduler::EOperationType Type;
    std::string State;
    bool InTimeframe;
    NYson::TYsonString Spec;

    using TSaveContext = NPhoenix::TSaveContext;
    using TLoadContext = NPhoenix::TLoadContext;

    PHOENIX_DECLARE_TYPE(TOperationDescription, 0x7b4e91f5);
};

void Deserialize(TOperationDescription& value, NYTree::INodePtr node);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSchedulerSimulator
