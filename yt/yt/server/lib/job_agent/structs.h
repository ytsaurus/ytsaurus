#pragma once

#include "public.h"

#include <yt/yt/ytlib/controller_agent/persistence.h>

#include <yt/yt/ytlib/controller_agent/proto/job.pb.h>

#include <yt/yt/core/phoenix/context.h>
#include <yt/yt/core/phoenix/type_decl.h>
#include <yt/yt/core/phoenix/type_def.h>

#include <yt/yt/core/yson/consumer.h>

namespace NYT::NJobAgent {

using NPhoenix::TLoadContext;
using NPhoenix::TSaveContext;
using NPhoenix::TPersistenceContext;

////////////////////////////////////////////////////////////////////////////////

struct TJobProfile
{
    TString Type;
    TString Blob;
    double ProfilingProbability;
};

////////////////////////////////////////////////////////////////////////////////

struct TTimeStatistics
{
    std::optional<TDuration> WaitingForResourcesDuration;
    std::optional<TDuration> PrepareDuration;
    std::optional<TDuration> ArtifactsDownloadDuration;
    std::optional<TDuration> PrepareRootFSDuration;
    std::optional<TDuration> ExecDuration;
    std::optional<TDuration> PrepareGpuCheckFSDuration;
    std::optional<TDuration> GpuCheckDuration;

    void AddSamplesTo(TStatistics* statistics) const;

    bool IsEmpty() const;

    using TLoadContext = NControllerAgent::TLoadContext;
    using TSaveContext = NControllerAgent::TSaveContext;
    using TPersistenceContext = NControllerAgent::TPersistenceContext;

    PHOENIX_DECLARE_TYPE(TTimeStatistics, 0x14ce2d22);
};

void ToProto(
    NControllerAgent::NProto::TTimeStatistics* timeStatisticsProto,
    const TTimeStatistics& timeStatistics);
void FromProto(
    TTimeStatistics* timeStatistics,
    const NControllerAgent::NProto::TTimeStatistics& timeStatisticsProto);

void Serialize(const TTimeStatistics& timeStatistics, NYson::IYsonConsumer* consumer);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobAgent
