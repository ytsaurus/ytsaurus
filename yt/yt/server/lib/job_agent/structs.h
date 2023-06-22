#pragma once

#include "public.h"

#include <yt/yt/ytlib/controller_agent/proto/job.pb.h>

#include <yt/yt/core/yson/consumer.h>

namespace NYT::NJobAgent {

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
    std::optional<TDuration> PrepareDuration;
    std::optional<TDuration> ArtifactsDownloadDuration;
    std::optional<TDuration> PrepareRootFSDuration;
    std::optional<TDuration> ExecDuration;
    std::optional<TDuration> GpuCheckDuration;

    void Persist(const TStreamPersistenceContext& context);

    void AddSamplesTo(TStatistics* statistics) const;

    bool IsEmpty() const;
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
