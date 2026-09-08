#pragma once

#include "public.h"

#include <yt/yt/server/lib/job_agent/proto/job_profile.pb.h>

#include <yt/yt/ytlib/controller_agent/persistence.h>

#include <yt/yt/ytlib/controller_agent/proto/job.pb.h>

#include <yt/yt/ytlib/scheduler/config.h>

#include <yt/yt/core/phoenix/context.h>
#include <yt/yt/core/phoenix/type_decl.h>
#include <yt/yt/core/phoenix/type_def.h>

#include <yt/yt/core/yson/consumer.h>

namespace NYT::NJobAgent {

using NPhoenix::TLoadContext;
using NPhoenix::TSaveContext;
using NPhoenix::TPersistenceContext;

////////////////////////////////////////////////////////////////////////////////

class TJobProfile
{
public:
    TJobProfile() = default;
    TJobProfile(
        NScheduler::EProfilingBinary profilingBinary,
        NScheduler::EProfilerType profilerType,
        double profilingProbability,
        TString blob = {});

    DEFINE_BYVAL_RO_PROPERTY(
        NScheduler::EProfilingBinary,
        ProfilingBinary,
        NScheduler::EProfilingBinary::JobProxy);
    DEFINE_BYVAL_RO_PROPERTY(
        NScheduler::EProfilerType,
        ProfilerType,
        NScheduler::EProfilerType::Cpu);
    DEFINE_BYVAL_RO_PROPERTY(double, ProfilingProbability, 0);
    DEFINE_BYREF_RO_PROPERTY(TString, Blob);

public:
    std::string GetType() const;

private:
    friend void ToProto(NProto::TJobProfile* protoProfile, const TJobProfile& profile);
    friend void FromProto(TJobProfile* profile, const NProto::TJobProfile& protoProfile);
};

void ToProto(NProto::TJobProfile* protoProfile, const TJobProfile& profile);
void FromProto(TJobProfile* profile, const NProto::TJobProfile& protoProfile);

////////////////////////////////////////////////////////////////////////////////

struct TTimeStatistics
{
    std::optional<TDuration> WaitingForResourcesDuration;
    std::optional<TDuration> PrepareDuration;
    std::optional<TDuration> ArtifactsCachingDuration;
    std::optional<TDuration> PrepareLayersDuration;
    std::optional<TDuration> PrepareRootFSDuration;
    std::optional<TDuration> PrepareNonRootVolumesDuration;
    std::optional<TDuration> LinkVolumesDuration;
    std::optional<TDuration> PrepareGpuCheckFSDuration;
    std::optional<TDuration> ValidateRootFSDuration;
    std::optional<TDuration> ExecDuration;
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
