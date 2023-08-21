#include "structs.h"

#include <yt/yt/core/misc/statistics.h>
#include <yt/yt/core/misc/protobuf_helpers.h>

namespace NYT::NJobAgent {

using namespace NYTree;

using NYT::ToProto;
using NYT::FromProto;

////////////////////////////////////////////////////////////////////////////////

void TTimeStatistics::Persist(const TStreamPersistenceContext& context)
{
    using NYT::Persist;

    Persist(context, PrepareDuration);
    Persist(context, ArtifactsDownloadDuration);
    Persist(context, PrepareRootFSDuration);
    Persist(context, ExecDuration);
    Persist(context, GpuCheckDuration);
}

void TTimeStatistics::AddSamplesTo(TStatistics* statistics) const
{
    if (PrepareDuration) {
        statistics->AddSample("/time/prepare", PrepareDuration->MilliSeconds());
    }
    if (ArtifactsDownloadDuration) {
        statistics->AddSample("/time/artifacts_download", ArtifactsDownloadDuration->MilliSeconds());
    }
    if (PrepareRootFSDuration) {
        statistics->AddSample("/time/prepare_root_fs", PrepareRootFSDuration->MilliSeconds());
    }
    if (ExecDuration) {
        statistics->AddSample("/time/exec", ExecDuration->MilliSeconds());
    }
    if (GpuCheckDuration) {
        statistics->AddSample("/time/gpu_check", GpuCheckDuration->MilliSeconds());
    }
}

bool TTimeStatistics::IsEmpty() const
{
    return !PrepareDuration && !ArtifactsDownloadDuration && !PrepareRootFSDuration && !GpuCheckDuration;
}

void ToProto(
    NControllerAgent::NProto::TTimeStatistics* timeStatisticsProto,
    const TTimeStatistics& timeStatistics)
{
    if (timeStatistics.PrepareDuration) {
        timeStatisticsProto->set_prepare_duration(ToProto<i64>(*timeStatistics.PrepareDuration));
    }
    if (timeStatistics.ArtifactsDownloadDuration) {
        timeStatisticsProto->set_artifacts_download_duration(ToProto<i64>(*timeStatistics.ArtifactsDownloadDuration));
    }
    if (timeStatistics.PrepareRootFSDuration) {
        timeStatisticsProto->set_prepare_root_fs_duration(ToProto<i64>(*timeStatistics.PrepareRootFSDuration));
    }
    if (timeStatistics.ExecDuration) {
        timeStatisticsProto->set_exec_duration(ToProto<i64>(*timeStatistics.ExecDuration));
    }
    if (timeStatistics.GpuCheckDuration) {
        timeStatisticsProto->set_gpu_check_duration(ToProto<i64>(*timeStatistics.GpuCheckDuration));
    }
}

void FromProto(
    TTimeStatistics* timeStatistics,
    const NControllerAgent::NProto::TTimeStatistics& timeStatisticsProto)
{
    if (timeStatisticsProto.has_prepare_duration()) {
        timeStatistics->PrepareDuration = FromProto<TDuration>(timeStatisticsProto.prepare_duration());
    }
    if (timeStatisticsProto.has_artifacts_download_duration()) {
        timeStatistics->ArtifactsDownloadDuration = FromProto<TDuration>(timeStatisticsProto.artifacts_download_duration());
    }
    if (timeStatisticsProto.has_prepare_root_fs_duration()) {
        timeStatistics->PrepareRootFSDuration = FromProto<TDuration>(timeStatisticsProto.prepare_root_fs_duration());
    }
    if (timeStatisticsProto.has_exec_duration()) {
        timeStatistics->ExecDuration = FromProto<TDuration>(timeStatisticsProto.exec_duration());
    }
    if (timeStatisticsProto.has_gpu_check_duration()) {
        timeStatistics->GpuCheckDuration = FromProto<TDuration>(timeStatisticsProto.gpu_check_duration());
    }
}

void Serialize(const TTimeStatistics& timeStatistics, NYson::IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .BeginMap()
            .DoIf(static_cast<bool>(timeStatistics.PrepareDuration), [&] (auto fluent) {
                fluent.Item("prepare").Value(*timeStatistics.PrepareDuration);
            })
            .DoIf(static_cast<bool>(timeStatistics.ArtifactsDownloadDuration), [&] (auto fluent) {
                fluent.Item("artifacts_download").Value(*timeStatistics.ArtifactsDownloadDuration);
            })
            .DoIf(static_cast<bool>(timeStatistics.PrepareRootFSDuration), [&] (auto fluent) {
                fluent.Item("prepare_root_fs").Value(*timeStatistics.PrepareRootFSDuration);
            })
            .DoIf(static_cast<bool>(timeStatistics.ExecDuration), [&] (auto fluent) {
                fluent.Item("exec").Value(*timeStatistics.ExecDuration);
            })
            .DoIf(static_cast<bool>(timeStatistics.GpuCheckDuration), [&] (auto fluent) {
                fluent.Item("gpu_check").Value(*timeStatistics.GpuCheckDuration);
            })
        .EndMap();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobAgent
