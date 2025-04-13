#include "structs.h"

#include <yt/yt/ytlib/controller_agent/serialize.h>

#include <yt/yt/core/misc/statistics.h>
#include <yt/yt/core/misc/statistic_path.h>
#include <yt/yt/core/misc/protobuf_helpers.h>

namespace NYT::NJobAgent {

using namespace NYTree;

using NYT::ToProto;
using NYT::FromProto;

////////////////////////////////////////////////////////////////////////////////

void TTimeStatistics::AddSamplesTo(TStatistics* statistics) const
{
    using namespace NStatisticPath;

    if (WaitingForResourcesDuration) {
        statistics->AddSample("/time/wait_for_resources"_SP, WaitingForResourcesDuration->MilliSeconds());
    }
    if (PrepareDuration) {
        statistics->AddSample("/time/prepare"_SP, PrepareDuration->MilliSeconds());
    }
    if (ArtifactsDownloadDuration) {
        statistics->AddSample("/time/artifacts_download"_SP, ArtifactsDownloadDuration->MilliSeconds());
    }
    if (PrepareRootFSDuration) {
        statistics->AddSample("/time/prepare_root_fs"_SP, PrepareRootFSDuration->MilliSeconds());
    }
    if (ExecDuration) {
        statistics->AddSample("/time/exec"_SP, ExecDuration->MilliSeconds());
    }
    if (GpuCheckDuration) {
        statistics->AddSample("/time/gpu_check"_SP, GpuCheckDuration->MilliSeconds());
    }
}

bool TTimeStatistics::IsEmpty() const
{
    return
        !WaitingForResourcesDuration &&
        !PrepareDuration &&
        !ArtifactsDownloadDuration &&
        !PrepareRootFSDuration &&
        !PrepareGpuCheckFSDuration &&
        !GpuCheckDuration;
}

// TODO(pogorelov): Move TTimeStatistics to NControllerAgent.
void TTimeStatistics::RegisterMetadata(auto&& registrar)
{
    PHOENIX_REGISTER_FIELD(1, PrepareDuration);
    PHOENIX_REGISTER_FIELD(2, ArtifactsDownloadDuration);
    PHOENIX_REGISTER_FIELD(3, PrepareRootFSDuration);
    PHOENIX_REGISTER_FIELD(4, ExecDuration);
    PHOENIX_REGISTER_FIELD(7, PrepareGpuCheckFSDuration,
        .SinceVersion(NControllerAgent::ESnapshotVersion::PrepareGpuCheckFSDuration));
    PHOENIX_REGISTER_FIELD(5, GpuCheckDuration);

    PHOENIX_REGISTER_FIELD(6, WaitingForResourcesDuration,
        .SinceVersion(NControllerAgent::ESnapshotVersion::WaitingForResourcesDuration));
}

void ToProto(
    NControllerAgent::NProto::TTimeStatistics* timeStatisticsProto,
    const TTimeStatistics& timeStatistics)
{
    if (timeStatistics.WaitingForResourcesDuration) {
        timeStatisticsProto->set_waiting_for_resources_duration(ToProto(*timeStatistics.WaitingForResourcesDuration));
    }
    if (timeStatistics.PrepareDuration) {
        timeStatisticsProto->set_prepare_duration(ToProto(*timeStatistics.PrepareDuration));
    }
    if (timeStatistics.ArtifactsDownloadDuration) {
        timeStatisticsProto->set_artifacts_download_duration(ToProto(*timeStatistics.ArtifactsDownloadDuration));
    }
    if (timeStatistics.PrepareRootFSDuration) {
        timeStatisticsProto->set_prepare_root_fs_duration(ToProto(*timeStatistics.PrepareRootFSDuration));
    }
    if (timeStatistics.PrepareGpuCheckFSDuration) {
        timeStatisticsProto->set_prepare_gpu_check_fs_duration(ToProto(*timeStatistics.PrepareGpuCheckFSDuration));
    }
    if (timeStatistics.ExecDuration) {
        timeStatisticsProto->set_exec_duration(ToProto(*timeStatistics.ExecDuration));
    }
    if (timeStatistics.GpuCheckDuration) {
        timeStatisticsProto->set_gpu_check_duration(ToProto(*timeStatistics.GpuCheckDuration));
    }
}

void FromProto(
    TTimeStatistics* timeStatistics,
    const NControllerAgent::NProto::TTimeStatistics& timeStatisticsProto)
{
    if (timeStatisticsProto.has_waiting_for_resources_duration()) {
        timeStatistics->WaitingForResourcesDuration = FromProto<TDuration>(timeStatisticsProto.waiting_for_resources_duration());
    }
    if (timeStatisticsProto.has_prepare_duration()) {
        timeStatistics->PrepareDuration = FromProto<TDuration>(timeStatisticsProto.prepare_duration());
    }
    if (timeStatisticsProto.has_artifacts_download_duration()) {
        timeStatistics->ArtifactsDownloadDuration = FromProto<TDuration>(timeStatisticsProto.artifacts_download_duration());
    }
    if (timeStatisticsProto.has_prepare_root_fs_duration()) {
        timeStatistics->PrepareRootFSDuration = FromProto<TDuration>(timeStatisticsProto.prepare_root_fs_duration());
    }
    if (timeStatisticsProto.has_prepare_gpu_check_fs_duration()) {
        timeStatistics->PrepareGpuCheckFSDuration = FromProto<TDuration>(timeStatisticsProto.prepare_gpu_check_fs_duration());
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
            .DoIf(static_cast<bool>(timeStatistics.WaitingForResourcesDuration), [&] (auto fluent) {
                fluent.Item("wait_for_resources").Value(*timeStatistics.WaitingForResourcesDuration);
            })
            .DoIf(static_cast<bool>(timeStatistics.PrepareDuration), [&] (auto fluent) {
                fluent.Item("prepare").Value(*timeStatistics.PrepareDuration);
            })
            .DoIf(static_cast<bool>(timeStatistics.ArtifactsDownloadDuration), [&] (auto fluent) {
                fluent.Item("artifacts_download").Value(*timeStatistics.ArtifactsDownloadDuration);
            })
            .DoIf(static_cast<bool>(timeStatistics.PrepareRootFSDuration), [&] (auto fluent) {
                fluent.Item("prepare_root_fs").Value(*timeStatistics.PrepareRootFSDuration);
            })
            .DoIf(static_cast<bool>(timeStatistics.PrepareGpuCheckFSDuration), [&] (auto fluent) {
                fluent.Item("prepare_gpu_check_fs").Value(*timeStatistics.PrepareGpuCheckFSDuration);
            })
            .DoIf(static_cast<bool>(timeStatistics.ExecDuration), [&] (auto fluent) {
                fluent.Item("exec").Value(*timeStatistics.ExecDuration);
            })
            .DoIf(static_cast<bool>(timeStatistics.GpuCheckDuration), [&] (auto fluent) {
                fluent.Item("gpu_check").Value(*timeStatistics.GpuCheckDuration);
            })
        .EndMap();
}

PHOENIX_DEFINE_TYPE(TTimeStatistics);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobAgent
