#include "job_report.h"

#include "estimate_size_helpers.h"

#include <yt/yt/ytlib/job_tracker_client/proto/job.pb.h>

#include <yt/yt/core/ytree/fluent.h>

#include <yt/yt/core/misc/statistics.h>

namespace NYT::NJobAgent {

using namespace NYTree;
using namespace NYson;
using namespace NCoreDump;

using NYT::ToProto;
using NYT::FromProto;

using namespace NControllerAgent;

////////////////////////////////////////////////////////////////////////////////

void TTimeStatistics::Persist(const TStreamPersistenceContext& context)
{
    using namespace NYT::NJobAgent;
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

TJobEvent::TJobEvent(NJobTrackerClient::EJobState state)
    : Timestamp_(Now())
    , State_(state)
{ }

TJobEvent::TJobEvent(NJobTrackerClient::EJobPhase phase)
    : Timestamp_(Now())
    , Phase_(phase)
{ }

TJobEvent::TJobEvent(NJobTrackerClient::EJobState state, NJobTrackerClient::EJobPhase phase)
    : Timestamp_(Now())
    , State_(state)
    , Phase_(phase)
{ }

////////////////////////////////////////////////////////////////////////////////

void Serialize(const TJobEvents& events, NYson::IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .BeginList()
        .DoFor(events, [] (TFluentList fluent, const TJobEvent& event) {
            fluent.Item().BeginMap()
                .Item("time").Value(event.Timestamp())
                .OptionalItem("state", event.State())
                .OptionalItem("phase", event.Phase())
            .EndMap();
        })
        .EndList();
}

////////////////////////////////////////////////////////////////////////////////

class TYsonAttributesStripper
    : public IYsonConsumer
{
public:
    TYsonAttributesStripper(IYsonConsumer* output)
        : Output_(output)
    { }

    void OnStringScalar(TStringBuf value) override
    {
        if (AttributesDepth_ == 0) {
            Output_->OnStringScalar(value);
        }
    }

    void OnInt64Scalar(i64 value) override
    {
        if (AttributesDepth_ == 0) {
            Output_->OnInt64Scalar(value);
        }
    }

    void OnUint64Scalar(ui64 value) override
    {
        if (AttributesDepth_ == 0) {
            Output_->OnInt64Scalar(value);
        }
    }

    void OnDoubleScalar(double value) override
    {
        if (AttributesDepth_ == 0) {
            Output_->OnDoubleScalar(value);
        }
    }

    void OnBooleanScalar(bool value) override
    {
        if (AttributesDepth_ == 0) {
            Output_->OnBooleanScalar(value);
        }
    }

    void OnEntity() override
    {
        if (AttributesDepth_ == 0) {
            Output_->OnEntity();
        }
    }

    void OnBeginList() override
    {
        if (AttributesDepth_ == 0) {
            Output_->OnBeginList();
        }
    }

    void OnListItem() override
    {
        if (AttributesDepth_ == 0) {
            Output_->OnListItem();
        }
    }

    void OnEndList() override
    {
        if (AttributesDepth_ == 0) {
            Output_->OnEndList();
        }
    }

    void OnBeginMap() override
    {
        if (AttributesDepth_ == 0) {
            Output_->OnBeginMap();
        }
    }

    void OnKeyedItem(TStringBuf key) override
    {
        if (AttributesDepth_ == 0) {
            Output_->OnKeyedItem(key);
        }
    }

    void OnEndMap() override
    {
        if (AttributesDepth_ == 0) {
            Output_->OnEndMap();
        }
    }

    void OnBeginAttributes() override
    {
        ++AttributesDepth_;
    }

    void OnEndAttributes() override
    {
        --AttributesDepth_;
    }

    void OnRaw(TStringBuf yson, EYsonType type) override
    {
        if (AttributesDepth_ == 0) {
            Output_->OnRaw(yson, type);
        }
    }

private:
    IYsonConsumer* Output_;
    int AttributesDepth_ = 0;
};

TYsonString StripAttributes(const TYsonString& yson)
{
    TStringStream outputStream;
    TYsonWriter writer(&outputStream);
    TYsonAttributesStripper stripper(&writer);
    ParseYsonStringBuffer(yson.AsStringBuf(), yson.GetType(), &stripper);
    return TYsonString(outputStream.Str(), yson.GetType());
}

////////////////////////////////////////////////////////////////////////////////

size_t TJobReport::EstimateSize() const
{
    return EstimateSizes(
        OperationId_,
        JobId_,
        Type_,
        State_,
        StartTime_,
        FinishTime_,
        Error_,
        Spec_,
        SpecVersion_,
        Statistics_,
        Events_);
}

TJobReport TJobReport::ExtractSpec() const
{
    TJobReport copy;
    copy.JobId_ = JobId_;
    copy.Spec_ = Spec_;
    copy.SpecVersion_ = SpecVersion_;
    copy.Type_ = Type_;
    return copy;
}

TJobReport TJobReport::ExtractStderr() const
{
    TJobReport copy;
    copy.JobId_ = JobId_;
    copy.OperationId_ = OperationId_;
    copy.Stderr_ = Stderr_;
    return copy;
}

TJobReport TJobReport::ExtractFailContext() const
{
    TJobReport copy;
    copy.JobId_ = JobId_;
    copy.OperationId_ = OperationId_;
    copy.FailContext_ = FailContext_;
    return copy;
}

TJobReport TJobReport::ExtractIds() const
{
    TJobReport copy;
    copy.JobId_ = JobId_;
    copy.OperationId_ = OperationId_;
    return copy;
}

TJobReport TJobReport::ExtractProfile() const
{
    TJobReport copy;
    copy.JobId_ = JobId_;
    copy.OperationId_ = OperationId_;
    copy.Profile_ = Profile_;
    return copy;
}

bool TJobReport::IsEmpty() const
{
    bool somethingSpecified =
        Type_ || State_ || StartTime_ || FinishTime_ || Error_ || Spec_ || SpecVersion_ ||
        Statistics_ || Events_ || Stderr_ || StderrSize_ || FailContext_ || Profile_ || JobCookie_ ||
        CoreInfos_ || HasCompetitors_ || HasProbingCompetitors_ || MonitoringDescriptor_ || ExecAttributes_;
    return !somethingSpecified;
}

TControllerJobReport TControllerJobReport::OperationId(NJobTrackerClient::TOperationId operationId)
{
    OperationId_ = operationId;
    return std::move(*this);
}

TControllerJobReport TControllerJobReport::JobId(NJobTrackerClient::TJobId jobId)
{
    JobId_ = jobId;
    return std::move(*this);
}

TControllerJobReport TControllerJobReport::HasCompetitors(bool hasCompetitors, EJobCompetitionType competitionType)
{
    switch (competitionType) {
        case EJobCompetitionType::Speculative:
            HasCompetitors_ = hasCompetitors;
            break;
        case EJobCompetitionType::Probing:
            HasProbingCompetitors_ = hasCompetitors;
            break;
        case EJobCompetitionType::LayerProbing:
            break;
        default:
            YT_ABORT();
    }
    return std::move(*this);
}

TControllerJobReport TControllerJobReport::JobCookie(ui64 jobCookie)
{
    JobCookie_ = jobCookie;
    return std::move(*this);
}

TNodeJobReport TNodeJobReport::OperationId(NJobTrackerClient::TOperationId operationId)
{
    OperationId_ = operationId;
    return std::move(*this);
}

TNodeJobReport TNodeJobReport::JobId(NJobTrackerClient::TJobId jobId)
{
    JobId_ = jobId;
    return std::move(*this);
}

TNodeJobReport TNodeJobReport::Type(NJobTrackerClient::EJobType type)
{
    Type_ = type;
    return std::move(*this);
}

TNodeJobReport TNodeJobReport::State(NJobTrackerClient::EJobState state)
{
    State_ = FormatEnum(state);
    return std::move(*this);
}

TNodeJobReport TNodeJobReport::StartTime(TInstant startTime)
{
    StartTime_ = startTime.MicroSeconds();
    return std::move(*this);
}

TNodeJobReport TNodeJobReport::FinishTime(TInstant finishTime)
{
    FinishTime_ = finishTime.MicroSeconds();
    return std::move(*this);
}

TNodeJobReport TNodeJobReport::Error(const TError& error)
{
    if (!error.IsOK()) {
        Error_ = ConvertToYsonString(error).ToString();
    }
    return std::move(*this);
}

TNodeJobReport TNodeJobReport::Spec(const NControllerAgent::NProto::TJobSpec& spec)
{
    TString specString;
    YT_VERIFY(spec.SerializeToString(&specString));
    Spec_ = std::move(specString);
    return std::move(*this);
}

TNodeJobReport TNodeJobReport::SpecVersion(i64 specVersion)
{
    SpecVersion_ = specVersion;
    return std::move(*this);
}

TNodeJobReport TNodeJobReport::Statistics(const TYsonString& statistics)
{
    Statistics_ = StripAttributes(statistics).ToString();
    return std::move(*this);
}

TNodeJobReport TNodeJobReport::Events(const TJobEvents& events)
{
    Events_ = ConvertToYsonString(events).ToString();
    return std::move(*this);
}

TNodeJobReport TNodeJobReport::StderrSize(i64 stderrSize)
{
    YT_VERIFY(!Stderr_.has_value());
    StderrSize_ = stderrSize;
    return std::move(*this);
}

TNodeJobReport TNodeJobReport::Stderr(const TString& stderr)
{
    Stderr_ = stderr;
    StderrSize_ = Stderr_->size();
    return std::move(*this);
}

TNodeJobReport TNodeJobReport::FailContext(const TString& failContext)
{
    FailContext_ = failContext;
    return std::move(*this);
}

TNodeJobReport TNodeJobReport::Profile(const TJobProfile& profile)
{
    Profile_ = profile;
    return std::move(*this);
}

TNodeJobReport TNodeJobReport::CoreInfos(NScheduler::TCoreInfos coreInfos)
{
    CoreInfos_ = std::move(coreInfos);
    return std::move(*this);
}

TNodeJobReport TNodeJobReport::ExecAttributes(const TYsonString& execAttributes)
{
    ExecAttributes_ = StripAttributes(execAttributes).ToString();
    return std::move(*this);
}

TNodeJobReport TNodeJobReport::TreeId(TString treeId)
{
    TreeId_ = std::move(treeId);
    return std::move(*this);
}

TNodeJobReport TNodeJobReport::MonitoringDescriptor(TString monitoringDescriptor)
{
    MonitoringDescriptor_ = std::move(monitoringDescriptor);
    return std::move(*this);
}

void TNodeJobReport::SetStatistics(const TYsonString& statistics)
{
    Statistics_ = StripAttributes(statistics).ToString();
}

void TNodeJobReport::SetStartTime(TInstant startTime)
{
    StartTime_ = startTime.MicroSeconds();
}

void TNodeJobReport::SetFinishTime(TInstant finishTime)
{
    FinishTime_ = finishTime.MicroSeconds();
}

void TNodeJobReport::SetJobCompetitionId(NJobTrackerClient::TJobId jobCompetitionId)
{
    JobCompetitionId_ = jobCompetitionId;
}

void TNodeJobReport::SetProbingJobCompetitionId(NJobTrackerClient::TJobId probingJobCompetitionId)
{
    ProbingJobCompetitionId_ = probingJobCompetitionId;
}

void TNodeJobReport::SetTaskName(const TString& taskName)
{
    TaskName_ = taskName;
}

////////////////////////////////////////////////////////////////////////////////

void TGpuDevice::Register(TRegistrar registrar)
{
    registrar.Parameter("device_number", &TThis::DeviceNumber)
        .Default();
    registrar.Parameter("device_name", &TThis::DeviceName)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

void TExecAttributes::Register(TRegistrar registrar)
{
    registrar.Parameter("slot_index", &TThis::SlotIndex)
        .Default(-1);
    registrar.Parameter("ip_addresses", &TThis::IPAddresses)
        .Default();
    registrar.Parameter("sandbox_path", &TThis::SandboxPath)
        .Default();
    registrar.Parameter("medium_name", &TThis::MediumName)
        .Default();
    registrar.Parameter("gpu_devices", &TThis::GpuDevices)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobAgent
