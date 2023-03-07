#include "job_report.h"

#include <yt/server/lib/core_dump/helpers.h>

#include <yt/ytlib/job_tracker_client/proto/job.pb.h>

#include <yt/core/ytree/fluent.h>

namespace NYT::NJobAgent {

using namespace NYTree;
using namespace NYson;
using namespace NCoreDump;

////////////////////////////////////////////////////////////////////////////////

namespace {

constexpr size_t EstimatedValueSize = 16;

size_t EstimateSize(const TString& s)
{
    return EstimatedValueSize + s.size();
}

size_t EstimateSize(i64)
{
    return EstimatedValueSize;
}

size_t EstimateSize(TGuid id)
{
    return id.IsEmpty() ? 0 : EstimatedValueSize * 2;
}

template <typename T>
size_t EstimateSize(const std::optional<T>& v)
{
    return v ? EstimateSize(*v) : 0;
}

size_t EstimateSizes()
{
    return 0;
}

template <typename T, typename... U>
size_t EstimateSizes(T&& t, U&& ... u)
{
    return EstimateSize(std::forward<T>(t)) + EstimateSizes(std::forward<U>(u)...);
}

} // namespace

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
            fluent.Item()
                .BeginMap()
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

    virtual void OnStringScalar(TStringBuf value) override
    {
        if (AttributesDepth_ == 0) {
            Output_->OnStringScalar(value);
        }
    }

    virtual void OnInt64Scalar(i64 value) override
    {
        if (AttributesDepth_ == 0) {
            Output_->OnInt64Scalar(value);
        }
    }

    virtual void OnUint64Scalar(ui64 value) override
    {
        if (AttributesDepth_ == 0) {
            Output_->OnInt64Scalar(value);
        }
    }

    virtual void OnDoubleScalar(double value) override
    {
        if (AttributesDepth_ == 0) {
            Output_->OnDoubleScalar(value);
        }
    }

    virtual void OnBooleanScalar(bool value) override
    {
        if (AttributesDepth_ == 0) {
            Output_->OnBooleanScalar(value);
        }
    }

    virtual void OnEntity() override
    {
        if (AttributesDepth_ == 0) {
            Output_->OnEntity();
        }
    }

    virtual void OnBeginList() override
    {
        if (AttributesDepth_ == 0) {
            Output_->OnBeginList();
        }
    }

    virtual void OnListItem() override
    {
        if (AttributesDepth_ == 0) {
            Output_->OnListItem();
        }
    }

    virtual void OnEndList() override
    {
        if (AttributesDepth_ == 0) {
            Output_->OnEndList();
        }
    }

    virtual void OnBeginMap() override
    {
        if (AttributesDepth_ == 0) {
            Output_->OnBeginMap();
        }
    }

    virtual void OnKeyedItem(TStringBuf key) override
    {
        if (AttributesDepth_ == 0) {
            Output_->OnKeyedItem(key);
        }
    }

    virtual void OnEndMap() override
    {
        if (AttributesDepth_ == 0) {
            Output_->OnEndMap();
        }
    }

    virtual void OnBeginAttributes() override
    {
        ++AttributesDepth_;
    }

    virtual void OnEndAttributes() override
    {
        --AttributesDepth_;
    }

    virtual void OnRaw(TStringBuf yson, EYsonType type) override
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
    ParseYsonStringBuffer(yson.GetData(), yson.GetType(), &stripper);
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
    return !(Type_ || State_ || StartTime_ || FinishTime_ || Error_ || Spec_ || SpecVersion_ ||
             Statistics_ || Events_ || Stderr_ || StderrSize_ || FailContext_ || Profile_ ||
             CoreInfos_ || HasCompetitors_);
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

TControllerJobReport TControllerJobReport::HasCompetitors(bool hasCompetitors)
{
    HasCompetitors_ = hasCompetitors;
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
    Type_ = FormatEnum(type);
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
        Error_ = ConvertToYsonString(error).GetData();
    }
    return std::move(*this);
}

TNodeJobReport TNodeJobReport::Spec(const NJobTrackerClient::NProto::TJobSpec& spec)
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
    Statistics_ = StripAttributes(statistics).GetData();
    return std::move(*this);
}

TNodeJobReport TNodeJobReport::Events(const TJobEvents& events)
{
    Events_ = ConvertToYsonString(events).GetData();
    return std::move(*this);
}

TNodeJobReport TNodeJobReport::StderrSize(ui64 stderrSize)
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

TNodeJobReport TNodeJobReport::CoreInfos(TCoreInfos coreInfos)
{
    CoreInfos_ = std::move(coreInfos);
    return std::move(*this);
}

TNodeJobReport TNodeJobReport::ExecAttributes(const TYsonString& execAttributes)
{
    ExecAttributes_ = StripAttributes(execAttributes).GetData();
    return std::move(*this);
}

void TNodeJobReport::SetStatistics(const TYsonString& statistics)
{
    Statistics_ = StripAttributes(statistics).GetData();
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

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobAgent
