#include "job_statistics.h"

#include <yt/ytlib/job_tracker_client/job.pb.h>

#include <yt/core/ytree/fluent.h>

namespace NYT {
namespace NJobAgent {

using namespace NYTree;
using namespace NYson;

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

size_t EstimateSize(const TGuid& id)
{
    return id.IsEmpty() ? 0 : EstimatedValueSize * 2;
}

template <typename T>
size_t EstimateSize(const TNullable<T>& v)
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

void Serialize(const TJobEvents& events, NYson::IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .BeginList()
        .DoFor(events, [] (TFluentList fluent, const TJobEvent& event) {
            fluent.Item()
                .BeginMap()
                .Item("time").Value(event.Timestamp())
                .DoIf(event.State().HasValue(), [&] (TFluentMap fluent) {
                    fluent.Item("state").Value(FormatEnum(*event.State()));
                })
                .DoIf(event.Phase().HasValue(), [&] (TFluentMap fluent) {
                    fluent.Item("phase").Value(FormatEnum(*event.Phase()));
                })
                .EndMap();
        })
        .EndList();
}

////////////////////////////////////////////////////////////////////////////////

class TYsonAtributesStripper
    : public IYsonConsumer
{
public:
    TYsonAtributesStripper(IYsonConsumer* output)
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
    TYsonAtributesStripper stripper(&writer);
    ParseYsonStringBuffer(yson.GetData(), yson.GetType(), &stripper);
    return TYsonString(outputStream.Str(), yson.GetType());
}

////////////////////////////////////////////////////////////////////////////////

size_t TJobStatistics::EstimateSize() const
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

TJobStatistics TJobStatistics::ExtractSpec()
{
    TJobStatistics copy;
    copy.JobId_ = JobId_;
    copy.Spec_ = std::move(Spec_);
    copy.SpecVersion_ = std::move(SpecVersion_);
    copy.Type_ = Type_;
    return copy;
}

TJobStatistics TJobStatistics::ExtractStderr()
{
    TJobStatistics copy;
    copy.JobId_ = JobId_;
    copy.OperationId_ = OperationId_;
    copy.Stderr_ = Stderr_;
    return copy;
}

bool TJobStatistics::IsEmpty() const
{
    return !(Type_ || State_ || StartTime_ || FinishTime_ || Error_ || Spec_ || SpecVersion_ || Statistics_ || Events_ || Stderr_);
}

void TJobStatistics::SetOperationId(NJobTrackerClient::TOperationId operationId)
{
    OperationId_ = operationId;
}

void TJobStatistics::SetJobId(NJobTrackerClient::TJobId jobId)
{
    JobId_ = jobId;
}

void TJobStatistics::SetType(NJobTrackerClient::EJobType type)
{
    Type_ = FormatEnum(type);
}

void TJobStatistics::SetState(NJobTrackerClient::EJobState state)
{
    State_ = FormatEnum(state);
}

void TJobStatistics::SetStartTime(TInstant startTime)
{
    StartTime_ = startTime.MicroSeconds();
}

void TJobStatistics::SetFinishTime(TInstant finishTime)
{
    FinishTime_ = finishTime.MicroSeconds();
}

void TJobStatistics::SetError(const TError& error)
{
    if (!error.IsOK()) {
        Error_ = ConvertToYsonString(error).GetData();
    }
}

void TJobStatistics::SetSpec(const NJobTrackerClient::NProto::TJobSpec& spec)
{
    TString specString;
    bool result = spec.SerializeToString(&specString);
    YCHECK(result);
    Spec_ = std::move(specString);
}

void TJobStatistics::SetSpecVersion(i64 specVersion)
{
    SpecVersion_ = specVersion;
}

void TJobStatistics::SetStatistics(const TYsonString& statistics)
{
    Statistics_ = StripAttributes(statistics).GetData();
}

void TJobStatistics::SetEvents(const TJobEvents& events)
{
    Events_ = ConvertToYsonString(events).GetData();
}

void TJobStatistics::SetStderr(const TString& stderr)
{
    Stderr_ = stderr;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobAgent
} // namespace NYT
