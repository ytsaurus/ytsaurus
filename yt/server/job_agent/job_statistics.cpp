#include "job_statistics.h"

#include <yt/ytlib/job_tracker_client/job.pb.h>

#include <yt/core/ytree/fluent.h>

namespace NYT {
namespace NJobAgent {

using namespace NYTree;

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

TJobStatistics::TJobStatistics()
    : Priority_(EReportPriority::Normal)
{ }

void TJobStatistics::SetPriority(EReportPriority priority)
{
    Priority_ = priority;
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
        Error_ = ConvertToYsonString(error).Data();
    }
}

void TJobStatistics::SetSpec(const NJobTrackerClient::NProto::TJobSpec& spec)
{
    Stroka specString;
    bool result = spec.SerializeToString(&specString);
    YCHECK(result);
    Spec_ = std::move(specString);
}

void TJobStatistics::SetSpecVersion(i64 specVersion)
{
    SpecVersion_ = specVersion;
}

void TJobStatistics::SetStatistics(const NYson::TYsonString& statistics)
{
    Statistics_ = ConvertToYsonString(statistics).Data();
}

void TJobStatistics::SetEvents(const TJobEvents& events)
{
    Events_ = ConvertToYsonString(events).Data();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobAgent
} // namespace NYT
