#pragma once

#include <yt/yt/client/controller_agent/public.h>

#include <yt/yt/client/job_tracker_client/public.h>

#include <library/cpp/yt/yson/consumer.h>

namespace NYT::NControllerAgent {

////////////////////////////////////////////////////////////////////////////////

struct TIncarnationSwitchInfo
{
    // For JobAborted, JobFailed, JobInterrupted reasons.
    std::optional<NJobTrackerClient::TJobId> TriggerJobId;

    // For JobLackAfterRevival reason.
    std::optional<i64> ExpectedJobCount;
    std::optional<i64> ActualJobCount;
    std::optional<std::string> TaskName;
};

void Serialize(const TIncarnationSwitchInfo& info, NYson::IYsonConsumer* consumer);

struct TIncarnationSwitchData
{
    // Nullopt means incarnation switch reason is operation started.
    std::optional<EOperationIncarnationSwitchReason> IncarnationSwitchReason;

    TIncarnationSwitchInfo IncarnationSwitchInfo;

    // NB(bystrovserg): Should be called only if EOperationIncarnationSwitchReason
    // is presented in IncarnationSwitchReason.
    EOperationIncarnationSwitchReason GetSwitchReason();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent
