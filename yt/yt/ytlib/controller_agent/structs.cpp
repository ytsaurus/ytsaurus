#include "structs.h"

#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NControllerAgent {

using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

void Serialize(const TIncarnationSwitchInfo& info, IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .BeginMap()
            .OptionalItem("trigger_job_id", info.TriggerJobId)
            .OptionalItem("interruption_reason", info.InterruptionReason)
            .OptionalItem("abort_reason", info.AbortReason)
            .OptionalItem("trigger_job_error", info.TriggerJobError)
            .OptionalItem("actual_job_count", info.ActualJobCount)
            .OptionalItem("expected_job_count", info.ExpectedJobCount)
            .OptionalItem("task_name", info.TaskName)
        .EndMap();
}

EOperationIncarnationSwitchReason TIncarnationSwitchData::GetSwitchReason()
{
    YT_VERIFY(IncarnationSwitchReason);
    return *IncarnationSwitchReason;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent
