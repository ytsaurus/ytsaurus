
#include "fair_share_tree_element_common.h"
#include "fair_share_tree.h"

#include <yt/core/profiling/profile_manager.h>

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

NProfiling::TTagIdList GetFailReasonProfilingTags(NControllerAgent::EScheduleJobFailReason reason)
{
    static const NProfiling::TEnumMemberTagCache<NControllerAgent::EScheduleJobFailReason> ReasonTagCache("reason");
    return {ReasonTagCache.GetTag(reason)};
}

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

TScheduleJobsProfilingCounters::TScheduleJobsProfilingCounters(
    const TString& prefix,
    const NProfiling::TTagIdList& treeIdProfilingTags)
    : PrescheduleJobTime(prefix + "/preschedule_job_time", treeIdProfilingTags)
    , TotalControllerScheduleJobTime(prefix + "/controller_schedule_job_time/total", treeIdProfilingTags)
    , ExecControllerScheduleJobTime(prefix + "/controller_schedule_job_time/exec", treeIdProfilingTags)
    , StrategyScheduleJobTime(prefix + "/strategy_schedule_job_time", treeIdProfilingTags)
    , PackingRecordHeartbeatTime(prefix + "/packing_record_heartbeat_time", treeIdProfilingTags)
    , PackingCheckTime(prefix + "/packing_check_time", treeIdProfilingTags)
    , ScheduleJobAttemptCount(prefix + "/schedule_job_attempt_count", treeIdProfilingTags)
    , ScheduleJobFailureCount(prefix + "/schedule_job_failure_count", treeIdProfilingTags)
{
    for (auto reason : TEnumTraits<NControllerAgent::EScheduleJobFailReason>::GetDomainValues()) {
        auto tags = NDetail::GetFailReasonProfilingTags(reason);
        tags.insert(tags.end(), treeIdProfilingTags.begin(), treeIdProfilingTags.end());

        ControllerScheduleJobFail[reason] = NProfiling::TMonotonicCounter(
            prefix + "/controller_schedule_job_fail",
            tags);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
