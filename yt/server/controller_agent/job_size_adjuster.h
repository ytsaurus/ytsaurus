#pragma once

#include "private.h"
#include "serialize.h"

#include <yt/server/scheduler/job.h>

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

struct IJobSizeAdjuster
    : public virtual IPersistent
{
    virtual void UpdateStatistics(const TCompletedJobSummary& jobSummary) = 0;
    virtual void UpdateStatistics(i64 jobDataSize, TDuration prepareDuration, TDuration execDuration) = 0;
    virtual i64 GetDataSizePerJob() const = 0;
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IJobSizeAdjuster> CreateJobSizeAdjuster(
    i64 dataSizePerJob,
    const TJobSizeAdjusterConfigPtr& config);

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

