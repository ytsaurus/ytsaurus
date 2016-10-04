#pragma once

#include "private.h"
#include "serialize.h"

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

struct IJobSizeManager
    : public virtual IPersistent
{
    virtual void OnJobCompleted(const TCompletedJobSummary& jobSummary) = 0;
    virtual void OnJobCompleted(i64 jobDataSize, TDuration prepareDuration, TDuration execDuration) = 0;
    virtual i64 GetIdealDataSizePerJob() const = 0;
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IJobSizeManager> CreateJobSizeManager(
    i64 dataSizePerJob,
    i64 maxDataSizePerJob,
    TJobSizeManagerConfigPtr config);

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

