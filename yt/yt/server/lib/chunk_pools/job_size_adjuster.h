#pragma once

#include "private.h"

#include <yt/yt/server/lib/controller_agent/structs.h>

namespace NYT::NChunkPools {

////////////////////////////////////////////////////////////////////////////////

// TODO(max42): make customizable?
constexpr double JobSizeBoostFactor = 2.0;

////////////////////////////////////////////////////////////////////////////////

struct IJobSizeAdjuster
    : public virtual TRefCounted
    , public virtual IPersistent
{
    virtual void UpdateStatistics(const NControllerAgent::TCompletedJobSummary& jobSummary) = 0;
    virtual void UpdateStatistics(i64 jobDataWeight, TDuration prepareDuration, TDuration execDuration) = 0;
    virtual i64 GetDataWeightPerJob() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IJobSizeAdjuster)

////////////////////////////////////////////////////////////////////////////////

IJobSizeAdjusterPtr CreateJobSizeAdjuster(
    i64 dataWeightPerJob,
    const TJobSizeAdjusterConfigPtr& config);

////////////////////////////////////////////////////////////////////////////////

struct IDiscreteJobSizeAdjuster
    : public virtual IPersistent
{
    virtual void UpdateStatistics(const NControllerAgent::TCompletedJobSummary& jobSummary) = 0;

    virtual i64 GetUnderlyingDataWeightPerJob() const = 0;

    //! Returns the underlying data weight per job if it is at least |dataWeightFactor| times
    //! the last effective data weight per job; otherwise returns nullopt.
    //! Updates the last effective data weight per job when returning a value.
    virtual std::optional<i64> TryGetNewDataWeightPerJob() = 0;

    virtual void Reset(i64 dataWeightPerJob) = 0;
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IDiscreteJobSizeAdjuster> CreateDiscreteJobSizeAdjuster(
    IJobSizeAdjusterPtr underlying,
    i64 dataWeightPerJob,
    double dataWeightFactor);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkPools
