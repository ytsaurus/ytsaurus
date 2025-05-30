#pragma once

#include "private.h"

#include <yt/yt/server/lib/controller_agent/structs.h>

namespace NYT::NChunkPools {

////////////////////////////////////////////////////////////////////////////////

// TODO(max42): make customizable?
constexpr double JobSizeBoostFactor = 2.0;

////////////////////////////////////////////////////////////////////////////////

struct IJobSizeAdjuster
    : public virtual IPersistent
{
    virtual void UpdateStatistics(const NControllerAgent::TCompletedJobSummary& jobSummary) = 0;
    virtual void UpdateStatistics(i64 jobDataWeight, TDuration prepareDuration, TDuration execDuration) = 0;
    virtual i64 GetDataWeightPerJob() const = 0;
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IJobSizeAdjuster> CreateJobSizeAdjuster(
    i64 dataWeightPerJob,
    const TJobSizeAdjusterConfigPtr& config);

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EJobAdjustmentAction,
    (None)
    (RebuildJobs)
);

//! Suggests to increase data weight if last data weight was at least
//! |dataWeightFactor| times smaller than the current ideal data weight.
//! NB: "Discrete" is opposed to the regular adjuster,
//! which does not suggest when to rebuild jobs.
struct IDiscreteJobSizeAdjuster
    : public virtual IPersistent
{
    virtual EJobAdjustmentAction UpdateStatistics(const NControllerAgent::TCompletedJobSummary& jobSummary) = 0;
    virtual i64 GetDataWeightPerJob() const = 0;
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IDiscreteJobSizeAdjuster> CreateDiscreteJobSizeAdjuster(
    i64 dataWeightPerJob,
    const TJobSizeAdjusterConfigPtr& config);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent
