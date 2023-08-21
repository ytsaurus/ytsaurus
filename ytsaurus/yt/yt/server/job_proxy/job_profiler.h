#pragma once

#include "public.h"

#include <yt/yt/server/lib/misc/job_report.h>

namespace NYT::NJobProxy {

////////////////////////////////////////////////////////////////////////////////

struct IJobProfiler
{
    virtual ~IJobProfiler() = default;

    //! Starts job profiling.
    virtual void Start() = 0;

    //! Stops job profiling.
    virtual void Stop() = 0;

    //! If called, job proxy peak memory usage will be profiled
    //! at the end of the job.
    virtual void ProfilePeakMemoryUsage() = 0;

    //! Returns spec of user job profiler if any and nullptr if none.
    virtual NScheduler::TJobProfilerSpecPtr GetUserJobProfilerSpec() const = 0;

    //! Returns stream for user job profile if user job profile is required.
    virtual IOutputStream* GetUserJobProfileOutput() const = 0;

    //! Returns list of collected profiles. Must be executed after |Stop| call.
    virtual std::vector<NJobAgent::TJobProfile> GetProfiles() const = 0;
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IJobProfiler> CreateJobProfiler(
    const NScheduler::NProto::TSchedulerJobSpecExt* schedulerJobSpecExt);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobProxy
