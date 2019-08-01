#include "config.h"

namespace NYT::NChunkPools {

/////////////////////////////////////////////////////////////////////////////

TJobSizeAdjusterConfig::TJobSizeAdjusterConfig()
{
    RegisterParameter("min_job_time", MinJobTime)
        .Default(TDuration::Seconds(60));

    RegisterParameter("max_job_time", MaxJobTime)
        .Default(TDuration::Minutes(10));

    RegisterParameter("exec_to_prepare_time_ratio", ExecToPrepareTimeRatio)
        .Default(20.0);
}

DEFINE_REFCOUNTED_TYPE(TJobSizeAdjusterConfig)

/////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkPools
