#include "config.h"

namespace NYT::NChunkPools {

/////////////////////////////////////////////////////////////////////////////

void TJobSizeAdjusterConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("min_job_time", &TThis::MinJobTime)
        .Default(TDuration::Seconds(60));

    registrar.Parameter("max_job_time", &TThis::MaxJobTime)
        .Default(TDuration::Minutes(10));

    registrar.Parameter("exec_to_prepare_time_ratio", &TThis::ExecToPrepareTimeRatio)
        .Default(20.0);
}

DEFINE_REFCOUNTED_TYPE(TJobSizeAdjusterConfig)

/////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkPools
