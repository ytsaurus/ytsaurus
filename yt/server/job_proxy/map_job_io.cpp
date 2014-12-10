#include "stdafx.h"
#include "map_job_io.h"
#include "config.h"
#include "user_job_io.h"
#include "job.h"

#include <ytlib/chunk_client/old_multi_chunk_parallel_reader.h>

#include <ytlib/scheduler/config.h>

namespace NYT {
namespace NJobProxy {

using namespace NScheduler;
using namespace NScheduler::NProto;
using namespace NJobTrackerClient::NProto;

////////////////////////////////////////////////////////////////////

class TMapJobIO
    : public TUserJobIO
{
public:
    TMapJobIO(
        TJobIOConfigPtr config,
        IJobHost* host)
        : TUserJobIO(config, host)
    { }

    virtual void PopulateResult(TJobResult* result) override
    {
        auto* resultExt = result->MutableExtension(TSchedulerJobResultExt::scheduler_job_result_ext);
        PopulateUserJobResult(resultExt->mutable_user_job_result());
    }

};

std::unique_ptr<TUserJobIO> CreateMapJobIO(
    TJobIOConfigPtr ioConfig,
    IJobHost* host)
{
    return std::unique_ptr<TUserJobIO>(new TMapJobIO(ioConfig, host));
}

////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
