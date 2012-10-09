#include "stdafx.h"
#include "map_job_io.h"
#include "config.h"

#include <ytlib/table_client/multi_chunk_parallel_reader.h>
#include <ytlib/scheduler/config.h>

namespace NYT {
namespace NJobProxy {

using namespace NTableClient;
using namespace NYTree;
using namespace NScheduler;

////////////////////////////////////////////////////////////////////

class TMapJobIO
    : public TUserJobIO
{
public:
    TMapJobIO(
        TJobIOConfigPtr config,
        NMetaState::TMasterDiscoveryConfigPtr mastersConfig,
        const NScheduler::NProto::TJobSpec& jobSpec)
        : TUserJobIO(config, mastersConfig, jobSpec)
    { }

    virtual void PopulateResult(NScheduler::NProto::TJobResult* result) override
    {
        auto* resultExt = result->MutableExtension(NScheduler::NProto::TMapJobResultExt::map_job_result_ext);
        PopulateUserJobResult(resultExt->mutable_mapper_result());
    }

};

TAutoPtr<TUserJobIO> CreateMapJobIO(
    TJobIOConfigPtr ioConfig,
    NMetaState::TMasterDiscoveryConfigPtr mastersConfig,
    const NScheduler::NProto::TJobSpec& jobSpec)
{
    return new TMapJobIO(ioConfig, mastersConfig, jobSpec);
}

////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
