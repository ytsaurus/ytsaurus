#include "partition_reduce_job_io.h"
#include "user_job_io_detail.h"

#include <yt/ytlib/table_client/schemaless_chunk_writer.h>

namespace NYT {
namespace NJobProxy {

using namespace NScheduler::NProto;

////////////////////////////////////////////////////////////////////////////////

class TPartitionReduceJobIO
    : public TUserJobIOBase
{
public:
    explicit TPartitionReduceJobIO(IJobHostPtr host)
        : TUserJobIOBase(host)
    { }

    virtual void PopulateResult(TSchedulerJobResultExt* schedulerJobResult) override
    {
        TUserJobIOBase::PopulateResult(schedulerJobResult);

        auto& writer = Writers_.front();

        // Partition reduce may come as intermediate job (reduce-combiner),
        // so we return written chunks to scheduler.
        ToProto(schedulerJobResult->mutable_output_chunk_specs(), writer->GetWrittenChunksMasterMeta());
    }
};

std::unique_ptr<IUserJobIO> CreatePartitionReduceJobIO(IJobHostPtr host)
{
    return std::unique_ptr<IUserJobIO>(new TPartitionReduceJobIO(host));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
