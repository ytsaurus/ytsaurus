#include "partition_map_job_io.h"
#include "job.h"
#include "user_job_io_detail.h"

#include <yt/ytlib/object_client/helpers.h>

#include <yt/ytlib/node_tracker_client/node_directory.h>

#include <yt/ytlib/scheduler/config.h>

#include <yt/ytlib/table_client/name_table.h>
#include <yt/ytlib/table_client/partitioner.h>
#include <yt/ytlib/table_client/schemaless_chunk_reader.h>
#include <yt/ytlib/table_client/schemaless_chunk_writer.h>

namespace NYT {
namespace NJobProxy {

using namespace NTableClient;
using namespace NTransactionClient;
using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NObjectClient;
using namespace NScheduler;
using namespace NScheduler::NProto;

using NChunkClient::TDataSliceDescriptor;

////////////////////////////////////////////////////////////////////////////////

class TPartitionMapJobIO
    : public TUserJobIOBase
{
public:
    explicit TPartitionMapJobIO(IJobHostPtr host)
        : TUserJobIOBase(host)
    { }

    virtual void PopulateResult(TSchedulerJobResultExt* schedulerJobResult) override
    {
        // Don't call base class method, no need to fill boundary keys.

        YCHECK(Writers_.size() == 1);
        auto& writer = Writers_.front();
        ToProto(schedulerJobResult->mutable_output_chunk_specs(), writer->GetWrittenChunksMasterMeta());
    }
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IUserJobIO> CreatePartitionMapJobIO(IJobHostPtr host)
{
    return std::unique_ptr<IUserJobIO>(new TPartitionMapJobIO(host));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
