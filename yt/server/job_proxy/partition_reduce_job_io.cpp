#include "stdafx.h"
#include "partition_reduce_job_io.h"
#include "config.h"
#include "sorting_reader.h"
#include "user_job_io_detail.h"
#include "job.h"

#include <core/misc/protobuf_helpers.h>

#include <ytlib/chunk_client/block_cache.h>
#include <ytlib/chunk_client/client_block_cache.h>
#include <ytlib/chunk_client/multi_chunk_sequential_writer.h>

#include <ytlib/table_client/sync_reader.h>
#include <ytlib/table_client/sync_writer.h>

#include <ytlib/node_tracker_client/node_directory.h>

namespace NYT {
namespace NJobProxy {

using namespace NScheduler;
using namespace NScheduler::NProto;
using namespace NChunkClient::NProto;
using namespace NTableClient;
using namespace NChunkClient;
using namespace NTransactionClient;

////////////////////////////////////////////////////////////////////

class TPartitionReduceJobIO
    : public TUserJobIOBase
{
public:
    TPartitionReduceJobIO(IJobHost* host)
        : TUserJobIOBase(host)
    { }

    virtual std::vector<ISyncReaderPtr> DoCreateReaders() override
    {
        YCHECK(SchedulerJobSpec_.input_specs_size() == 1);

        const auto& inputSpec = SchedulerJobSpec_.input_specs(0);
        std::vector<TChunkSpec> chunks(
            inputSpec.chunks().begin(),
            inputSpec.chunks().end());

        const auto& reduceJobSpecExt = Host_->GetJobSpec().GetExtension(TReduceJobSpecExt::reduce_job_spec_ext);
        auto keyColumns = FromProto<Stroka>(reduceJobSpecExt.key_columns());

        auto reader = CreateSortingReader(
            JobIOConfig_->TableReader,
            Host_->GetMasterChannel(),
            Host_->GetCompressedBlockCache(),
            Host_->GetUncompressedBlockCache(),
            Host_->GetNodeDirectory(),
            keyColumns,
            BIND(&IJobHost::ReleaseNetwork, Host_),
            std::move(chunks),
            SchedulerJobSpec_.input_row_count(),
            SchedulerJobSpec_.is_approximate());

        return std::vector<ISyncReaderPtr>(1, reader);
    }

    virtual ISyncWriterUnsafePtr DoCreateWriter(
        TTableWriterOptionsPtr options,
        const TChunkListId& chunkListId,
        const TTransactionId& transactionId) override
    {
        return CreateTableWriter(options, chunkListId, transactionId);
    }

    virtual void PopulateResult(TSchedulerJobResultExt* schedulerJobResult) override
    {
        TUserJobIOBase::PopulateResult(schedulerJobResult);

        auto& writer = Writers_.front();
        writer->GetNodeDirectory()->DumpTo(schedulerJobResult->mutable_node_directory());
        ToProto(schedulerJobResult->mutable_chunks(), writer->GetWrittenChunks());
    }

};

std::unique_ptr<IUserJobIO> CreatePartitionReduceJobIO(IJobHost* host)
{
    return std::unique_ptr<IUserJobIO>(new TPartitionReduceJobIO(host));
}

////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
