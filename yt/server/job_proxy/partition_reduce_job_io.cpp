#include "stdafx.h"
#include "partition_reduce_job_io.h"
#include "config.h"
#include "user_job_io_detail.h"
#include "job.h"

#include <ytlib/new_table_client/name_table.h>
#include <ytlib/new_table_client/schemaless_partition_sort_reader.h>
#include <ytlib/new_table_client/schemaless_chunk_writer.h>

#include <ytlib/node_tracker_client/node_directory.h>

namespace NYT {
namespace NJobProxy {

using namespace NScheduler;
using namespace NScheduler::NProto;
using namespace NChunkClient::NProto;
using namespace NVersionedTableClient;
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

    virtual std::vector<ISchemalessMultiChunkReaderPtr> DoCreateReaders() override
    {
        YCHECK(SchedulerJobSpec_.input_specs_size() == 1);

        const auto& inputSpec = SchedulerJobSpec_.input_specs(0);
        std::vector<TChunkSpec> chunks(
            inputSpec.chunks().begin(),
            inputSpec.chunks().end());

        const auto& reduceJobSpecExt = Host_->GetJobSpec().GetExtension(TReduceJobSpecExt::reduce_job_spec_ext);
        auto keyColumns = FromProto<Stroka>(reduceJobSpecExt.key_columns());
        auto nameTable = TNameTable::FromKeyColumns(keyColumns);

        auto reader = CreateSchemalessPartitionSortReader(
            JobIOConfig_->NewTableReader,
            Host_->GetMasterChannel(),
            Host_->GetCompressedBlockCache(),
            Host_->GetUncompressedBlockCache(),
            Host_->GetNodeDirectory(),
            keyColumns,
            nameTable,
            BIND(&IJobHost::ReleaseNetwork, Host_),
            chunks,
            SchedulerJobSpec_.input_row_count(),
            SchedulerJobSpec_.is_approximate());

        return std::vector<ISchemalessMultiChunkReaderPtr>(1, reader);
    }

    virtual ISchemalessMultiChunkWriterPtr DoCreateWriter(
        TTableWriterOptionsPtr options,
        const TChunkListId& chunkListId,
        const TTransactionId& transactionId,
        const TKeyColumns& keyColumns) override
    {
        return CreateTableWriter(options, chunkListId, transactionId, keyColumns);
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
