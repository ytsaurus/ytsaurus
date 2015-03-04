#include "stdafx.h"

#include "partition_job.h"

#include "job_detail.h"
#include "config.h"
#include "private.h"

#include <ytlib/chunk_client/chunk_spec.h>

#include <ytlib/new_table_client/name_table.h>
#include <ytlib/new_table_client/partitioner.h>
#include <ytlib/new_table_client/schemaless_chunk_reader.h>
#include <ytlib/new_table_client/schemaless_chunk_writer.h>

#include <ytlib/transaction_client/public.h>

#include <core/ytree/yson_string.h>


namespace NYT {
namespace NJobProxy {

using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NScheduler::NProto;
using namespace NTransactionClient;
using namespace NVersionedTableClient;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

class TPartitionJob
    : public TSimpleJobBase
{
public:
    explicit TPartitionJob(IJobHost* host)
        : TSimpleJobBase(host)
        , PartitionJobSpecExt_(host->GetJobSpec().GetExtension(TPartitionJobSpecExt::partition_job_spec_ext))
    {
        auto config = host->GetConfig();

        YCHECK(SchedulerJobSpecExt_.input_specs_size() == 1);
        const auto& inputSpec = SchedulerJobSpecExt_.input_specs(0);

        std::vector<TChunkSpec> chunkSpecs(inputSpec.chunks().begin(), inputSpec.chunks().end());

        TotalRowCount_ = GetCumulativeRowCount(chunkSpecs);

        auto keyColumns = FromProto<TKeyColumns>(PartitionJobSpecExt_.sort_key_columns());
        auto nameTable = TNameTable::FromKeyColumns(keyColumns);

        Reader_ = CreateSchemalessParallelMultiChunkReader(
            config->JobIO->NewTableReader,
            New<TMultiChunkReaderOptions>(),
            host->GetMasterChannel(),
            host->GetCompressedBlockCache(),
            host->GetUncompressedBlockCache(),
            host->GetNodeDirectory(),
            std::move(chunkSpecs),
            nameTable,
            TKeyColumns());

        std::unique_ptr<IPartitioner> partitioner;
        if (PartitionJobSpecExt_.partition_keys_size() > 0) {
            YCHECK(PartitionJobSpecExt_.partition_keys_size() + 1 == PartitionJobSpecExt_.partition_count());
            for (const auto& protoKey : PartitionJobSpecExt_.partition_keys()) {
                TOwningKey key;
                FromProto(&key, protoKey);
                PartitionKeys_.push_back(key);
            }
            partitioner = CreateOrderedPartitioner(&PartitionKeys_);
        } else {
            partitioner = CreateHashPartitioner(
                PartitionJobSpecExt_.partition_count(), 
                PartitionJobSpecExt_.reduce_key_column_count());
        }

        YCHECK(SchedulerJobSpecExt_.output_specs_size() == 1);
        const auto& outputSpec = SchedulerJobSpecExt_.output_specs(0);

        auto transactionId = FromProto<TTransactionId>(SchedulerJobSpecExt_.output_transaction_id());
        auto chunkListId = FromProto<TChunkListId>(outputSpec.chunk_list_id());

        auto options = ConvertTo<TTableWriterOptionsPtr>(TYsonString(outputSpec.table_writer_options()));

        Writer_ = CreatePartitionMultiChunkWriter(
            config->JobIO->NewTableWriter,
            options,
            nameTable,
            keyColumns,
            host->GetMasterChannel(),
            transactionId,
            chunkListId,
            std::move(partitioner));
    }

private:
    const TPartitionJobSpecExt& PartitionJobSpecExt_;
    std::vector<TOwningKey> PartitionKeys_;

};

IJobPtr CreatePartitionJob(IJobHost* host)
{
    return New<TPartitionJob>(host);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
