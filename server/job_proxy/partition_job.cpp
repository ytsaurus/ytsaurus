#include "partition_job.h"
#include "private.h"
#include "config.h"
#include "job_detail.h"

#include <yt/ytlib/object_client/helpers.h>

#include <yt/ytlib/chunk_client/chunk_spec.h>

#include <yt/ytlib/table_client/name_table.h>
#include <yt/ytlib/table_client/partitioner.h>
#include <yt/ytlib/table_client/schemaless_chunk_reader.h>
#include <yt/ytlib/table_client/schemaless_chunk_writer.h>

namespace NYT {
namespace NJobProxy {

using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NScheduler::NProto;
using namespace NTransactionClient;
using namespace NTableClient;
using namespace NObjectClient;
using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

class TPartitionJob
    : public TSimpleJobBase
{
public:
    explicit TPartitionJob(IJobHostPtr host)
        : TSimpleJobBase(host)
        , PartitionJobSpecExt_(host->GetJobSpec().GetExtension(TPartitionJobSpecExt::partition_job_spec_ext))
    { }

    virtual void Initialize() override
    {
        auto config = Host_->GetConfig();

        YCHECK(SchedulerJobSpecExt_.input_specs_size() == 1);
        const auto& inputSpec = SchedulerJobSpecExt_.input_specs(0);

        std::vector<TChunkSpec> chunkSpecs(inputSpec.chunks().begin(), inputSpec.chunks().end());

        TotalRowCount_ = GetCumulativeRowCount(chunkSpecs);

        auto keyColumns = FromProto<TKeyColumns>(PartitionJobSpecExt_.sort_key_columns());

        NameTable_ = TNameTable::FromKeyColumns(keyColumns);

        ReaderFactory_ = [=] (TNameTablePtr nameTable, const TColumnFilter& columnFilter) {
            YCHECK(!Reader_);
            // NB: don't create parallel reader to eliminate non-deterministic behavior,
            // which is a nightmare for restarted (lost) jobs.
            Reader_ = CreateSchemalessSequentialMultiChunkReader(
                config->JobIO->TableReader,
                New<NTableClient::TTableReaderOptions>(),
                Host_->GetClient(),
                Host_->LocalDescriptor(),
                Host_->GetBlockCache(),
                Host_->GetInputNodeDirectory(),
                std::move(chunkSpecs),
                nameTable,
                columnFilter,
                TKeyColumns());
            return Reader_;
        };

        YCHECK(SchedulerJobSpecExt_.output_specs_size() == 1);
        const auto& outputSpec = SchedulerJobSpecExt_.output_specs(0);

        auto transactionId = FromProto<TTransactionId>(SchedulerJobSpecExt_.output_transaction_id());
        auto chunkListId = FromProto<TChunkListId>(outputSpec.chunk_list_id());

        auto options = ConvertTo<TTableWriterOptionsPtr>(TYsonString(outputSpec.table_writer_options()));

        WriterFactory_ = [=] (TNameTablePtr nameTable) mutable {
            YCHECK(!Writer_);
            Writer_ = CreatePartitionMultiChunkWriter(
                config->JobIO->TableWriter,
                options,
                nameTable,
                keyColumns,
                Host_->GetClient(),
                CellTagFromId(chunkListId),
                transactionId,
                chunkListId,
                GetPartitioner());
            return Writer_;
        };
    }

private:
    const TPartitionJobSpecExt& PartitionJobSpecExt_;

    std::vector<TOwningKey> PartitionKeys_;
    TNameTablePtr NameTable_;


    virtual void CreateReader() override
    {
        ReaderFactory_(NameTable_, TColumnFilter());
    }

    virtual void CreateWriter() override
    {
        WriterFactory_(NameTable_);
    }

    std::unique_ptr<IPartitioner> GetPartitioner()
    {
        if (PartitionJobSpecExt_.partition_keys_size() > 0) {
            YCHECK(PartitionJobSpecExt_.partition_keys_size() + 1 == PartitionJobSpecExt_.partition_count());
            PartitionKeys_ = FromProto<std::vector<TOwningKey>>(PartitionJobSpecExt_.partition_keys());
            return CreateOrderedPartitioner(&PartitionKeys_);
        } else {
            return CreateHashPartitioner(
                PartitionJobSpecExt_.partition_count(),
                PartitionJobSpecExt_.reduce_key_column_count());
        }
    }
};

IJobPtr CreatePartitionJob(IJobHostPtr host)
{
    return New<TPartitionJob>(host);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
