#include "partition_job.h"
#include "private.h"
#include "job_detail.h"

#include <yt/ytlib/chunk_client/chunk_spec.h>
#include <yt/ytlib/chunk_client/data_source.h>
#include <yt/ytlib/chunk_client/job_spec_extensions.h>

#include <yt/ytlib/job_proxy/helpers.h>

#include <yt/ytlib/object_client/helpers.h>

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

using NChunkClient::TDataSliceDescriptor;

////////////////////////////////////////////////////////////////////////////////

class TPartitionJob
    : public TSimpleJobBase
{
public:
    explicit TPartitionJob(IJobHostPtr host)
        : TSimpleJobBase(host)
        , PartitionJobSpecExt_(host->GetJobSpecHelper()->GetJobSpec().GetExtension(TPartitionJobSpecExt::partition_job_spec_ext))
    { }

    virtual void Initialize() override
    {
        YCHECK(SchedulerJobSpecExt_.input_table_specs_size() == 1);
        const auto& inputSpec = SchedulerJobSpecExt_.input_table_specs(0);

        auto dataSliceDescriptors = UnpackDataSliceDescriptors(inputSpec);

        auto dataSourceDirectoryExt = GetProtoExtension<TDataSourceDirectoryExt>(SchedulerJobSpecExt_.extensions());
        auto dataSourceDirectory = FromProto<TDataSourceDirectoryPtr>(dataSourceDirectoryExt);
        auto readerOptions = ConvertTo<TTableReaderOptionsPtr>(TYsonString(
            SchedulerJobSpecExt_.table_reader_options()));

        TotalRowCount_ = GetCumulativeRowCount(dataSliceDescriptors);

        auto keyColumns = FromProto<TKeyColumns>(PartitionJobSpecExt_.sort_key_columns());

        NameTable_ = TNameTable::FromKeyColumns(keyColumns);

        ReaderFactory_ = [=] (TNameTablePtr nameTable, const TColumnFilter& columnFilter) {
            YCHECK(!Reader_);
            // NB: don't create parallel reader to eliminate non-deterministic behavior,
            // which is a nightmare for restarted (lost) jobs.
            Reader_ = CreateSchemalessSequentialMultiReader(
                Host_->GetJobSpecHelper()->GetJobIOConfig()->TableReader,
                readerOptions,
                Host_->GetClient(),
                Host_->LocalDescriptor(),
                Host_->GetBlockCache(),
                Host_->GetInputNodeDirectory(),
                dataSourceDirectory,
                std::move(dataSliceDescriptors),
                nameTable,
                TReadSessionId(),
                columnFilter,
                TKeyColumns());
            return Reader_;
        };

        YCHECK(SchedulerJobSpecExt_.output_table_specs_size() == 1);
        const auto& outputSpec = SchedulerJobSpecExt_.output_table_specs(0);

        auto transactionId = FromProto<TTransactionId>(SchedulerJobSpecExt_.output_transaction_id());
        auto chunkListId = FromProto<TChunkListId>(outputSpec.chunk_list_id());

        auto options = ConvertTo<TTableWriterOptionsPtr>(TYsonString(outputSpec.table_writer_options()));
        // We pass key column for partitioning through schema, but input stream is not sorted.
        options->ValidateSorted = false;
        auto writerConfig = GetWriterConfig(outputSpec);

        WriterFactory_ = [=] (TNameTablePtr nameTable) mutable {
            YCHECK(!Writer_);
            Writer_ = CreatePartitionMultiChunkWriter(
                writerConfig,
                options,
                nameTable,
                TTableSchema::FromKeyColumns(keyColumns),
                Host_->GetClient(),
                CellTagFromId(chunkListId),
                transactionId,
                chunkListId,
                CreatePartitioner());
            return Writer_;
        };
    }

private:
    const TPartitionJobSpecExt& PartitionJobSpecExt_;

    TNameTablePtr NameTable_;


    virtual void CreateReader() override
    {
        ReaderFactory_(NameTable_, TColumnFilter());
    }

    virtual void CreateWriter() override
    {
        WriterFactory_(NameTable_);
    }

    virtual bool ShouldSendBoundaryKeys() const override
    {
        return false;
    }

    IPartitionerPtr CreatePartitioner()
    {
        if (PartitionJobSpecExt_.has_wire_partition_keys()) {
            auto wirePartitionKeys = TSharedRef::FromString(PartitionJobSpecExt_.wire_partition_keys());
            return CreateOrderedPartitioner(wirePartitionKeys);
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
