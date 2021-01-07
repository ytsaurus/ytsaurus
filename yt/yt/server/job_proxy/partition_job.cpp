#include "partition_job.h"
#include "private.h"
#include "job_detail.h"

#include <yt/ytlib/chunk_client/chunk_spec.h>
#include <yt/ytlib/chunk_client/data_source.h>
#include <yt/ytlib/chunk_client/job_spec_extensions.h>
#include <yt/ytlib/chunk_client/parallel_reader_memory_manager.h>

#include <yt/ytlib/job_proxy/helpers.h>

#include <yt/client/object_client/helpers.h>

#include <yt/client/table_client/name_table.h>
#include <yt/ytlib/table_client/partitioner.h>
#include <yt/ytlib/table_client/schemaless_multi_chunk_reader.h>
#include <yt/ytlib/table_client/schemaless_chunk_writer.h>

namespace NYT::NJobProxy {

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
    explicit TPartitionJob(IJobHost* host)
        : TSimpleJobBase(host)
        , PartitionJobSpecExt_(host->GetJobSpecHelper()->GetJobSpec().GetExtension(TPartitionJobSpecExt::partition_job_spec_ext))
    { }

    virtual void Initialize() override
    {
        TSimpleJobBase::Initialize();

        YT_VERIFY(SchedulerJobSpecExt_.input_table_specs_size() == 1);
        const auto& inputSpec = SchedulerJobSpecExt_.input_table_specs(0);

        auto dataSliceDescriptors = UnpackDataSliceDescriptors(inputSpec);

        auto dataSourceDirectoryExt = GetProtoExtension<TDataSourceDirectoryExt>(SchedulerJobSpecExt_.extensions());
        auto dataSourceDirectory = FromProto<TDataSourceDirectoryPtr>(dataSourceDirectoryExt);
        auto readerOptions = ConvertTo<TTableReaderOptionsPtr>(TYsonString(
            SchedulerJobSpecExt_.table_reader_options()));

        TotalRowCount_ = GetCumulativeRowCount(dataSliceDescriptors);

        auto keyColumns = FromProto<TKeyColumns>(PartitionJobSpecExt_.sort_key_columns());
        auto sortColumns = FromProto<TSortColumns>(PartitionJobSpecExt_.sort_columns());
        // COMPAT(gritukan)
        if (sortColumns.empty()) {
            for (const auto& keyColumn : keyColumns) {
                sortColumns.push_back(TColumnSortSchema{
                    .Name = keyColumn,
                    .SortOrder = ESortOrder::Ascending
                });
            }
        }

        NameTable_ = TNameTable::FromKeyColumns(keyColumns);

        std::optional<int> partitionTag;
        if (SchedulerJobSpecExt_.has_partition_tag()) {
            partitionTag = SchedulerJobSpecExt_.partition_tag();
        }

        ReaderFactory_ = [=] (TNameTablePtr nameTable, const TColumnFilter& columnFilter) {
            YT_VERIFY(!Reader_);
            // NB: don't create parallel reader to eliminate non-deterministic behavior,
            // which is a nightmare for restarted (lost) jobs.
            const auto& tableReaderConfig = Host_->GetJobSpecHelper()->GetJobIOConfig()->TableReader;

            Reader_ = CreateSchemalessSequentialMultiReader(
                tableReaderConfig,
                readerOptions,
                Host_->GetClient(),
                Host_->LocalDescriptor(),
                std::nullopt,
                Host_->GetBlockCache(),
                Host_->GetInputNodeDirectory(),
                dataSourceDirectory,
                std::move(dataSliceDescriptors),
                nameTable,
                BlockReadOptions_,
                columnFilter,
                /* sortColumns */ {},
                partitionTag,
                Host_->GetTrafficMeter(),
                Host_->GetInBandwidthThrottler(),
                Host_->GetOutRpsThrottler(),
                MultiReaderMemoryManager_->CreateMultiReaderMemoryManager(tableReaderConfig->MaxBufferSize));
            return Reader_;
        };

        YT_VERIFY(SchedulerJobSpecExt_.output_table_specs_size() == 1);
        const auto& outputSpec = SchedulerJobSpecExt_.output_table_specs(0);
        auto outputSchema = TTableSchema::FromSortColumns(sortColumns);
        if (outputSpec.has_table_schema()) {
            DeserializeFromWireProto(&outputSchema, outputSpec.table_schema());
            outputSchema = outputSchema->ToSorted(sortColumns);
        }

        auto transactionId = FromProto<TTransactionId>(SchedulerJobSpecExt_.output_transaction_id());
        auto chunkListId = FromProto<TChunkListId>(outputSpec.chunk_list_id());

        auto options = ConvertTo<TTableWriterOptionsPtr>(TYsonString(outputSpec.table_writer_options()));
        // We pass key column for partitioning through schema, but input stream is not sorted.
        options->ValidateSorted = false;
        auto writerConfig = GetWriterConfig(outputSpec);

        WriterFactory_ = [=] (TNameTablePtr nameTable, TTableSchemaPtr /*schema*/) mutable {
            YT_VERIFY(!Writer_);
            Writer_ = CreatePartitionMultiChunkWriter(
                writerConfig,
                options,
                nameTable,
                outputSchema,
                Host_->GetClient(),
                CellTagFromId(chunkListId),
                transactionId,
                chunkListId,
                CreatePartitioner(PartitionJobSpecExt_),
                Host_->GetTrafficMeter(),
                Host_->GetOutBandwidthThrottler());
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
        WriterFactory_(NameTable_, nullptr);
    }

    virtual bool ShouldSendBoundaryKeys() const override
    {
        return false;
    }

    virtual i64 GetTotalReaderMemoryLimit() const
    {
        return Host_->GetJobSpecHelper()->GetJobIOConfig()->TableReader->MaxBufferSize;
    }
};

IJobPtr CreatePartitionJob(IJobHost* host)
{
    return New<TPartitionJob>(host);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobProxy
