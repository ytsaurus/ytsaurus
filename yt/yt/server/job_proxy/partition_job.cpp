#include "partition_job.h"
#include "private.h"
#include "job_detail.h"

#include <yt/yt/ytlib/chunk_client/chunk_spec.h>
#include <yt/yt/ytlib/chunk_client/data_source.h>
#include <yt/yt/ytlib/chunk_client/job_spec_extensions.h>
#include <yt/yt/ytlib/chunk_client/parallel_reader_memory_manager.h>

#include <yt/yt/ytlib/controller_agent/proto/job.pb.h>

#include <yt/yt/ytlib/job_proxy/helpers.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/ytlib/table_client/config.h>
#include <yt/yt/ytlib/table_client/partitioner.h>
#include <yt/yt/ytlib/table_client/schemaless_multi_chunk_reader.h>
#include <yt/yt/ytlib/table_client/schemaless_chunk_writer.h>

namespace NYT::NJobProxy {

using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NControllerAgent::NProto;
using namespace NTransactionClient;
using namespace NTableClient;
using namespace NObjectClient;
using namespace NYTree;
using namespace NYson;

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

class TPartitionJob
    : public TSimpleJobBase
{
public:
    explicit TPartitionJob(IJobHostPtr host)
        : TSimpleJobBase(host)
        , PartitionJobSpecExt_(host->GetJobSpecHelper()->GetJobSpec().GetExtension(TPartitionJobSpecExt::partition_job_spec_ext))
    { }

    void Initialize() override
    {
        TSimpleJobBase::Initialize();

        YT_VERIFY(JobSpecExt_.input_table_specs_size() == 1);

        auto dataSliceDescriptors = Host_->GetJobSpecHelper()->UnpackDataSliceDescriptors();
        auto dataSourceDirectory = Host_->GetJobSpecHelper()->GetDataSourceDirectory();
        auto readerOptions = ConvertTo<TTableReaderOptionsPtr>(TYsonString(
            JobSpecExt_.table_reader_options()));

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

        auto partitionTag = YT_OPTIONAL_FROM_PROTO(JobSpecExt_, partition_tag);

        ReaderFactory_ = [=, this, this_ = MakeStrong(this)] (TNameTablePtr nameTable, const TColumnFilter& columnFilter) {
            const auto& tableReaderConfig = Host_->GetJobSpecHelper()->GetJobIOConfig()->TableReader;

            auto factory = PartitionJobSpecExt_.use_sequential_reader()
                ? CreateSchemalessSequentialMultiReader
                : CreateSchemalessParallelMultiReader;
            return factory(
                tableReaderConfig,
                readerOptions,
                Host_->GetChunkReaderHost(),
                dataSourceDirectory,
                std::move(dataSliceDescriptors),
                /*hintKeys*/ std::nullopt,
                nameTable,
                ChunkReadOptions_,
                TReaderInterruptionOptions::InterruptibleWithEmptyKey(),
                columnFilter,
                partitionTag,
                MultiReaderMemoryManager_->CreateMultiReaderMemoryManager(tableReaderConfig->MaxBufferSize));
        };

        YT_VERIFY(JobSpecExt_.output_table_specs_size() == 1);
        const auto& outputSpec = JobSpecExt_.output_table_specs(0);
        TTableSchemaPtr outputSchema;
        if (outputSpec.has_table_schema()) {
            DeserializeFromWireProto(&outputSchema, outputSpec.table_schema());
            outputSchema = outputSchema->ToSorted(sortColumns);
        } else {
            outputSchema = TTableSchema::FromSortColumns(sortColumns);
        }

        auto transactionId = FromProto<TTransactionId>(JobSpecExt_.output_transaction_id());
        auto chunkListId = FromProto<TChunkListId>(outputSpec.chunk_list_id());
        auto options = ConvertTo<TTableWriterOptionsPtr>(TYsonString(outputSpec.table_writer_options()));
        // We pass key column for partitioning through schema, but input stream is not sorted.
        options->ValidateSorted = false;
        auto schemaId = FromProto<TMasterTableSchemaId>(outputSpec.schema_id());
        auto writerConfig = GetWriterConfig(outputSpec);

        std::optional<NChunkClient::TDataSink> dataSink;
        if (auto dataSinkDirectoryExt = FindProtoExtension<TDataSinkDirectoryExt>(JobSpecExt_.extensions())) {
            auto dataSinkDirectory = FromProto<TDataSinkDirectoryPtr>(*dataSinkDirectoryExt);
            YT_VERIFY(std::ssize(dataSinkDirectory->DataSinks()) == 1);
            dataSink = dataSinkDirectory->DataSinks()[0];
        }

        WriterFactory_ = [=, this] (TNameTablePtr nameTable, TTableSchemaPtr /*schema*/) {
            return CreatePartitionMultiChunkWriter(
                writerConfig,
                options,
                nameTable,
                outputSchema,
                Host_->GetClient(),
                Host_->GetLocalHostName(),
                CellTagFromId(chunkListId),
                transactionId,
                schemaId,
                chunkListId,
                CreatePartitioner(PartitionJobSpecExt_),
                dataSink,
                WriteBlocksOptions_,
                Host_->GetTrafficMeter(),
                Host_->GetOutBandwidthThrottler());
        };
    }

private:
    const TPartitionJobSpecExt& PartitionJobSpecExt_;

    TNameTablePtr NameTable_;


    void InitializeReader() override
    {
        DoInitializeReader(NameTable_, TColumnFilter());
    }

    void InitializeWriter() override
    {
        DoInitializeWriter(NameTable_, nullptr);
    }

    bool ShouldSendBoundaryKeys() const override
    {
        return false;
    }

    i64 GetTotalReaderMemoryLimit() const override
    {
        return Host_->GetJobSpecHelper()->GetJobIOConfig()->TableReader->MaxBufferSize;
    }
};

IJobPtr CreatePartitionJob(IJobHostPtr host)
{
    return New<TPartitionJob>(host);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobProxy
