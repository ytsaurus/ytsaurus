#include "simple_sort_job.h"
#include "job_detail.h"

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/ytlib/chunk_client/chunk_spec.h>
#include <yt/yt/ytlib/chunk_client/data_source.h>
#include <yt/yt/ytlib/chunk_client/job_spec_extensions.h>
#include <yt/yt/ytlib/chunk_client/parallel_reader_memory_manager.h>

#include <yt/yt/ytlib/job_proxy/helpers.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/ytlib/table_client/config.h>
#include <yt/yt/ytlib/table_client/schemaless_multi_chunk_reader.h>
#include <yt/yt/ytlib/table_client/schemaless_chunk_writer.h>
#include <yt/yt/ytlib/table_client/sorting_reader.h>

#include <yt/yt/library/query/engine_api/column_evaluator.h>
#include <yt/yt/library/query/row_comparer_api/row_comparer_generator.h>

namespace NYT::NJobProxy {

using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NScheduler::NProto;
using namespace NTransactionClient;
using namespace NTableClient;
using namespace NObjectClient;
using namespace NYTree;
using namespace NYson;

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

class TSimpleSortJob
    : public TSimpleJobBase
{
public:
    using TSimpleJobBase::TSimpleJobBase;

    void Initialize() override
    {
        TSimpleJobBase::Initialize();

        YT_VERIFY(JobSpecExt_.output_table_specs_size() == 1);
        const auto& outputSpec = JobSpecExt_.output_table_specs(0);

        TTableSchemaPtr outputSchema;
        DeserializeFromWireProto(&outputSchema, outputSpec.table_schema());
        auto options = ConvertTo<TTableWriterOptionsPtr>(TYsonString(outputSpec.table_writer_options()));

        auto rowReordererSchema = options->SchemaModification == NTableClient::ETableSchemaModification::None
            ? outputSchema
            : outputSchema->ToModifiedSchema(options->SchemaModification);

        auto nameTable = TNameTable::FromSchema(*outputSchema);

        YT_VERIFY(JobSpecExt_.input_table_specs_size() == 1);
        auto dataSliceDescriptors = Host_->GetJobSpecHelper()->UnpackDataSliceDescriptors();
        auto dataSourceDirectory = Host_->GetJobSpecHelper()->GetDataSourceDirectory();
        auto readerOptions = Host_->GetJobSpecHelper()->GetTableReaderOptions();

        TotalRowCount_ = GetCumulativeRowCount(dataSliceDescriptors);

        ReaderFactory_ = [=, this, this_ = MakeStrong(this)] (TNameTablePtr /*nameTable*/, const TColumnFilter& /*columnFilter*/) {
            const auto& tableReaderConfig = Host_->GetJobSpecHelper()->GetJobIOConfig()->TableReader;
            auto reader = CreateSchemalessParallelMultiReader(
                tableReaderConfig,
                readerOptions,
                Host_->GetChunkReaderHost(),
                dataSourceDirectory,
                std::move(dataSliceDescriptors),
                /*hintKeys*/ std::nullopt,
                nameTable,
                ChunkReadOptions_,
                TReaderInterruptionOptions::InterruptibleWithEmptyKey(),
                /*columnFilter*/ {},
                /*partitionTags*/ {},
                MultiReaderMemoryManager_->CreateMultiReaderMemoryManager(tableReaderConfig->MaxBufferSize));

            TCallback<TUUComparerSignature> cgComparer;
            if (JobSpecExt_.enable_codegen_comparator() && rowReordererSchema->IsCGComparatorApplicable()) {
                cgComparer = NQueryClient::GenerateStaticTableKeyComparer(rowReordererSchema->GetKeyColumnTypes());
            }

            auto columnEvaluator = Host_
                ->GetClient()
                ->GetNativeConnection()
                ->GetColumnEvaluatorCache()
                ->Find(rowReordererSchema);

            return CreateSortingReader(
                reader,
                nameTable,
                rowReordererSchema->GetColumnNames(),
                rowReordererSchema->ToComparator(std::move(cgComparer)),
                std::move(columnEvaluator));
        };

        auto transactionId = FromProto<TTransactionId>(JobSpecExt_.output_transaction_id());
        auto chunkListId = FromProto<TChunkListId>(outputSpec.chunk_list_id());
        options->ExplodeOnValidationError = true;
        options->ValidateKeyWeight = true;
        auto schemaId = FromProto<TMasterTableSchemaId>(outputSpec.schema_id());

        auto writerConfig = GetWriterConfig(outputSpec);
        auto timestamp = static_cast<TTimestamp>(outputSpec.timestamp());

        std::optional<NChunkClient::TDataSink> dataSink = std::nullopt;
        if (auto dataSinkDirectoryExt = FindProtoExtension<TDataSinkDirectoryExt>(JobSpecExt_.extensions())) {
            auto dataSinkDirectory = FromProto<TDataSinkDirectoryPtr>(*dataSinkDirectoryExt);
            YT_VERIFY(std::ssize(dataSinkDirectory->DataSinks()) == 1);
            dataSink = dataSinkDirectory->DataSinks()[0];
        }

        WriterFactory_ = [=, this] (TNameTablePtr /*nameTable*/, TTableSchemaPtr /*schema*/) {
            return CreateSchemalessMultiChunkWriter(
                writerConfig,
                options,
                nameTable,
                outputSchema,
                TLegacyOwningKey(),
                Host_->GetClient(),
                Host_->GetLocalHostName(),
                CellTagFromId(chunkListId),
                transactionId,
                schemaId,
                dataSink,
                WriteBlocksOptions_,
                chunkListId,
                TChunkTimestamps{timestamp, timestamp},
                Host_->GetTrafficMeter(),
                Host_->GetOutBandwidthThrottler());
        };
    }

private:
    void InitializeReader() override
    {
        DoInitializeReader(nullptr, TColumnFilter());
    }

    void InitializeWriter() override
    {
        DoInitializeWriter(nullptr, nullptr);
    }

    i64 GetTotalReaderMemoryLimit() const override
    {
        return Host_->GetJobSpecHelper()->GetJobIOConfig()->TableReader->MaxBufferSize;
    }
};

IJobPtr CreateSimpleSortJob(IJobHostPtr host)
{
    return New<TSimpleSortJob>(host);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobProxy
