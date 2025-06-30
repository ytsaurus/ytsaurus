#include "merge_job.h"
#include "private.h"
#include "job_detail.h"

#include <yt/yt/ytlib/chunk_client/chunk_spec.h>
#include <yt/yt/ytlib/chunk_client/data_source.h>
#include <yt/yt/ytlib/chunk_client/data_sink.h>
#include <yt/yt/ytlib/chunk_client/job_spec_extensions.h>
#include <yt/yt/ytlib/chunk_client/parallel_reader_memory_manager.h>

#include <yt/yt/ytlib/controller_agent/proto/job.pb.h>

#include <yt/yt/ytlib/job_proxy/helpers.h>

#include <yt/yt/ytlib/table_client/config.h>
#include <yt/yt/ytlib/table_client/schemaless_multi_chunk_reader.h>
#include <yt/yt/ytlib/table_client/schemaless_chunk_writer.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/client/table_client/name_table.h>

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

constinit const auto Logger = JobProxyLogger;

////////////////////////////////////////////////////////////////////////////////

class TMergeJob
    : public TSimpleJobBase
{
public:
    TMergeJob(IJobHostPtr host, bool useParallelReader)
        : TSimpleJobBase(host)
        , UseParallelReader_(useParallelReader)
    {
        YT_VERIFY(JobSpecExt_.output_table_specs_size() == 1);
    }

    void Initialize() override
    {
        TSimpleJobBase::Initialize();

        TKeyColumns keyColumns;
        std::optional<int> partitionTag;
        if (JobSpec_.HasExtension(TMergeJobSpecExt::merge_job_spec_ext)) {
            const auto& mergeJobSpec = JobSpec_.GetExtension(TMergeJobSpecExt::merge_job_spec_ext);
            keyColumns = FromProto<TKeyColumns>(mergeJobSpec.key_columns());
            if (JobSpecExt_.has_partition_tag()) {
                partitionTag = JobSpecExt_.partition_tag();
            } else if (mergeJobSpec.has_partition_tag()) {
                partitionTag = mergeJobSpec.partition_tag();
            }
            YT_LOG_INFO("Ordered merge produces sorted output");
        }

        auto dataSliceDescriptors = Host_->GetJobSpecHelper()->UnpackDataSliceDescriptors();

        TotalRowCount_ = JobSpecExt_.input_row_count();

        auto readerOptions = Host_->GetJobSpecHelper()->GetTableReaderOptions();
        auto dataSourceDirectory = Host_->GetJobSpecHelper()->GetDataSourceDirectory();

        NameTable_ = TNameTable::FromKeyColumns(keyColumns);

        ReaderFactory_ = [=, this, this_ = MakeStrong(this)] (TNameTablePtr nameTable, const TColumnFilter& columnFilter) {
            const auto& tableReaderConfig = Host_->GetJobSpecHelper()->GetJobIOConfig()->TableReader;
            auto readerFactory = UseParallelReader_
                ? CreateSchemalessParallelMultiReader
                : CreateSchemalessSequentialMultiReader;
            return readerFactory(
                tableReaderConfig,
                readerOptions,
                Host_->GetChunkReaderHost(),
                dataSourceDirectory,
                std::move(dataSliceDescriptors),
                /*hintKeys*/ std::nullopt,
                std::move(nameTable),
                ChunkReadOptions_,
                TReaderInterruptionOptions::InterruptibleWithEmptyKey(),
                columnFilter,
                partitionTag,
                MultiReaderMemoryManager_->CreateMultiReaderMemoryManager(tableReaderConfig->MaxBufferSize));
        };

        auto transactionId = FromProto<TTransactionId>(JobSpecExt_.output_transaction_id());
        const auto& outputSpec = JobSpecExt_.output_table_specs(0);
        auto chunkListId = FromProto<TChunkListId>(outputSpec.chunk_list_id());
        auto options = ConvertTo<TTableWriterOptionsPtr>(TYsonString(outputSpec.table_writer_options()));
        options->CastAnyToComposite = true;
        auto schemaId = FromProto<TMasterTableSchemaId>(outputSpec.schema_id());

        TTableSchemaPtr schema;
        DeserializeFromWireProto(&schema, outputSpec.table_schema());

        auto writerConfig = GetWriterConfig(outputSpec);
        auto timestamp = static_cast<TTimestamp>(outputSpec.timestamp());

        std::optional<NChunkClient::TDataSink> dataSink;
        if (auto dataSinkDirectoryExt = FindProtoExtension<TDataSinkDirectoryExt>(JobSpecExt_.extensions())) {
            auto dataSinkDirectory = FromProto<TDataSinkDirectoryPtr>(*dataSinkDirectoryExt);
            YT_VERIFY(std::ssize(dataSinkDirectory->DataSinks()) == 1);
            dataSink = dataSinkDirectory->DataSinks()[0];
        }

        WriterFactory_ = [=, this] (TNameTablePtr nameTable, TTableSchemaPtr /*schema*/) {
            return CreateSchemalessMultiChunkWriter(
                writerConfig,
                options,
                nameTable,
                schema,
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
    const bool UseParallelReader_;

    TNameTablePtr NameTable_;

    void InitializeReader() override
    {
        DoInitializeReader(NameTable_, TColumnFilter());
    }

    void InitializeWriter() override
    {
        // NB. WriterFactory_ ignores schema argument and uses schema of output table.
        DoInitializeWriter(NameTable_, nullptr);
    }

    i64 GetTotalReaderMemoryLimit() const override
    {
        return Host_->GetJobSpecHelper()->GetJobIOConfig()->TableReader->MaxBufferSize;
    }
};

IJobPtr CreateOrderedMergeJob(IJobHostPtr host)
{
    return New<TMergeJob>(host, false);
}

IJobPtr CreateUnorderedMergeJob(IJobHostPtr host)
{
    return New<TMergeJob>(host, true);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobProxy
