#include "partition_sort_job.h"
#include "private.h"
#include "job_detail.h"

#include <yt/yt/ytlib/chunk_client/data_source.h>
#include <yt/yt/ytlib/chunk_client/job_spec_extensions.h>
#include <yt/yt/ytlib/chunk_client/parallel_reader_memory_manager.h>

#include <yt/yt/ytlib/job_proxy/helpers.h>
#include <yt/yt/ytlib/job_proxy/profiling_reader.h>
#include <yt/yt/ytlib/job_proxy/profiling_writer.h>

#include <yt/yt/ytlib/table_client/config.h>
#include <yt/yt/ytlib/table_client/partition_sort_reader.h>
#include <yt/yt/ytlib/table_client/schemaless_chunk_writer.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/client/table_client/name_table.h>

#include <yt/yt/library/query/row_comparer_api/row_comparer_generator.h>

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

static constexpr auto& Logger = JobProxyLogger;

////////////////////////////////////////////////////////////////////////////////

class TPartitionSortJob
    : public TSimpleJobBase
{
public:
    explicit TPartitionSortJob(IJobHostPtr host)
        : TSimpleJobBase(host)
        , SortJobSpecExt_(JobSpec_.GetExtension(TSortJobSpecExt::sort_job_spec_ext))
    { }

    void Initialize() override
    {
        TSimpleJobBase::Initialize();

        auto keyColumns = FromProto<TKeyColumns>(SortJobSpecExt_.key_columns());
        auto nameTable = TNameTable::FromKeyColumns(keyColumns);

        TotalRowCount_ = JobSpecExt_.input_row_count();

        YT_VERIFY(JobSpecExt_.input_table_specs_size() == 1);
        auto dataSliceDescriptors = Host_->GetJobSpecHelper()->UnpackDataSliceDescriptors();
        auto dataSourceDirectory = Host_->GetJobSpecHelper()->GetDataSourceDirectory();

        std::optional<int> partitionTag;
        if (JobSpecExt_.has_partition_tag()) {
            partitionTag = JobSpecExt_.partition_tag();
        } else if (SortJobSpecExt_.has_partition_tag()) {
            partitionTag = SortJobSpecExt_.partition_tag();
        }
        YT_VERIFY(partitionTag);

        YT_VERIFY(JobSpecExt_.output_table_specs_size() == 1);
        const auto& outputSpec = JobSpecExt_.output_table_specs(0);
        TTableSchemaPtr outputSchema;
        DeserializeFromWireProto(&outputSchema, outputSpec.table_schema());

        ReaderFactory_ = [=, this, this_ = MakeStrong(this)] (TNameTablePtr /*nameTable*/, const TColumnFilter& /*columnFilter*/) {
            const auto& tableReaderConfig = Host_->GetJobSpecHelper()->GetJobIOConfig()->TableReader;

            TCallback<TUUComparerSignature> cgComparer;
            if (JobSpecExt_.enable_codegen_comparator() && outputSchema->IsCGComparatorApplicable()) {
                cgComparer = NQueryClient::GenerateStaticTableKeyComparer(outputSchema->GetKeyColumnTypes());
            }

            return CreatePartitionSortReader(
                tableReaderConfig,
                Host_->GetChunkReaderHost(),
                outputSchema->ToComparator(std::move(cgComparer)),
                nameTable,
                BIND(&IJobHost::ReleaseNetwork, MakeWeak(Host_)),
                dataSourceDirectory,
                std::move(dataSliceDescriptors),
                TotalRowCount_,
                JobSpecExt_.is_approximate(),
                *partitionTag,
                ChunkReadOptions_,
                MultiReaderMemoryManager_->CreateMultiReaderMemoryManager(tableReaderConfig->MaxBufferSize));
        };

        auto transactionId = FromProto<TTransactionId>(JobSpecExt_.output_transaction_id());
        auto chunkListId = FromProto<TChunkListId>(outputSpec.chunk_list_id());
        auto options = ConvertTo<TTableWriterOptionsPtr>(TYsonString(outputSpec.table_writer_options()));
        options->ExplodeOnValidationError = true;
        options->ValidateKeyWeight = true;

        // Right now intermediate data in sort operation doesn't have schema
        // so all composite values in input tables become Any values.
        // Cast them back.
        options->CastAnyToComposite = true;
        auto schemaId = FromProto<TMasterTableSchemaId>(outputSpec.schema_id());

        auto writerConfig = GetWriterConfig(outputSpec);
        auto timestamp = static_cast<TTimestamp>(outputSpec.timestamp());

        std::optional<NChunkClient::TDataSink> dataSink;
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

    double GetProgress() const override
    {
        auto total = TotalRowCount_;
        if (total == 0) {
            YT_LOG_WARNING("GetProgress: empty total");
            return 0;
        } else {
            // Split progress evenly between reading and writing.
            double progress =
                0.5 * Reader_->GetDataStatistics().row_count() / total +
                0.5 * Writer_->GetDataStatistics().row_count() / total;
            YT_LOG_DEBUG("GetProgress: %lf", progress);
            return progress;
        }
    }

private:
    const TSortJobSpecExt& SortJobSpecExt_;

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

IJobPtr CreatePartitionSortJob(IJobHostPtr host)
{
    return New<TPartitionSortJob>(host);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobProxy
