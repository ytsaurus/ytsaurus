#include "partition_sort_job.h"
#include "private.h"
#include "job_detail.h"

#include <yt/yt/ytlib/chunk_client/data_source.h>
#include <yt/yt/ytlib/chunk_client/job_spec_extensions.h>
#include <yt/yt/ytlib/chunk_client/parallel_reader_memory_manager.h>

#include <yt/yt/ytlib/job_proxy/helpers.h>

#include <yt/yt/ytlib/table_client/partition_sort_reader.h>
#include <yt/yt/ytlib/table_client/schemaless_chunk_writer.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/client/table_client/name_table.h>

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

static const auto& Logger = JobProxyLogger;

////////////////////////////////////////////////////////////////////////////////

class TPartitionSortJob
    : public TSimpleJobBase
{
public:
    explicit TPartitionSortJob(IJobHost* host)
        : TSimpleJobBase(host)
        , SortJobSpecExt_(JobSpec_.GetExtension(TSortJobSpecExt::sort_job_spec_ext))
    { }

    virtual void Initialize() override
    {
        TSimpleJobBase::Initialize();

        auto keyColumns = FromProto<TKeyColumns>(SortJobSpecExt_.key_columns());
        auto nameTable = TNameTable::FromKeyColumns(keyColumns);

        TotalRowCount_ = SchedulerJobSpecExt_.input_row_count();

        YT_VERIFY(SchedulerJobSpecExt_.input_table_specs_size() == 1);
        const auto& inputSpec = SchedulerJobSpecExt_.input_table_specs(0);
        auto dataSliceDescriptors = UnpackDataSliceDescriptors(inputSpec);

        auto dataSourceDirectoryExt = GetProtoExtension<TDataSourceDirectoryExt>(SchedulerJobSpecExt_.extensions());
        auto dataSourceDirectory = FromProto<TDataSourceDirectoryPtr>(dataSourceDirectoryExt);

        std::optional<int> partitionTag;
        if (SchedulerJobSpecExt_.has_partition_tag()) {
            partitionTag = SchedulerJobSpecExt_.partition_tag();
        } else if (SortJobSpecExt_.has_partition_tag()) {
            partitionTag = SortJobSpecExt_.partition_tag();
        }
        YT_VERIFY(partitionTag);

        YT_VERIFY(SchedulerJobSpecExt_.output_table_specs_size() == 1);
        const auto& outputSpec = SchedulerJobSpecExt_.output_table_specs(0);
        TTableSchemaPtr outputSchema;
        DeserializeFromWireProto(&outputSchema, outputSpec.table_schema());

        ReaderFactory_ = [=] (TNameTablePtr /*nameTable*/, const TColumnFilter& /*columnFilter*/) {
            const auto& tableReaderConfig = Host_->GetJobSpecHelper()->GetJobIOConfig()->TableReader;

            return CreatePartitionSortReader(
                tableReaderConfig,
                Host_->GetClient(),
                Host_->GetReaderBlockCache(),
                /*chunkMetaCache*/ nullptr,
                Host_->GetInputNodeDirectory(),
                outputSchema->ToComparator(),
                nameTable,
                BIND(&IJobHost::ReleaseNetwork, MakeWeak(Host_)),
                dataSourceDirectory,
                std::move(dataSliceDescriptors),
                TotalRowCount_,
                SchedulerJobSpecExt_.is_approximate(),
                *partitionTag,
                ChunkReadOptions_,
                Host_->GetTrafficMeter(),
                Host_->GetInBandwidthThrottler(),
                Host_->GetOutRpsThrottler(),
                MultiReaderMemoryManager_->CreateMultiReaderMemoryManager(tableReaderConfig->MaxBufferSize));
        };

        auto transactionId = FromProto<TTransactionId>(SchedulerJobSpecExt_.output_transaction_id());
        auto chunkListId = FromProto<TChunkListId>(outputSpec.chunk_list_id());
        auto options = ConvertTo<TTableWriterOptionsPtr>(TYsonString(outputSpec.table_writer_options()));
        options->ExplodeOnValidationError = true;
        options->ValidateKeyWeight = true;

        // Right now intermediate data in sort operation doesn't have schema
        // so all composite values in input tables become Any values.
        // Cast them back.
        options->CastAnyToComposite = true;

        auto writerConfig = GetWriterConfig(outputSpec);
        auto timestamp = static_cast<TTimestamp>(outputSpec.timestamp());

        WriterFactory_ = [=] (TNameTablePtr /*nameTable*/, TTableSchemaPtr /*schema*/) {
            return CreateSchemalessMultiChunkWriter(
                writerConfig,
                options,
                nameTable,
                outputSchema,
                TLegacyOwningKey(),
                Host_->GetClient(),
                CellTagFromId(chunkListId),
                transactionId,
                chunkListId,
                TChunkTimestamps{timestamp, timestamp},
                Host_->GetTrafficMeter(),
                Host_->GetOutBandwidthThrottler());
        };
    }

    virtual double GetProgress() const override
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

    virtual void InitializeReader() override
    {
        DoInitializeReader(nullptr, TColumnFilter());
    }

    virtual void InitializeWriter() override
    {
        DoInitializeWriter(nullptr, nullptr);
    }

    virtual i64 GetTotalReaderMemoryLimit() const override
    {
        return Host_->GetJobSpecHelper()->GetJobIOConfig()->TableReader->MaxBufferSize;
    }
};

IJobPtr CreatePartitionSortJob(IJobHost* host)
{
    return New<TPartitionSortJob>(host);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobProxy
