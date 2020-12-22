#include "simple_sort_job.h"
#include "private.h"
#include "job_detail.h"

#include <yt/ytlib/chunk_client/chunk_spec.h>
#include <yt/ytlib/chunk_client/data_source.h>
#include <yt/ytlib/chunk_client/job_spec_extensions.h>
#include <yt/ytlib/chunk_client/parallel_reader_memory_manager.h>

#include <yt/ytlib/job_proxy/helpers.h>

#include <yt/client/object_client/helpers.h>

#include <yt/client/table_client/name_table.h>
#include <yt/ytlib/table_client/schemaless_multi_chunk_reader.h>
#include <yt/ytlib/table_client/schemaless_chunk_writer.h>
#include <yt/ytlib/table_client/sorting_reader.h>

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

class TSimpleSortJob
    : public TSimpleJobBase
{
public:
    explicit TSimpleSortJob(IJobHost* host)
        : TSimpleJobBase(host)
    { }

    virtual void Initialize() override
    {
        TSimpleJobBase::Initialize();

        YT_VERIFY(SchedulerJobSpecExt_.output_table_specs_size() == 1);
        const auto& outputSpec = SchedulerJobSpecExt_.output_table_specs(0);

        TTableSchemaPtr outputSchema;
        DeserializeFromWireProto(&outputSchema, outputSpec.table_schema());

        auto keyColumns = outputSchema->GetKeyColumns();
        auto nameTable = TNameTable::FromKeyColumns(keyColumns);

        YT_VERIFY(SchedulerJobSpecExt_.input_table_specs_size() == 1);
        const auto& inputSpec = SchedulerJobSpecExt_.input_table_specs(0);
        auto dataSliceDescriptors = UnpackDataSliceDescriptors(inputSpec);
        auto dataSourceDirectoryExt = GetProtoExtension<TDataSourceDirectoryExt>(SchedulerJobSpecExt_.extensions());
        auto dataSourceDirectory = FromProto<TDataSourceDirectoryPtr>(dataSourceDirectoryExt);
        auto readerOptions = ConvertTo<TTableReaderOptionsPtr>(TYsonString(
            SchedulerJobSpecExt_.table_reader_options()));

        TotalRowCount_ = GetCumulativeRowCount(dataSliceDescriptors);

        const auto& tableReaderConfig = Host_->GetJobSpecHelper()->GetJobIOConfig()->TableReader;
        auto reader = CreateSchemalessParallelMultiReader(
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
            /* columnFilter */ {},
            /* keyColumns */ {},
            /* partitionTag */ std::nullopt,
            Host_->GetTrafficMeter(),
            Host_->GetInBandwidthThrottler(),
            Host_->GetOutRpsThrottler(),
            MultiReaderMemoryManager_->CreateMultiReaderMemoryManager(tableReaderConfig->MaxBufferSize));

        Reader_ = CreateSortingReader(reader, nameTable, keyColumns, outputSchema->ToComparator());

        auto transactionId = FromProto<TTransactionId>(SchedulerJobSpecExt_.output_transaction_id());
        auto chunkListId = FromProto<TChunkListId>(outputSpec.chunk_list_id());
        auto options = ConvertTo<TTableWriterOptionsPtr>(TYsonString(outputSpec.table_writer_options()));
        options->ExplodeOnValidationError = true;
        options->ValidateKeyWeight = true;

        auto writerConfig = GetWriterConfig(outputSpec);
        auto timestamp = static_cast<TTimestamp>(outputSpec.timestamp());

        Writer_ = CreateSchemalessMultiChunkWriter(
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
    }

private:
    virtual void CreateReader() override
    { }

    virtual void CreateWriter() override
    { }

    virtual i64 GetTotalReaderMemoryLimit() const
    {
        return Host_->GetJobSpecHelper()->GetJobIOConfig()->TableReader->MaxBufferSize;
    }
};

IJobPtr CreateSimpleSortJob(IJobHost* host)
{
    return New<TSimpleSortJob>(host);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobProxy
