#include "sorted_merge_job.h"
#include "job_detail.h"

#include <yt/yt/ytlib/chunk_client/chunk_spec.h>
#include <yt/yt/ytlib/chunk_client/data_source.h>
#include <yt/yt/ytlib/chunk_client/job_spec_extensions.h>
#include <yt/yt/ytlib/chunk_client/parallel_reader_memory_manager.h>

#include <yt/yt/ytlib/controller_agent/proto/job.pb.h>

#include <yt/yt/ytlib/job_proxy/helpers.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/ytlib/table_client/schemaless_multi_chunk_reader.h>
#include <yt/yt/ytlib/table_client/schemaless_chunk_writer.h>
#include <yt/yt/ytlib/table_client/sorted_merging_reader.h>

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

class TSortedMergeJob
    : public TSimpleJobBase
{
public:
    explicit TSortedMergeJob(IJobHostPtr host)
        : TSimpleJobBase(host)
        , MergeJobSpecExt_(JobSpec_.GetExtension(TMergeJobSpecExt::merge_job_spec_ext))
    { }

    void Initialize() override
    {
        TSimpleJobBase::Initialize();

        YT_VERIFY(SchedulerJobSpecExt_.output_table_specs_size() == 1);
        const auto& outputSpec = SchedulerJobSpecExt_.output_table_specs(0);

        auto keyColumns = FromProto<TKeyColumns>(MergeJobSpecExt_.key_columns());
        auto sortColumns = FromProto<TSortColumns>(MergeJobSpecExt_.sort_columns());

        // COMPAT(gritukan)
        if (sortColumns.empty()) {
            for (const auto& keyColumn : keyColumns) {
                sortColumns.push_back({keyColumn, ESortOrder::Ascending});
            }
        }

        auto nameTable = TNameTable::FromKeyColumns(keyColumns);

        auto dataSourceDirectoryExt = GetProtoExtension<TDataSourceDirectoryExt>(SchedulerJobSpecExt_.extensions());
        auto dataSourceDirectory = FromProto<TDataSourceDirectoryPtr>(dataSourceDirectoryExt);
        auto readerOptions = ConvertTo<NTableClient::TTableReaderOptionsPtr>(TYsonString(SchedulerJobSpecExt_.table_reader_options()));

        // We must always enable key widening to prevent out of range access of key prefixes in sorted merging/joining readers.
        readerOptions->EnableKeyWidening = true;

        YT_VERIFY(!dataSourceDirectory->DataSources().empty());

        ReaderFactory_ = [=, this, this_ = MakeStrong(this)] (TNameTablePtr /*nameTable*/, const TColumnFilter& /*columnFilter*/) {
            std::vector<ISchemalessMultiChunkReaderPtr> readers;
            for (const auto& inputSpec : SchedulerJobSpecExt_.input_table_specs()) {
                auto dataSliceDescriptors = UnpackDataSliceDescriptors(inputSpec);

                TotalRowCount_ += GetCumulativeRowCount(dataSliceDescriptors);

                const auto& tableReaderConfig = Host_->GetJobSpecHelper()->GetJobIOConfig()->TableReader;
                auto reader = CreateSchemalessSequentialMultiReader(
                    tableReaderConfig,
                    readerOptions,
                    Host_->GetChunkReaderHost(),
                    dataSourceDirectory,
                    std::move(dataSliceDescriptors),
                    /*hintKeys*/ std::nullopt,
                    nameTable,
                    ChunkReadOptions_,
                    ReaderInterruptionOptions::InterruptibleWithKeyLength(std::ssize(sortColumns)),
                    /*columnFilter*/ {},
                    /*partitionTag*/ std::nullopt,
                    MultiReaderMemoryManager_->CreateMultiReaderMemoryManager(tableReaderConfig->MaxBufferSize));

                readers.push_back(reader);
            }

            auto sortComparator = GetComparator(sortColumns);
            return CreateSortedMergingReader(
                readers,
                sortComparator,
                sortComparator,
                /*interruptAtKeyEdge*/ false);
        };

        auto transactionId = FromProto<TTransactionId>(SchedulerJobSpecExt_.output_transaction_id());
        auto chunkListId = FromProto<TChunkListId>(outputSpec.chunk_list_id());
        auto options = ConvertTo<TTableWriterOptionsPtr>(TYsonString(outputSpec.table_writer_options()));
        // Right now intermediate data in sort operation doesn't have schema
        // so all composite values in input tables become Any values.
        // Cast them back.
        options->CastAnyToComposite = true;

        auto writerConfig = GetWriterConfig(outputSpec);
        auto timestamp = static_cast<TTimestamp>(outputSpec.timestamp());

        TTableSchemaPtr schema;
        DeserializeFromWireProto(&schema, outputSpec.table_schema());

        std::optional<NChunkClient::TDataSink> dataSink;
        if (auto dataSinkDirectoryExt = FindProtoExtension<TDataSinkDirectoryExt>(SchedulerJobSpecExt_.extensions())) {
            auto dataSinkDirectory = FromProto<TDataSinkDirectoryPtr>(*dataSinkDirectoryExt);
            YT_VERIFY(std::ssize(dataSinkDirectory->DataSinks()) == 1);
            dataSink = dataSinkDirectory->DataSinks()[0];
        }

        WriterFactory_ = [=, this] (TNameTablePtr /*nameTable*/, TTableSchemaPtr /*schema*/) {
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
                dataSink,
                chunkListId,
                TChunkTimestamps{timestamp, timestamp},
                Host_->GetTrafficMeter(),
                Host_->GetOutBandwidthThrottler());
        };
    }

    NControllerAgent::NProto::TJobResult Run() override
    {
        try {
            return TSimpleJobBase::Run();
        } catch (const TErrorException& ex) {
            if (ex.Error().FindMatching(NTableClient::EErrorCode::SortOrderViolation)) {
                // We assume that sort order violation happens only in cases similar to YT-9487
                // when there are overlapping ranges specified for the same table. Note that
                // we cannot reliably detect such a situation in controller.
                THROW_ERROR_EXCEPTION(
                    "Sort order violation in a sorted merge job detected; one of the possible reasons is "
                    "that there are overlapping ranges specified on one of the input tables that is not allowed")
                    << ex;
            }
            throw;
        }
    }

private:
    const TMergeJobSpecExt& MergeJobSpecExt_;

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
        auto readerMemoryLimit = Host_->GetJobSpecHelper()->GetJobIOConfig()->TableReader->MaxBufferSize;
        return readerMemoryLimit * SchedulerJobSpecExt_.input_table_specs_size();
    }
};

IJobPtr CreateSortedMergeJob(IJobHostPtr host)
{
    return New<TSortedMergeJob>(host);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobProxy
