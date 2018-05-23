#include "sorted_merge_job.h"
#include "job_detail.h"

#include <yt/ytlib/chunk_client/chunk_spec.h>
#include <yt/ytlib/chunk_client/data_source.h>
#include <yt/ytlib/chunk_client/job_spec_extensions.h>

#include <yt/ytlib/job_proxy/helpers.h>

#include <yt/ytlib/object_client/helpers.h>

#include <yt/ytlib/table_client/name_table.h>
#include <yt/ytlib/table_client/schemaless_chunk_reader.h>
#include <yt/ytlib/table_client/schemaless_chunk_writer.h>
#include <yt/ytlib/table_client/schemaless_sorted_merging_reader.h>

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

class TSortedMergeJob
    : public TSimpleJobBase
{
public:
    explicit TSortedMergeJob(IJobHostPtr host)
        : TSimpleJobBase(host)
        , MergeJobSpecExt_(JobSpec_.GetExtension(TMergeJobSpecExt::merge_job_spec_ext))
    { }

    virtual void Initialize() override
    {
        YCHECK(SchedulerJobSpecExt_.output_table_specs_size() == 1);
        const auto& outputSpec = SchedulerJobSpecExt_.output_table_specs(0);

        auto keyColumns = FromProto<TKeyColumns>(MergeJobSpecExt_.key_columns());

        auto nameTable = TNameTable::FromKeyColumns(keyColumns);
        std::vector<ISchemalessMultiChunkReaderPtr> readers;

        auto dataSourceDirectoryExt = GetProtoExtension<TDataSourceDirectoryExt>(SchedulerJobSpecExt_.extensions());
        auto dataSourceDirectory = FromProto<TDataSourceDirectoryPtr>(dataSourceDirectoryExt);
        auto readerOptions = ConvertTo<NTableClient::TTableReaderOptionsPtr>(TYsonString(SchedulerJobSpecExt_.table_reader_options()));

        for (const auto& inputSpec : SchedulerJobSpecExt_.input_table_specs()) {
            auto dataSliceDescriptors = UnpackDataSliceDescriptors(inputSpec);

            TotalRowCount_ += GetCumulativeRowCount(dataSliceDescriptors);

            auto reader = CreateSchemalessSequentialMultiReader(
                Host_->GetJobSpecHelper()->GetJobIOConfig()->TableReader,
                readerOptions,
                Host_->GetClient(),
                Host_->LocalDescriptor(),
                Host_->GetBlockCache(),
                Host_->GetInputNodeDirectory(),
                dataSourceDirectory,
                std::move(dataSliceDescriptors),
                nameTable,
                BlockReadOptions_,
                TColumnFilter(),
                keyColumns,
                /* partitionTag */ Null,
                Host_->GetTrafficMeter());

            readers.push_back(reader);
        }

        Reader_ = CreateSchemalessSortedMergingReader(readers, keyColumns.size(), keyColumns.size());

        auto transactionId = FromProto<TTransactionId>(SchedulerJobSpecExt_.output_transaction_id());
        auto chunkListId = FromProto<TChunkListId>(outputSpec.chunk_list_id());
        auto options = ConvertTo<TTableWriterOptionsPtr>(TYsonString(outputSpec.table_writer_options()));
        auto writerConfig = GetWriterConfig(outputSpec);
        auto timestamp = static_cast<TTimestamp>(outputSpec.timestamp());
        auto schema = FromProto<TTableSchema>(outputSpec.table_schema());

        Writer_ = CreateSchemalessMultiChunkWriter(
            writerConfig,
            options,
            nameTable,
            schema,
            TOwningKey(),
            Host_->GetClient(),
            CellTagFromId(chunkListId),
            transactionId,
            chunkListId,
            TChunkTimestamps{timestamp, timestamp},
            Host_->GetTrafficMeter());
    }

private:
    const TMergeJobSpecExt& MergeJobSpecExt_;


    virtual void CreateReader() override
    { }

    virtual void CreateWriter() override
    { }
};

IJobPtr CreateSortedMergeJob(IJobHostPtr host)
{
    return New<TSortedMergeJob>(host);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
