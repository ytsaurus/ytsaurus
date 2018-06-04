#include "simple_sort_job.h"
#include "private.h"
#include "job_detail.h"

#include <yt/ytlib/chunk_client/chunk_spec.h>
#include <yt/ytlib/chunk_client/data_source.h>
#include <yt/ytlib/chunk_client/job_spec_extensions.h>

#include <yt/ytlib/job_proxy/helpers.h>

#include <yt/ytlib/object_client/helpers.h>

#include <yt/ytlib/table_client/name_table.h>
#include <yt/ytlib/table_client/schemaless_chunk_reader.h>
#include <yt/ytlib/table_client/schemaless_chunk_writer.h>
#include <yt/ytlib/table_client/schemaless_sorting_reader.h>

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

class TSimpleSortJob
    : public TSimpleJobBase
{
public:
    explicit TSimpleSortJob(IJobHostPtr host)
        : TSimpleJobBase(host)
        , SortJobSpecExt_(JobSpec_.GetExtension(TSortJobSpecExt::sort_job_spec_ext))
    { }

    virtual void Initialize() override
    {
        auto keyColumns = FromProto<TKeyColumns>(SortJobSpecExt_.key_columns());
        auto nameTable = TNameTable::FromKeyColumns(keyColumns);

        YCHECK(SchedulerJobSpecExt_.input_table_specs_size() == 1);
        const auto& inputSpec = SchedulerJobSpecExt_.input_table_specs(0);
        auto dataSliceDescriptors = UnpackDataSliceDescriptors(inputSpec);
        auto dataSourceDirectoryExt = GetProtoExtension<TDataSourceDirectoryExt>(SchedulerJobSpecExt_.extensions());
        auto dataSourceDirectory = FromProto<TDataSourceDirectoryPtr>(dataSourceDirectoryExt);
        auto readerOptions = ConvertTo<TTableReaderOptionsPtr>(TYsonString(
            SchedulerJobSpecExt_.table_reader_options()));

        TotalRowCount_ = GetCumulativeRowCount(dataSliceDescriptors);

        auto reader = CreateSchemalessParallelMultiReader(
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
            TKeyColumns(),
            /* partitionTag */ Null,
            Host_->GetTrafficMeter());

        Reader_ = CreateSchemalessSortingReader(reader, nameTable, keyColumns);

        auto transactionId = FromProto<TTransactionId>(SchedulerJobSpecExt_.output_transaction_id());
        const auto& outputSpec = SchedulerJobSpecExt_.output_table_specs(0);
        auto chunkListId = FromProto<TChunkListId>(outputSpec.chunk_list_id());
        auto options = ConvertTo<TTableWriterOptionsPtr>(TYsonString(outputSpec.table_writer_options()));
        options->ExplodeOnValidationError = true;
        options->ValidateKeyWeight = true;

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
    const TSortJobSpecExt& SortJobSpecExt_;

    virtual void CreateReader() override
    { }

    virtual void CreateWriter() override
    { }
};

IJobPtr CreateSimpleSortJob(IJobHostPtr host)
{
    return New<TSimpleSortJob>(host);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
