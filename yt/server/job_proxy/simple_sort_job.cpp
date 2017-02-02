#include "simple_sort_job.h"
#include "private.h"
#include "config.h"
#include "job_detail.h"

#include <yt/ytlib/object_client/helpers.h>

#include <yt/ytlib/chunk_client/chunk_spec.h>

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
        auto config = Host_->GetConfig();

        auto keyColumns = FromProto<TKeyColumns>(SortJobSpecExt_.key_columns());
        auto nameTable = TNameTable::FromKeyColumns(keyColumns);

        YCHECK(SchedulerJobSpecExt_.input_table_specs_size() == 1);
        const auto& inputSpec = SchedulerJobSpecExt_.input_table_specs(0);
        std::vector<TChunkSpec> chunkSpecs(inputSpec.chunks().begin(), inputSpec.chunks().end());
        auto readerOptions = ConvertTo<NTableClient::TTableReaderOptionsPtr>(TYsonString(inputSpec.table_reader_options()));
        TotalRowCount_ = GetCumulativeRowCount(chunkSpecs);

        auto reader = CreateSchemalessParallelMultiChunkReader(
            config->JobIO->TableReader,
            readerOptions,
            Host_->GetClient(),
            Host_->LocalDescriptor(),
            Host_->GetBlockCache(),
            Host_->GetInputNodeDirectory(),
            chunkSpecs,
            nameTable);

        Reader_ = CreateSchemalessSortingReader(reader, nameTable, keyColumns);

        auto transactionId = FromProto<TTransactionId>(SchedulerJobSpecExt_.output_transaction_id());
        const auto& outputSpec = SchedulerJobSpecExt_.output_table_specs(0);
        auto chunkListId = FromProto<TChunkListId>(outputSpec.chunk_list_id());
        auto options = ConvertTo<TTableWriterOptionsPtr>(TYsonString(outputSpec.table_writer_options()));
        options->ExplodeOnValidationError = true;
        options->ValidateKeyWeight = true;

        auto schema = FromProto<TTableSchema>(outputSpec.table_schema());

        Writer_ = CreateSchemalessMultiChunkWriter(
            config->JobIO->TableWriter,
            options,
            nameTable,
            schema,
            TOwningKey(),
            Host_->GetClient(),
            CellTagFromId(chunkListId),
            transactionId,
            chunkListId);
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
